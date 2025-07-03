from flask import Flask, request, jsonify
from werkzeug.utils import secure_filename
import os
import time
import logging
import datetime
from ocr_processor import ReceiptProcessor  # OCR 처리 모듈
from date_utils import extract_date, parse_date  # 날짜 처리 모듈
from receipt_processor import DbCategorizer


app = Flask(__name__)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('server.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 상대경로 설정
app.config['UPLOAD_FOLDER'] = 'image'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}

# OCR 엔진 초기화
processor = UniversalReceiptProcessor()
db_categorizer = DbCategorizer()

@app.route('/upload', methods=['POST'])
def upload_image():
    start_time = time.time()
    logger.info("===== 요청 시작 =====")
    
    if 'image' not in request.files:
        logger.error("요청에 파일이 없음")
        return jsonify({'error': 'No file part'}), 400
    
    file = request.files['image']
    
    if file.filename == '':
        logger.error("선택된 파일 없음")
        return jsonify({'error': 'No selected file'}), 400
    
    if file and file.filename.lower().endswith(tuple(ALLOWED_EXTENSIONS)):
        filename = secure_filename(file.filename)
        
        # 업로드 폴더 생성
        os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
        save_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        
        # 파일 저장
        file.save(save_path)
        logger.info(f"파일 저장 완료: {save_path} (크기: {os.path.getsize(save_path)} 바이트)")
        
        try:
            # OCR 처리
            ocr_start = time.time()
            semantic_groups = processor.debug_ocr_grouping(save_path)
            ocr_time = time.time() - ocr_start
            logger.info(f"OCR 처리 완료 (소요 시간: {ocr_time:.2f}초)")
            logger.debug(f"OCR 그룹 결과: {semantic_groups}")
            
            # 전체 텍스트 결합 (날짜 추출용)
            full_text = " ".join([menu for menu, _ in semantic_groups])
            
            # 날짜 정보 추출 (ocrasping 모듈 사용)
            date_start = time.time()
            extracted_date = extract_date(full_text)
            if extracted_date:
                year, month, day = parse_date(extracted_date)
                logger.info(f"영수증에서 날짜 추출: {year}-{month}-{day}")
            else:
                today = datetime.date.today()
                year, month, day = today.year, today.month, today.day
                logger.info(f"현재 날짜 사용: {year}-{month}-{day}")
            date_time = time.time() - date_start
            
            # 메뉴-가격 쌍 필터링
            menu_start = time.time()
            total_keywords = ['주문금액', '배달비', '할인', '총', '합계', '카드', '현금']
            filtered_pairs = [
                [menu, int(price)] 
                for menu, price in semantic_groups
                if not any(keyword in menu for keyword in total_keywords)
            ]
            menu_time = time.time() - menu_start
            logger.info(f"메뉴 필터링 완료 (항목 수: {len(filtered_pairs)}, 소요 시간: {menu_time:.2f}초)")
            logger.debug(f"필터링된 메뉴: {filtered_pairs}")
            
            # JSON 응답 생성
            response = {
                "year": f"{year:04d}",
                "month": f"{month:02d}",
                "day": f"{day:02d}",
                "menu_price_pairs": filtered_pairs
            }
            
            total_time = time.time() - start_time
            logger.info(f"요청 처리 완료 (총 소요 시간: {total_time:.2f}초)")
            logger.info(f"OCR: {ocr_time:.2f}초 | 날짜: {date_time:.2f}초 | 메뉴: {menu_time:.2f}초")
            
            return jsonify(response), 200
        
        except Exception as e:
            logger.error(f"처리 실패: {str(e)}", exc_info=True)
            return jsonify({'error': f'Processing failed: {str(e)}'}), 500
    
    logger.error("잘못된 파일 형식")
    return jsonify({'error': 'Invalid file type'}), 400

@app.route('/save_receipt', methods=['POST'])
def save_receipt():
    data = request.get_json()
    receipt_id = data.get('receipt_id')
    if receipt_id:
        save_success = db_categorizer.update_receipt(
            receipt_id,
            data['store_name'],
            data['date'],
            data['items'],
            data['total_amount']
        )
    else:
        receipt_id = db_categorizer.add_receipt(
            data['store_name'],
            data['date'],
            data['items'],
            data['total_amount']
        )
        save_success = receipt_id is not None
    return jsonify({'db_saved': save_success, 'receipt_id': receipt_id}), 200

if __name__ == '__main__':
    # 업로드 폴더 생성
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    logger.info(f"서버 시작 - 업로드 폴더: {os.path.abspath(app.config['UPLOAD_FOLDER'])}")
    
    # 디버그 모드에서 자세한 로깅
    if app.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("디버그 모드 활성화 - 상세 로깅 출력")
    
    app.run(host='0.0.0.0', port=18080, debug=True)
