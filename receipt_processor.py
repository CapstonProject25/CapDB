import os
import json
import sqlite3
from datetime import datetime
import requests
from dotenv import load_dotenv
from typing import Dict, Any
import re
from difflib import get_close_matches

# CategoryManager 모듈 임포트
from category_manager import CategoryManager

# .env 파일에서 환경 변수 로드
load_dotenv()

# 영수증 데이터를 처리하고 데이터베이스에 저장하며, 통계 기능을 제공하는 클래스입니다.
class DbCategorizer:
    def __init__(self):
        """초기화"""
        self.db_path = 'receipts.db'
        self.ollama_url = os.getenv('OLLAMA_API_BASE', 'http://localhost:11434') + '/api/chat'
        
        # 유효한 카테고리와 서브카테고리 정의
        self.valid_categories = {
            '미용': ['피부 미용', '헤어', '화장'],
            '쇼핑': ['생필품', '의류', '가전', '가구', '식재료', '장난감', '화장품', '스마트기기'],
            '교통': ['택시', '대중교통', '여객선', '기차', '항공기', '주유'],
            '의료': ['약품', '진료'],
            '여행': ['식비', '숙박비', '티켓'],
            '음식': ['아침', '점심', '저녁', '간식', '음료', '과일', '디저트', '유제품'],
            '취미': ['게임', '영화', '공연', '놀이공원', '운동', '도서'],
            '투자': ['교육', '보험', '주식', '부동산', '신용카드'],
            '공과금': ['통신비', '전기세', '가스비', '구독료', '멤버십', '인터넷 요금', '휴대폰 요금', '세금']
        }
        
        # 카테고리 검색을 위한 역인덱스 생성
        self.category_index = {}
        for cat, subcats in self.valid_categories.items():
            for subcat in subcats:
                self.category_index[subcat] = cat
        
        self._init_db()
        
    def _init_db(self):
        """데이터베이스 초기화"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 기존 테이블 삭제 (데이터 초기화)
                cursor.execute('DROP TABLE IF EXISTS items')
                cursor.execute('DROP TABLE IF EXISTS receipts')
                cursor.execute('DROP TABLE IF EXISTS sub_categories')
                cursor.execute('DROP TABLE IF EXISTS main_categories')
                
                # 메인 카테고리 테이블
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS main_categories (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL UNIQUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # 서브 카테고리 테이블
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS sub_categories (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        main_category_id INTEGER NOT NULL,
                        name TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (main_category_id) REFERENCES main_categories(id),
                        UNIQUE(main_category_id, name)
                    )
                ''')
                
                # 영수증 테이블
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS receipts (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        store_name TEXT NOT NULL,
                        date TEXT NOT NULL,
                        total_amount INTEGER NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # 품목 테이블
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS items (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        receipt_id INTEGER NOT NULL,
                        name TEXT NOT NULL,
                        main_category_id INTEGER NOT NULL,
                        sub_category_id INTEGER NOT NULL,
                        amount INTEGER NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (receipt_id) REFERENCES receipts(id),
                        FOREIGN KEY (main_category_id) REFERENCES main_categories(id),
                        FOREIGN KEY (sub_category_id) REFERENCES sub_categories(id)
                    )
                ''')
                
                # 기본 카테고리 데이터 삽입
                main_categories = [(cat,) for cat in self.valid_categories.keys()]
                cursor.executemany(
                    'INSERT OR IGNORE INTO main_categories (name) VALUES (?)',
                    main_categories
                )
                
                # 서브 카테고리 데이터 삽입
                for main_cat, sub_cats in self.valid_categories.items():
                    for sub_cat in sub_cats:
                        cursor.execute('''
                            INSERT OR IGNORE INTO sub_categories (main_category_id, name)
                            SELECT id, ? FROM main_categories WHERE name = ?
                        ''', (sub_cat, main_cat))
                
                conn.commit()
                
        except Exception as e:
            print(f"데이터베이스 초기화 중 오류 발생: {str(e)}")

    def _get_category_ids(self, main_category: str, sub_category: str) -> tuple:
        """카테고리 ID 조회"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 메인 카테고리 ID 조회
                cursor.execute(
                    'SELECT id FROM main_categories WHERE name = ?',
                    (main_category,)
                )
                main_id = cursor.fetchone()
                
                if not main_id:
                    return None, None
                
                # 서브 카테고리 ID 조회
                cursor.execute('''
                    SELECT id FROM sub_categories 
                    WHERE main_category_id = ? AND name = ?
                ''', (main_id[0], sub_category))
                
                sub_id = cursor.fetchone()
                
                return main_id[0], sub_id[0] if sub_id else None
                
        except Exception as e:
            print(f"카테고리 ID 조회 중 오류 발생: {str(e)}")
            return None, None

    def add_receipt(self, store_name: str, date: str, items: list, total_amount: int) -> int:
        """영수증 추가 (receipt_id 반환)"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                # 영수증 추가
                cursor.execute('''
                    INSERT INTO receipts (store_name, date, total_amount)
                    VALUES (?, ?, ?)
                ''', (store_name, date, total_amount))
                receipt_id = cursor.lastrowid
                # 품목 추가
                for item in items:
                    main_id, sub_id = self._get_category_ids(
                        item['category'],
                        item['subcategory']
                    )
                    # 자동 유사 매칭 시도
                    if not (main_id and sub_id):
                        valid_subcats = self.valid_categories.get(item['category'], [])
                        best_match = get_close_matches(item['subcategory'], valid_subcats, n=1, cutoff=0.4)
                        if best_match:
                            main_id, sub_id = self._get_category_ids(item['category'], best_match[0])
                    if main_id and sub_id:
                        cursor.execute('''
                            INSERT INTO items (
                                receipt_id, name, main_category_id,
                                sub_category_id, amount
                            )
                            VALUES (?, ?, ?, ?, ?)
                        ''', (
                            receipt_id,
                            item['name'],
                            main_id,
                            sub_id,
                            item['amount']
                        ))
                    else:
                        return None
                conn.commit()
                return receipt_id
        except Exception as e:
            print(f"영수증 추가 중 오류 발생: {str(e)}")
            return None

    def update_receipt(self, receipt_id: int, store_name: str, date: str, items: list, total_amount: int) -> bool:
        """영수증 정보 수정 (receipt_id 기준)"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                # 영수증 정보 업데이트
                cursor.execute('''
                    UPDATE receipts SET store_name = ?, date = ?, total_amount = ? WHERE id = ?
                ''', (store_name, date, total_amount, receipt_id))
                # 기존 품목 삭제 후 재삽입
                cursor.execute('DELETE FROM items WHERE receipt_id = ?', (receipt_id,))
                for item in items:
                    main_id, sub_id = self._get_category_ids(
                        item['category'],
                        item['subcategory']
                    )
                    if not (main_id and sub_id):
                        valid_subcats = self.valid_categories.get(item['category'], [])
                        best_match = get_close_matches(item['subcategory'], valid_subcats, n=1, cutoff=0.4)
                        if best_match:
                            main_id, sub_id = self._get_category_ids(item['category'], best_match[0])
                    if main_id and sub_id:
                        cursor.execute('''
                            INSERT INTO items (
                                receipt_id, name, main_category_id,
                                sub_category_id, amount
                            )
                            VALUES (?, ?, ?, ?, ?)
                        ''', (
                            receipt_id,
                            item['name'],
                            main_id,
                            sub_id,
                            item['amount']
                        ))
                    else:
                        return False
                conn.commit()
                return True
        except Exception as e:
            print(f"영수증 수정 중 오류 발생: {str(e)}")
            return False

    def get_statistics(self, period: str = 'monthly') -> dict:
        """통계 데이터 조회"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 기간별 날짜 포맷 설정
                date_format = {
                    'daily': '%Y-%m-%d',
                    'monthly': '%Y-%m',
                    'yearly': '%Y'
                }.get(period, '%Y-%m')
                
                # 통계 쿼리
                cursor.execute(f"""
                    SELECT 
                        strftime('{date_format}', r.date) as period,
                        mc.name as main_category,
                        sc.name as sub_category,
                        COUNT(*) as count,
                        SUM(i.amount) as total_amount
                    FROM receipts r
                    JOIN items i ON r.id = i.receipt_id
                    JOIN main_categories mc ON i.main_category_id = mc.id
                    JOIN sub_categories sc ON i.sub_category_id = sc.id
                    GROUP BY period, main_category, sub_category
                    ORDER BY period DESC, total_amount DESC
                """)
                
                results = cursor.fetchall()
                
                # 결과 구조화
                stats = {}
                for row in results:
                    period, main_cat, sub_cat, count, total = row
                    
                    if period not in stats:
                        stats[period] = {
                            'total_amount': 0,
                            'categories': {}
                        }
                    
                    if main_cat not in stats[period]['categories']:
                        stats[period]['categories'][main_cat] = {
                            'total_amount': 0,
                            'subcategories': {}
                        }
                    
                    stats[period]['categories'][main_cat]['subcategories'][sub_cat] = {
                        'count': count,
                        'total_amount': total
                    }
                    
                    stats[period]['categories'][main_cat]['total_amount'] += total
                    stats[period]['total_amount'] += total
                
                return stats
                
        except Exception as e:
            print(f"통계 조회 중 오류 발생: {str(e)}")
            return None

    def _call_ollama_api(self, prompt: str) -> str:
        """Ollama API 호출"""
        try:
            # 프롬프트 구성
            full_prompt = f"""다음 영수증 정보를 분석하여 각 품목의 카테고리와 서브카테고리를 분류해주세요.
영수증 정보:
{prompt}

분류 규칙:
1. 반드시 다음 형식을 정확히 지켜주세요:
   가게명: [가게이름]
   날짜: [YYYY-MM-DD]
   [품목명]: [카테고리]:[서브카테고리] ([금액]원)
   총액: [금액]원

2. 카테고리는 다음 중 하나여야 함: 음식, 쇼핑, 교통, 미용, 취미
3. 서브카테고리는 다음 중 하나여야 함:
   - 음식: 음료, 간식, 아침, 점심, 저녁, 디저트, 과일, 유제품
   - 쇼핑: 생필품, 의류, 가전, 가구, 식재료, 장난감, 화장품, 스마트기기
   - 교통: 택시, 대중교통, 여객선, 기차, 항공기, 주유
   - 미용: 피부 미용, 헤어, 화장, 세정
   - 취미: 게임, 영화, 공연, 놀이공원, 운동, 도서

4. 추가 설명이나 다른 내용은 절대 포함하지 마세요
5. 반드시 지정된 형식으로만 출력하세요

예시 출력:
가게명: 스타벅스
날짜: 2024-03-15
아메리카노: 음식:음료 (4,500원)
카페라떼: 음식:음료 (5,000원)
총액: 9,500원"""

            # API 요청 데이터
            data = {
                "model": "antegral/llama-varco",
                "messages": [
                    {
                        "role": "system",
                        "content": "당신은 영수증 분류 전문가입니다. 반드시 지정된 형식으로만 출력하세요. 추가 설명이나 다른 내용은 절대 포함하지 마세요."
                    },
                    {
                        "role": "user",
                        "content": full_prompt
                    }
                ],
                "stream": True,
                "options": {
                    "temperature": 0.1,
                    "num_predict": 100,
                    "top_k": 1,
                    "top_p": 0.1
                }
            }
            
            # API 호출
            response = requests.post(
                self.ollama_url,
                json=data,
                stream=True,
                timeout=60
            )
            
            if response.status_code == 200:
                full_response = ""
                for line in response.iter_lines():
                    if line:
                        try:
                            json_response = json.loads(line)
                            if 'message' in json_response:
                                content = json_response['message'].get('content', '')
                                full_response += content
                                if json_response.get('done', False):
                                    break
                        except json.JSONDecodeError:
                            continue
                return full_response.strip()
            else:
                print(f"API 호출 실패: {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"API 호출 중 오류 발생: {str(e)}")
            return None
        except Exception as e:
            print(f"처리 중 오류 발생: {str(e)}")
            return None
    
    def _parse_store_name(self, line: str) -> str:
        """가게명 파싱"""
        print(f"가게명 파싱 시도: {line}")
        # "가게명: " 형식 제거
        line = line.replace("가게명:", "").strip()
        # 날짜가 포함된 경우 제거 (YYYY-MM-DD 형식)
        line = re.sub(r'\d{4}-\d{2}-\d{2}', '', line).strip()
        print(f"파싱된 가게명: {line}")
        return line

    def _parse_date(self, line: str) -> str:
        """날짜 파싱"""
        print(f"날짜 파싱 시도: {line}")
        # "날짜: " 형식 제거
        line = line.replace("날짜:", "").strip()
        # YYYY-MM-DD 형식의 날짜 추출
        match = re.search(r'\d{4}-\d{2}-\d{2}', line)
        if match:
            date_str = match.group()
            print(f"파싱된 날짜: {date_str}")
            return date_str
        print("날짜를 찾을 수 없음")
        return None

    def process_receipt(self, receipt_text: str) -> bool:
        """영수증 처리"""
        try:
            # API 호출
            response = self._call_ollama_api(receipt_text)
            if not response:
                return False
                
            print("\n파싱 과정:")
            lines = response.strip().split('\n')
            
            # 필수 정보 초기화
            store_name = None
            date = None
            items = []
            total_amount = None
            
            # 총액 관련 패턴 추가
            total_patterns = [
                r'총\s*결제\s*금액\s*[:：]?\s*([\d,\s]+)',
                r'총결제금액\s*[:：]?\s*([\d,\s]+)',
                r'합계\s*[:：]?\s*([\d,\s]+)',
                r'총액\s*[:：]?\s*([\d,\s]+)',
                r'결제금액\s*[:：]?\s*([\d,\s]+)',
                r'총금액\s*[:：]?\s*([\d,\s]+)'
            ]
            
            # 각 줄 처리
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                    
                print(f"처리 중인 줄: {line}")
                
                # 가게명 파싱
                if not store_name and ("가게명:" in line or not any(c.isdigit() for c in line)):
                    store_name = self._parse_store_name(line)
                    if store_name:
                        continue
                
                # 날짜 파싱
                if not date and ("날짜:" in line or re.search(r'\d{4}-\d{2}-\d{2}', line)):
                    date = self._parse_date(line)
                    if date:
                        continue
                
                # 품목 파싱
                if ":" in line and "(" in line and ")" in line:
                    parts = line.split(":")
                    if len(parts) >= 3:
                        item_name = parts[0].strip()
                        category = parts[1].strip()
                        subcategory = parts[2].split("(")[0].strip()
                        amount_str = re.search(r'\(([^)]+)\)', line)
                        
                        if amount_str:
                            amount = int(amount_str.group(1).replace(",", "").replace("원", ""))
                            if self._is_valid_category(category, subcategory):
                                items.append({
                                    "name": item_name,
                                    "category": category,
                                    "subcategory": subcategory,
                                    "amount": amount
                                })
                                print(f"유효한 카테고리 찾음: {category}:{subcategory}")
                            else:
                                print(f"유효하지 않은 카테고리: {category}:{subcategory}")
                
                # 총액 파싱 (패턴 반복)
                for pattern in total_patterns:
                    match = re.search(pattern, line)
                    if match:
                        total_amount = int(match.group(1).replace(",", "").replace(" ", ""))
                        print(f"총액 추출: {total_amount}")
                        break
                if total_amount:
                    continue
            
            # 필수 정보 검증
            if not store_name:
                print("가게명 정보 누락")
                return False
            if not date:
                print("날짜 정보 누락")
                return False
            if not items:
                print("품목 정보 누락")
                return False
            if not total_amount:
                # Ollama 응답에 총액이 없으면, OCR 원본에서 직접 추출 시도
                ocr_total_patterns = [
                    r'총\s*결제\s*금액\s*[:：]?\s*([\d,\s]+)',
                    r'총결제금액\s*[:：]?\s*([\d,\s]+)',
                    r'합계\s*[:：]?\s*([\d,\s]+)',
                    r'총액\s*[:：]?\s*([\d,\s]+)',
                    r'결제금액\s*[:：]?\s*([\d,\s]+)',
                    r'총금액\s*[:：]?\s*([\d,\s]+)'
                ]
                for pattern in ocr_total_patterns:
                    match = re.search(pattern, receipt_text)
                    if match:
                        total_amount = int(match.group(1).replace(",", "").replace(" ", ""))
                        print(f'OCR 원본에서 총액 보완 추출: {total_amount}')
                        break
                if not total_amount:
                    print("총액 정보 누락")
                    return False
            
            # DB에 저장 (add_receipt 함수 사용)
            result = self.add_receipt(store_name, date, items, total_amount)
            if result:
                print("처리 결과: 성공 (DB에 저장됨)")
                return True
            else:
                print("DB 저장 실패")
                return False
                
        except Exception as e:
            print(f"처리 중 오류 발생: {str(e)}")
            return False
    
    def get_receipts(self):
        """
        저장된 모든 영수증 데이터를 데이터베이스에서 조회하여 반환합니다.
        각 영수증에 연결된 카테고리 및 서브카테고리 이름도 함께 가져옵니다.
        
        Returns:
            list: 각 영수증이 딕셔너리 형태로 담긴 리스트.
                  (예: [{'store_name': '가게명', 'date': '날짜', 'items': [...], 'category': '카테고리', 'subcategory': '서브카테고리'}, ...])
        """
        try:
            # 데이터베이스에 연결 (timeout=10초 설정)
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cursor = conn.cursor()
                
                # 영수증, 카테고리, 서브카테고리 정보를 조인하여 조회
                cursor.execute("""
                    SELECT store_name, date, total_amount, category, subcategory
                    FROM receipts
                    ORDER BY date DESC
                """)
                
                receipts = []
                for row in cursor.fetchall():
                    store_name, date, total_amount, category, subcategory = row
                    
                    # 각 영수증 정보에 카테고리 정보 추가
                    receipt = {
                        'store_name': store_name,
                        'date': date,
                        'total_amount': total_amount,
                        'category': category,
                        'subcategory': subcategory
                    }
                    receipts.append(receipt)
                
                return receipts
                
        except sqlite3.Error as e:
            print(f"데이터베이스 오류: {e}")
            return []
    
    def get_expense_analysis(self, period: str = 'monthly') -> dict:
        """지출 분석 데이터 조회"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 기간별 날짜 포맷 설정
                date_format = {
                    'daily': '%Y-%m-%d',
                    'monthly': '%Y-%m',
                    'yearly': '%Y'
                }.get(period, '%Y-%m')
                
                # 카테고리별 지출 통계
                cursor.execute(f"""
                    SELECT 
                        strftime('{date_format}', date) as period,
                        category,
                        subcategory,
                        COUNT(*) as count,
                        SUM(amount) as total_amount,
                        AVG(amount) as avg_amount
                    FROM receipts
                    GROUP BY period, category, subcategory
                    ORDER BY period DESC, total_amount DESC
                """)
                
                results = cursor.fetchall()
                
                # 결과 구조화
                analysis = {}
                for row in results:
                    period, category, subcategory, count, total, avg = row
                    
                    if period not in analysis:
                        analysis[period] = {
                            'total_amount': 0,
                            'categories': {}
                        }
                    
                    if category not in analysis[period]['categories']:
                        analysis[period]['categories'][category] = {
                            'total_amount': 0,
                            'subcategories': {}
                        }
                    
                    analysis[period]['categories'][category]['subcategories'][subcategory] = {
                        'count': count,
                        'total_amount': total,
                        'avg_amount': avg
                    }
                    
                    analysis[period]['categories'][category]['total_amount'] += total
                    analysis[period]['total_amount'] += total
                
                return analysis
                
        except Exception as e:
            print(f"지출 분석 중 오류 발생: {str(e)}")
            return None

    def get_expense_trends(self, category: str = None, period: str = 'monthly') -> dict:
        """지출 추이 분석"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 기간별 날짜 포맷 설정
                date_format = {
                    'daily': '%Y-%m-%d',
                    'monthly': '%Y-%m',
                    'yearly': '%Y'
                }.get(period, '%Y-%m')
                
                # 기본 쿼리
                query = f"""
                    SELECT 
                        strftime('{date_format}', date) as period,
                        SUM(amount) as total_amount
                    FROM receipts
                """
                
                # 카테고리 필터 추가
                if category:
                    query += f" WHERE category = '{category}'"
                
                query += " GROUP BY period ORDER BY period"
                
                cursor.execute(query)
                results = cursor.fetchall()
                
                # 결과 구조화
                trends = {
                    'periods': [],
                    'amounts': [],
                    'total': 0
                }
                
                for period, amount in results:
                    trends['periods'].append(period)
                    trends['amounts'].append(amount)
                    trends['total'] += amount
                
                return trends
                
        except Exception as e:
            print(f"지출 추이 분석 중 오류 발생: {str(e)}")
            return None

    def get_category_insights(self) -> dict:
        """카테고리별 인사이트"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 카테고리별 통계
                cursor.execute("""
                    SELECT 
                        category,
                        COUNT(*) as count,
                        SUM(amount) as total_amount,
                        AVG(amount) as avg_amount,
                        MIN(amount) as min_amount,
                        MAX(amount) as max_amount
                    FROM receipts
                    GROUP BY category
                """)
                
                results = cursor.fetchall()
                
                # 결과 구조화
                insights = {}
                for row in results:
                    category, count, total, avg, min_amt, max_amt = row
                    insights[category] = {
                        'count': count,
                        'total_amount': total,
                        'avg_amount': avg,
                        'min_amount': min_amt,
                        'max_amount': max_amt
                    }
                
                return insights
                
        except Exception as e:
            print(f"카테고리 인사이트 분석 중 오류 발생: {str(e)}")
            return None

    def __del__(self):
        """소멸자: 데이터베이스 연결 종료 (특별히 할 작업 없음) """
        pass

    def _is_valid_category(self, category: str, subcategory: str) -> bool:
        """카테고리와 서브카테고리 유효성 검사"""
        # 카테고리 매핑
        valid_categories = {
            '음식': ['음료', '간식', '아침', '점심', '저녁', '디저트', '과일', '유제품'],
            '쇼핑': ['생필품', '의류', '가전', '가구', '식재료', '장난감', '화장품', '스마트기기'],
            '교통': ['택시', '대중교통', '여객선', '기차', '항공기', '주유'],
            '미용': ['피부 미용', '헤어', '화장', '세정'],
            '취미': ['게임', '영화', '공연', '놀이공원', '운동', '도서']
        }
        
        # 카테고리와 서브카테고리에서 공백 제거
        category = category.strip()
        subcategory = subcategory.strip()
        
        # 카테고리 존재 여부 확인
        if category not in valid_categories:
            return False
            
        # 서브카테고리 존재 여부 확인
        return subcategory in valid_categories[category]
