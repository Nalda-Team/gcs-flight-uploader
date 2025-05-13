import os
from datetime import datetime
from zoneinfo import ZoneInfo
from utils.file_io import read_json_file
from config.big_query_logger import Big_Query_Logger
from database.redis_manager import Redis_Manager
from storage.storage_manager import Storage_Manager
import time
import random
from dotenv import load_dotenv

class GlobalObjects:
    new_instance = None

    def __new__(cls):
        if cls.new_instance is None:
            cls.new_instance = super().__new__(cls)
            cls.new_instance._initialize()
        return cls.new_instance

    def _initialize(self):
        load_dotenv()
        self.timezone = ZoneInfo("Asia/Seoul")
        self.today = datetime.now(self.timezone).date()
        print('수집 날짜:', self.today)

        # 레디스 큐 연결
        REDIS_HOST = os.getenv("REDIS_HOST", "****")
        print('REDIS_HOST:', REDIS_HOST)
        REDIS_PORT = os.getenv("REDIS_PORT", "****")
        print('REDIS_PORT:', REDIS_PORT)
        REDIS_DB = os.getenv("REDIS_DB", "0")
        print('REDIS_DB:', REDIS_DB)
        
        self.redis_manager = Redis_Manager(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        print('Redis_Manager 객체 생성 및 연결 완료')

        # 로깅용 빅쿼리 설정
        PROJECT_ID = os.getenv("PROJECT_ID", "****")
        print('PROJECT_ID:', PROJECT_ID)
        SUCCESS_TABLE_ID = os.getenv("SUCCESS_TABLE_ID", "****")
        print('SUCCESS_TABLE_ID:', SUCCESS_TABLE_ID)
        ERROR_TABLE_ID = os.getenv("ERROR_TABLE_ID", "****")
        print('ERROR_TABLE_ID:', ERROR_TABLE_ID)
        INSTANCE_ID = os.getenv("INSTANCE_ID", 'main_pc')
        print('INSTANCE_ID:', INSTANCE_ID)
        
        self.bigquery_logger = Big_Query_Logger(PROJECT_ID, SUCCESS_TABLE_ID, ERROR_TABLE_ID, INSTANCE_ID)
        print('Big_Query_Logger 객체 생성 완료')

        # GCS 버킷 연결
        key_file_path=os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '****')
        self.storage_manager=Storage_Manager(key_file_path=key_file_path)


        # 공항 정보 맵
        self.airport_map = read_json_file('maps/airport_map.json')
        print('airport_map 로드 완료')

        # 배치 큐 초기화
        TOMORROW_FLAG = os.getenv("TOMORROW_FLAG", 'N')
        print('내일 출발 항공권 수집 여부:', TOMORROW_FLAG)

        # 최대 스레드 수
        self.max_thread_count = os.getenv("MAX_THREAD_COUNT", "50")
        print('최대 스레드 수:', self.max_thread_count)
        print('초기화 완료')

# 전역 객체 인스턴스
global_objects = GlobalObjects()

# 크롤링 날짜
def get_today():
    return global_objects.today

# 공항 코드 맵 객체
def get_airport_map():
    return global_objects.airport_map

# 로거
def get_bigquery_logger():
    return global_objects.bigquery_logger

# 스케줄 파라미터 가져오기
def get_schedule_params():
    return global_objects.redis_manager.get_schedule_params()

# 스케줄 처리 완료 (실패시 다시 스케줄 큐에 삽입됨)
def finish_schedule_processing(schedule, success=True):
    global_objects.redis_manager.finish_schedule_processing(schedule, success)

# 프록시 서버 가져오기
def get_next_proxy():
    return global_objects.redis_manager.get_next_proxy()

# 사용한 프록시 서버 반납
def requeue_proxy(proxy):
    random_sec=round(random.randint(1,3))
    random_sec=1
    time.sleep(random_sec)
    global_objects.redis_manager.requeue_proxy(proxy)

def remove_from_processing(proxy):
    global_objects.redis_manager.remove_from_processing(proxy)
    pass

def get_proxy_queue_count():
    main_queue_count, processing_queue_count, total_count = global_objects.redis_manager.get_proxy_queue_count()
    return main_queue_count, processing_queue_count, total_count

# 전역 변수 객체 그자체
def get_global_object():
    return global_objects

def get_max_thread_count():
    return int(global_objects.max_thread_count)

def increment_crawler_cnt():
    print('실행중인 크롤러 수 증가 전:', global_objects.redis_manager.get_crawler_cnt())
    global_objects.redis_manager.increment_crawler_cnt()
    print('실행중인 크롤러 수 증가 후:', global_objects.redis_manager.get_crawler_cnt())

def decrement_crawler_cnt():
    print('실행중인 크롤러 수 감소 전:', global_objects.redis_manager.get_crawler_cnt())
    global_objects.redis_manager.decrement_crawler_cnt()
    print('실행중인 크롤러 수 감소 후:', global_objects.redis_manager.get_crawler_cnt())

def check_all_schedules_done():
    # 스케줄 처리 완료
    if global_objects.redis_manager.check_all_schedules_processed():
        global_objects.redis_manager.set_crawling_completed()
        return True
    else:
        return False

def crawler_running_check():
    global_objects.redis_manager.reset_crawling_status()


def upload_fetched_flight_date(bucket_name, json_data, destination_blob):
    global_objects.storage_manager.upload_json_to_gcs(bucket_name, json_data, destination_blob)