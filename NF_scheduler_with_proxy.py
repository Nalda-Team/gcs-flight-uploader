import threading
import time
import sys
import os
from datetime import datetime
from utils.mem_monitoring import mem_monitoring
sys.stdout.reconfigure(line_buffering=True)
import traceback
from queue import Queue
from utils.multi_request import send_request
from config.api_params import return_header, domastic_payload_form, international_payload_form
from NF_global_objects import (
    get_today,
    get_airport_map,
    get_bigquery_logger,
    get_schedule_params, 
    get_next_proxy, 
    finish_schedule_processing, 
    requeue_proxy,remove_from_processing,
    get_max_thread_count,
    check_all_schedules_done,
    increment_crawler_cnt,
    decrement_crawler_cnt,
    crawler_running_check,
    upload_fetched_flight_date
)

def check_response(response, is_domestic):
    # 재시도 필요 X
    if not response:
        return None
    
    # 정상적인 응답값인지 확인
    if is_domestic:
        try:
            schedules = response['data']['domesticFlights']['departures']
            return response
        except Exception as e:
            # print(e)
            # print(response)
            return None
    else:
        try:
            schedules = response['data']['internationalList']['results']['schedules']
            return response
        except Exception as e:
            # print(e)
            # print(response)
            return None
    
    
class CrawlerManager:
    def __init__(self):
        self.threads = []
        self.worker_done_events = []
        self.response_queue = Queue()
        
        # 전역 객체들 초기화
        self.airport_map = get_airport_map()
        self.today = get_today()
        self.logger = get_bigquery_logger()
        self.seat_class_map = {'Y':'일반석', 'C':'비즈니스석','YC':'일반/비즈니스'}

    def multi_request_worker(self, worker_done_event):
        """워커 스레드 함수"""
        thread_id = threading.current_thread().name
        while True:
            if not mem_monitoring():  # 메모리 사용량이 높으면
                time.sleep(10)  # 잠시 대기
                continue
            
            process_start = time.time()
            proxy = get_next_proxy()
            schedule = get_schedule_params()
            
            # 프록시 없음 -> 종료
            if not proxy:
                worker_done_event.set()
                break
            
            # 스케줄 처리 다 안됐는데 대기큐에서 스케줄 못가져옴 -> 다른 스레드에서 실패할 수 있음 -> 다시 대기
            if not schedule:
                if check_all_schedules_done():
                    worker_done_event.set()
                    break
                else:
                    requeue_proxy(proxy=proxy)
                    time.sleep(1)
                    continue
            
            depart_airport = schedule['depart_airport']
            arrival_airport = schedule['arrival_airport']
            is_domestic = schedule['is_domestic']
            seat_class = schedule['seat_class']
            target_date = schedule['target_date']
            route_key = depart_airport.upper() + '_' + arrival_airport.upper()

            if is_domestic:
                self._process_domestic_flight(thread_id, schedule, proxy, process_start, 
                                            depart_airport, arrival_airport, target_date, 
                                            seat_class, route_key, is_domestic)
            else:
                self._process_international_flight(thread_id, schedule, proxy, process_start, 
                                                depart_airport, arrival_airport, target_date, 
                                                seat_class, route_key, is_domestic)



    def _process_domestic_flight(self, thread_id, schedule, proxy, process_start, 
                               depart_airport, arrival_airport, target_date, 
                               seat_class, route_key, is_domestic):
        """국내선 항공편 처리"""
        headers = return_header(is_domestic, depart_airport, arrival_airport, target_date, seat_class)
        payload = domastic_payload_form(departure=depart_airport, arrival=arrival_airport, 
                                    date=target_date, fare_type=seat_class)
        
        # API 요청 시간 측정
        api_start = time.time()
        response = send_request(payload=payload, headers=headers, proxy=proxy)
        response = check_response(response, is_domestic)
        api_end = time.time()
        api_duration = api_end - api_start
        
        if response:
            target_date=datetime.strptime(target_date, '%Y%m%d').date().isoformat()

            schedules = response['data']['domesticFlights']['departures']

            # 일정이 없으면 'N'로 로깅하고 return
            if len(schedules)==0:
                finish_schedule_processing(schedule, success=True)
                print('일정이 없습니다!')
                process_end = time.time()
                duration = process_end - process_start
                self.logger.insert_success_log(route_key=route_key, target_date=target_date, seat_class=self.seat_class_map[seat_class], total_duration=int(duration), status='N')
                requeue_proxy(proxy=proxy)
                return 0
            
            # GCS 저장 경로
            upload_dir = self.today.strftime('%Y-%m-%d')
            filename = f'{target_date}_{depart_airport}_{arrival_airport}_{seat_class}_{"domestic" if is_domestic else "international"}.json'
            destination_blob = os.path.join(upload_dir, 'domestic', filename)

            # GCS로 업로드
            upload_start = time.time()
            upload_fetched_flight_date(bucket_name='fetched_flight_data_bucket', json_data=response, destination_blob=destination_blob)
            finish_schedule_processing(schedule, success=True)
            upload_end = time.time()
            upload_duration = upload_end - upload_start
            process_end = time.time()
            duration = process_end - process_start
            self.logger.insert_success_log(route_key=route_key, target_date=target_date, seat_class=seat_class, total_duration=int(duration), status='Y')
            requeue_proxy(proxy=proxy)
            
            # 기타 시간 계산
            other_duration = duration - (api_duration + upload_duration)               
            print(f'GCS 업로드 완료: {destination_blob}, '
                    f'(총 소요시간: {int(duration)}초')
                #   f'API 요청 소요시간: {int(api_duration)}초, '
                #   f'업로드 소요시간: {int(upload_duration)}초, '
        else:
            finish_schedule_processing(schedule, success=False)
            requeue_proxy(proxy=proxy) # if response=='retry' else remove_from_processing(proxy=proxy)


    def _process_international_flight(self, thread_id, schedule, proxy, process_start, 
                                    depart_airport, arrival_airport, target_date, 
                                    seat_class, route_key, is_domestic):
        """국제선 항공편 처리"""
        headers = return_header(is_domestic, depart_airport, arrival_airport, target_date, seat_class)
        payload1 = international_payload_form(first=True, departure=depart_airport, arrival=arrival_airport, 
                                           date=target_date, fare_type=seat_class)
        
        # 첫 번째 API 요청 시간 측정
        api1_start = time.time()
        response = send_request(payload=payload1, headers=headers, proxy=proxy)
        response = check_response(response, is_domestic)
        api1_end = time.time()
        
        # 대기 시간
        time.sleep(3)
        
        if response :
            international_list = response.get("data", {}).get("internationalList", {})
            galileo_key = international_list.get("galileoKey")
            travel_biz_key = international_list.get("travelBizKey")
            
            payload2 = international_payload_form(
                first=False,
                departure=depart_airport,
                arrival=arrival_airport,
                date=target_date,
                fare_type=seat_class,
                galileo_key=galileo_key,
                travel_biz_key=travel_biz_key
            )
            
            # 두 번째 API 요청 시간 측정
            api2_start = time.time()
            response = send_request(payload=payload2, headers=headers, proxy=proxy)
            response = check_response(response, is_domestic)
            api2_end = time.time()
            
            # 총 API 요청 시간
            api_duration = (api1_end - api1_start) + (api2_end - api2_start)
            
            if response :
                target_date=datetime.strptime(target_date, '%Y%m%d').date().isoformat()
                
                schedules = response['data']['internationalList']['results']['schedules']
                if len(schedules)==0:
                    finish_schedule_processing(schedule, success=True)
                    print('일정이 없습니다!')
                    process_end = time.time()
                    duration = process_end - process_start
                    self.logger.insert_success_log(route_key=route_key, target_date=target_date, seat_class=self.seat_class_map[seat_class], total_duration=int(duration), status='N')
                    requeue_proxy(proxy=proxy)
                    return 0
                
                         
                # GCS 저장 경로
                upload_dir = self.today.strftime('%Y-%m-%d')
                
                filename = f'{target_date}_{depart_airport}_{arrival_airport}_{seat_class}_{"domestic" if is_domestic else "international"}.json'
                destination_blob = os.path.join(upload_dir,'international', filename)

                # GCS로 업로드
                upload_start = time.time()
                upload_fetched_flight_date(bucket_name='fetched_flight_data_bucket', json_data=response, destination_blob=destination_blob)
                upload_end = time.time()
                upload_duration = upload_end - upload_start
                
                process_end = time.time()
                duration = process_end - process_start
                finish_schedule_processing(schedule, success=True)
                self.logger.insert_success_log(route_key=route_key, target_date=target_date, seat_class=seat_class, total_duration=int(duration), status='Y')
                requeue_proxy(proxy=proxy)
            
            
                # 기타 시간 계산 (sleep 포함)
                other_duration = duration - (api_duration + upload_duration)
            
                print(f'GCS 업로드 완료: {destination_blob}, '
                        f'(총 소요시간: {int(duration)}초')
                    #   f'API 요청 소요시간: {int(api_duration)}초, '
                    #   f'업로드 소요시간: {int(upload_duration)}초, '
                    #   f'time sleep: {3}초, '
                    #   f'기타 소요시간: {int(other_duration)}초)')
            else:
                finish_schedule_processing(schedule, success=False)
                requeue_proxy(proxy=proxy) # if response=='retry' else remove_from_processing(proxy=proxy)
                
        else:
            finish_schedule_processing(schedule, success=False)
            requeue_proxy(proxy=proxy) # if response=='retry' else remove_from_processing(proxy=proxy)
            

    def start_workers(self, num_workers):
        """워커 스레드들 시작"""
        for i in range(num_workers):
            worker_done_event = threading.Event()
            t = threading.Thread(
                target=self.multi_request_worker, 
                args=(worker_done_event,),
                name=f"Worker-{i+1}"
            )
            self.threads.append(t)
            self.worker_done_events.append(worker_done_event)
            t.start()
        return self.threads, self.worker_done_events

    def wait_for_request_threads(self):
        """모든 워커 스레드 완료 대기"""
        for t, event in zip(self.threads, self.worker_done_events):
            event.wait()

    def run(self):
        """크롤러 실행"""
        try:
            num_workers = get_max_thread_count()
            # 워커 스레드 시작
            self.start_workers(num_workers)

            # 워커 스레드 완료 대기
            self.wait_for_request_threads()

        except Exception as e:
            print(f"크롤러 실행 중 오류 발생: {e}")
            print(traceback.format_exc())

if __name__ == "__main__":
    crawler = CrawlerManager()
    crawler_running_check()
    increment_crawler_cnt() # 실행중인 크롤러 수 1증가
    crawler.run()
    decrement_crawler_cnt() # 크롤러 수 감소
    