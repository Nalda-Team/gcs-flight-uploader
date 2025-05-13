from google.cloud import bigquery
from datetime import datetime
from zoneinfo import ZoneInfo

class Big_Query_Logger:

    def __init__(self, project_id, success_table_id, error_table_id, instance_id):
        self.project_id=project_id
        self.client=bigquery.Client(project=project_id)
        self.success_table_id=success_table_id
        self.error_table_id=error_table_id
        self.instance_id=instance_id
    

    def insert_success_log(self, route_key,target_date,seat_class, total_duration, status):
        row = {
            'instance_id': self.instance_id,
            'route_key': route_key,
            'target_date': target_date,
            'seat_class': seat_class,
            'created_at': datetime.now(ZoneInfo("Asia/Seoul")).isoformat(),
            'total_duration': total_duration,
            'status' : status
        }
        errors = self.client.insert_rows_json(self.success_table_id, [row])
        if len(errors)!=0:
            print(errors)
        # print(errors)

    def insert_error_log(self, route_key, target_date, seat_class,error_func, error_log):
        
        row = {
            'instance_id': self.instance_id,
            'route_key': route_key,
            'target_date': target_date,
            'seat_class': seat_class,
            'created_at': datetime.now(ZoneInfo("Asia/Seoul")).isoformat(),
            'error_func': error_func,
            'error_log': error_log
        }
        
        errors = self.client.insert_rows_json(self.error_table_id, [row])