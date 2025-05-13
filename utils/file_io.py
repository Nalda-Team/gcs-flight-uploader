import json
import os
def read_json_file(file_path):
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        return json.load(f)


def dump_json_file(file_path, data):
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def remove_done_json(file_path):
    if os.path.exists(file_path):
        # 파일 삭제
        os.remove(file_path)