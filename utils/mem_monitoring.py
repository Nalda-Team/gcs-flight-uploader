import psutil

def mem_monitoring():
    # 전체 시스템 메모리와 스왑 사용량 정보 가져오기
    virtual_memory = psutil.virtual_memory()
    swap_memory = psutil.swap_memory()
    
    # # 디버깅 정보 출력
    # print('== RAM 정보 ==')
    # print(f'RAM 총량: {virtual_memory.total} 바이트')
    # print(f'RAM 사용량: {virtual_memory.used} 바이트')
    # print(f'RAM 사용률: {virtual_memory.percent}%')
    
    # print('\n== 스왑 정보 ==')
    # print(f'스왑 총량: {swap_memory.total} 바이트')
    # print(f'스왑 사용량: {swap_memory.used} 바이트')
    # print(f'스왑 사용률: {swap_memory.percent}%')
    # print('\n== 총합 정보 ==')
    # print(f'총 메모리(RAM+스왑): {total_memory} 바이트')
    # print(f'사용중인 메모리(RAM+스왑): {used_memory} 바이트')
    # print(f'총 메모리 사용률: {round(memory_usage_percent, 2)}%')
    
    # 전체 메모리 + 스왑 사용량 비율 계산
    total_memory = virtual_memory.total + swap_memory.total
    used_memory = virtual_memory.used + swap_memory.used
    
    # 전체 메모리 사용 비율
    memory_usage_percent = (used_memory / total_memory) * 100
    
    
    if memory_usage_percent < 80:
        return True
    else:
        # 디버깅 정보 출력
        print('== RAM 정보 ==')
        print(f'RAM 총량: {virtual_memory.total} 바이트')
        print(f'RAM 사용량: {virtual_memory.used} 바이트')
        print(f'RAM 사용률: {virtual_memory.percent}%')
        
        print('\n== 스왑 정보 ==')
        print(f'스왑 총량: {swap_memory.total} 바이트')
        print(f'스왑 사용량: {swap_memory.used} 바이트')
        print(f'스왑 사용률: {swap_memory.percent}%')
        return False