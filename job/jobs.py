import sentry_sdk
import redis
import json
import logging
from datetime import datetime, timedelta

#  定义异常捕获
sentry_sdk.init(
    dsn="https://c1258879e6ab76c537abecb0d76ede05@o4504661413462016.ingest.us.sentry.io/4507253282570240",
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    traces_sample_rate=1.0,
    # Set profiles_sample_rate to 1.0 to profile 100%
    # of sampled transactions.
    # We recommend adjusting this value in production.
    profiles_sample_rate=1.0,
)

# 定义redis链接
redis_client = redis.Redis(
  host='redis-17087.c292.ap-southeast-1-1.ec2.cloud.redislabs.com',
  port=17087,
  password='yxl981204')

def calculate_elapsed_time(func):
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        result = func(*args, **kwargs)
        end_time = datetime.now()
        elapsed_time = end_time - start_time
        logging.info(f"Job {func.__name__} finished in {elapsed_time.total_seconds()} seconds.")
        return result
    return wrapper

@calculate_elapsed_time
def job_acc_redis():
    print(777)

@calculate_elapsed_time
def job_test():
    print(666)

def initialize():
    job_data = {
        "job_acc_redis": {"last_run_time": None, "enable": 1, "max_num": 5, "actual_num": 0},
        "job_test": {"last_run_time": None, "enable": 1, "max_num": 10, "actual_num": 0}
    }
    for key, value in job_data.items():
        redis_client.hset("s-job", key, json.dumps(value))

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    
    if not redis_client.exists("s-job"):
        initialize()
    job_data = redis_client.hgetall("s-job")

    for key, value in job_data.items():
        key_str = key.decode("utf-8")  # 将字节字符串解码为字符串
        value_str = value.decode("utf-8")  # 将字节字符串解码为字符串
        
        job_info = json.loads(value_str)
        if job_info["enable"] == 1:
            if job_info["actual_num"] < job_info["max_num"]:
                job_name = key_str
                if job_name in globals() and callable(globals()[job_name]):
                    job_func = globals()[job_name]
                    job_func()
                    job_info["actual_num"] += 1
                    job_info["last_run_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    redis_client.hset("s-job", key, json.dumps(job_info))
                else:
                    print(f"Error: Function {job_name} not found or not callable.")
            else:
                print(f"Warning: Max number of runs reached for job {key_str}.")