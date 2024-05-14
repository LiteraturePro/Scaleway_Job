import sentry_sdk
import redis
import json
import logging
from datetime import datetime
import requests
import os
from botocore.exceptions import NoCredentialsError
import boto3

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
def job_sy_bing():
    # 原始的图片地址
    original_image_urls = [
        "https://jscdn.cachefly.net/BingCdn/2024/01/01/2024-01-01_hd.jpg",
        "https://jscdn.cachefly.net/BingCdn/2024/01/01/2024-01-01_hd_gaussian_20.jpg",
        "https://jscdn.cachefly.net/BingCdn/2024/01/01/2024-01-01_hd_greyscale.jpg",
        "https://jscdn.cachefly.net/BingCdn/2024/01/01/2024-01-01_hd_thumbnail_480_270.jpg",
        "https://jscdn.cachefly.net/BingCdn/2024/01/01/2024-01-01_uhd.jpg"
    ]
    # -------------------------------Cloudflare对象储存服务配置----------------------------------------
    # 空间名称：object
    CF_ACCESS_KEY = 'a98f6ce3d22278a7931ca0343aede9f3'  
    CF_SECRET_KEY = 'b4b9a5364f5d4e9b4d16ed8d75816360fae0e51175cab8e1f23f68452ab85c0e'  
    CF_s3 = boto3.resource('s3',
        endpoint_url = 'https://e680e4488fdd6d1b8c827cba29dc3410.r2.cloudflarestorage.com',
        aws_access_key_id = CF_ACCESS_KEY,
        aws_secret_access_key = CF_SECRET_KEY
    )

    # S3对象存储服务配置
    s3_bucket_name = "oss"
    s3_base_path = "BingCdn"
    # 日期范围
    #start_date = datetime.date(2024, 1, 1)
    #end_date = datetime.date(2025, 1, 1)
    start_date = datetime.date.today()
    end_date = datetime.date.today()
    # 构建待更新的图片地址
    updated_image_urls = []
    for date in (start_date + datetime.timedelta(days=n) for n in range((end_date - start_date).days + 1)):
        for image_url in original_image_urls:
            image_name = os.path.basename(image_url)
            image_name = image_name.replace("2024-01-01", str(date))
            image_path = os.path.join(s3_base_path, str(date.year), str(date.month).zfill(2), str(date.day).zfill(2), image_name)
            #updated_image_urls.append("https://jscdn.cachefly.net/"+image_path)
            updated_image_urls.append(("https://jscdn.cachefly.net/"+image_path, image_path))
    # 请求待更新的图片地址并更新到S3对象存储服务
    for image_url, image_path in updated_image_urls:
        response = requests.head(image_url)
        if response.status_code == 404:
            original_url = image_url.replace("jscdn.cachefly.net/BingCdn/", "bing.mcloc.cn/img/")
            response = requests.head(original_url)
            if response.status_code == 200:
                try:
                    image_name = os.path.basename(image_path)
                    response = requests.get(original_url)
                    with open(image_name, 'wb') as f:
                        f.write(response.content)
                    s3_bucket = CF_s3.Bucket(s3_bucket_name)
                    s3_bucket.upload_file(image_name, image_path)
                    os.remove(image_name)
                    print(f"Image {image_name} uploaded to S3")
                except NoCredentialsError:
                    print("Credentials not found.")
            else:
                print(f"Original image {original_url} not found.")
        else:
            print(f"Image {image_path} already exists.")

@calculate_elapsed_time
def job_test():
    print(666)

def initialize():
    job_data = {
        "job_sy_bing": {"last_run_time": None, "enable": 1, "max_num": 5, "actual_num": 0},
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