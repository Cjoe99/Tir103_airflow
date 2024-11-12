#--------------------airflow---------------------------
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
#----------------crawl---------------------------------
from time import sleep
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
# from selenium.common.exceptions import NoSuchElementException
#----------------------MySQL--------------------------------------
import pymysql
def selenium_scraper():
    #--------mysql-------------------------------
    connection = pymysql.connect(
        host='IP',  # 主機名稱
        port=3306, # 指定 MySQL 使用的端口號
        user='XXXX',  # 資料庫使用者名稱
        password='XXXX',  # 資料庫密碼
        database='DCARD',  # 資料庫名稱
        charset='utf8mb4',  # 使用的編碼
        )
    # 建立游標
    cursor = connection.cursor()
    #-----------------爬蟲----------------------------------
    # 設定 ChromeOptions
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # 無頭模式，沒有 GUI
    options.add_argument('--no-sandbox')
    options.add_argument("--window-size=1920,1080")
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-software-rasterizer')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    def crawl_home_url(head, dcard_url, num_articles):
        # 使用 webdriver_manager 安裝 ChromeDriver，並讓 WebDriver 自動找到
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        # 前往dcard 首頁
        driver.get("https://www.google.com")
        search = driver.find_element(By.NAME, "q")
        search.send_keys(f"{dcard_url}")
        search.send_keys(Keys.ENTER)
        sleep(10)  # 最多等待 10 秒
        driver.find_element(By.XPATH, f"//*[@id='rso']/div[1]/div/div/div/div/div/div/div/div[1]/div/span/a[@href='{dcard_url}']").click()
        # 等待頁面載入
        sleep(10)
        for i in range(head, num_articles+head):
            # 預防爬的過程出問題，還是可以把爬到的存下來
            try:
                # if i == block:
                #     continue
                while True:
                    try:
                        # 定位連結位址
                        element_by_data_key = driver.find_element(By.XPATH, f"//div[@data-key='{i}']")
                        break
                    # 當抓不到data_key就往下滑動一點，直到找到為止       
                    except Exception:
                        driver.execute_script("window.scrollBy(0, 500);")
                # 抓取連結
                url = element_by_data_key.find_element(By.CLASS_NAME, "t1gihpsa").get_attribute("href")
                # 抓取文章ID
                article_ID = url.split('/')[-1]
                # 設定爬取日期
                current_date = datetime.now().date()
                # 將data insert 到 MySQL
                insert = """insert ignore into article_confirm(ID, Date, Url)
                                values(%s, %s, %s)"""
                data = (article_ID, current_date, f"{url}")
                cursor.execute(insert, data)
                connection.commit()
                print(f"成功抓取: {url}")
                # 頁面向下滾動
                driver.execute_script("arguments[0].scrollIntoView({block:'start'});", element_by_data_key)
                sleep(1)
            except Exception:
                continue
        driver.quit()
    
    def crawl_hot_url(block, head, dcard_url, num_articles):
        # 使用 undetected_chromedriver 初始化 Chrome 瀏覽器 (用來爬標題和連結)
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        # 前往dcard 首頁
        driver.get("https://www.google.com")
        search = driver.find_element(By.NAME, "q")
        search.send_keys(f"{dcard_url}")
        search.send_keys(Keys.ENTER)
        sleep(10)  # 最多等待 10 秒
        driver.find_element(By.XPATH, f"//*[@id='rso']/div[1]/div/div/div/div/div/div/div/div[1]/div/span/a[@href='{dcard_url}']").click()
        # 等待頁面載入
        sleep(10)
        for i in range(head, num_articles+head):
            # 預防爬的過程出問題，還是可以把爬到的存下來
            try:
                if i == block:
                    continue
                while True:
                    try:
                        # 定位連結位址
                        element_by_data_key = driver.find_element(By.XPATH, f"//div[@data-key='{i}']")
                        break
                    # 當抓不到data_key就往下滑動一點，直到找到為止       
                    except Exception:
                        driver.execute_script("window.scrollBy(0, 500);")
                # 抓取連結
                url = element_by_data_key.find_element(By.CLASS_NAME, "t1gihpsa").get_attribute("href")
                # 抓取文章ID
                article_ID = url.split('/')[-1]
                # 設定爬取日期
                current_date = datetime.now().date()
                # 將data insert 到 MySQL
                insert = """insert ignore into article_confirm(ID, Date, Url)
                                values(%s, %s, %s)"""
                data = (article_ID, current_date, f"{url}")
                cursor.execute(insert, data)
                connection.commit()
                print(f"成功抓取: {url}")
                # 頁面向下滾動
                driver.execute_script("arguments[0].scrollIntoView({block:'start'});", element_by_data_key)
                sleep(1)
            except Exception:
                continue
        driver.quit()
    dcard_tech = "https://www.dcard.tw/f/tech_job"
    dcard_trending = "https://www.dcard.tw/f/trending"
    dcard_home = "https://www.dcard.tw/f"
    # dcard首頁
    crawl_home_url(0, dcard_home, 1)
    sleep(10)
    # dcard 時事版(熱門)
    crawl_hot_url(9, 2, dcard_trending, 1)
    sleep(10)
    # dcard 科技版(熱門)
    crawl_hot_url(7, 1, dcard_tech, 1)
    sleep(10)

# 設置默認參數
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 定義 DAG
with DAG(
    'dcard_url',
    default_args=default_args,
    description='A simple Selenium DAG',
    schedule_interval=timedelta(days=1),  # 每天運行一次
    catchup=False,
) as dag:

    run_selenium_task = PythonOperator(
        task_id='run_selenium_task',
        python_callable=selenium_scraper
    )