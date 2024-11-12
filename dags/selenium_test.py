
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

def selenium_scraper():
    # 設定 ChromeOptions
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # 無頭模式，沒有 GUI
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    # 使用 webdriver_manager 安裝 ChromeDriver，並讓 WebDriver 自動找到
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    try:
        driver.get("https://www.google.com")
        print("Page title is:", driver.title)

        search_box = driver.find_element(By.NAME, "q")
        print(search_box.get_attribute('outerHTML'))
        search_box.send_keys("Selenium")
        search_box.submit()

        driver.implicitly_wait(10)  # 最多等待 10 秒
        print("New page title is:", driver.title)

    finally:
        driver.quit()

# 設置默認參數
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 定義 DAG
with DAG(
    'selenium_scraper_test',
    default_args=default_args,
    description='A simple Selenium DAG',
    schedule_interval=timedelta(days=1),  # 每天運行一次
    catchup=False,
) as dag:

    run_selenium_task = PythonOperator(
        task_id='run_selenium_task',
        python_callable=selenium_scraper
    )