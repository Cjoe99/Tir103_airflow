#------------------爬蟲--------------------------------------------
from time import sleep
from datetime import datetime
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from pymysql import OperationalError
import pymysql
import logging
from retrying import retry

#------------------ 日誌配置 ---------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#------------------ MySQL 連接 ------------------------------------
def create_connection():
    try:
        connection = pymysql.connect(
            host='IP',  # 主機名稱
            port=3306,             # 指定 MySQL 使用的端口號
            user='XXXX',          # 資料庫使用者名稱
            password='XXXX',  # 資料庫密碼
            database='DCARD',      # 資料庫名稱
            charset='utf8mb4'      # 使用的編碼
        )
        return connection
    except OperationalError as e:
        logger.error(f"Failed to connect to MySQL: {e}")
        raise

connection = create_connection()
cursor = connection.cursor()

#------------------ Chrome 瀏覽器配置 ------------------------------
options = uc.ChromeOptions()
options.headless = True
options.add_argument('--no-sandbox')  # 允許無 root 權限的情況下運行
options.add_argument('--disable-dev-shm-usage')  # 解決共享內存限制問題
options.add_argument('--disable-gpu')  # 如果啟動無頭模式，禁用 GPU

driver = uc.Chrome(options=options)

#------------------ 設定 Dcard 首頁 ------------------------------
dcard_url = "https://www.dcard.tw/f"

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def open_page(url):
    driver.get(url)

open_page(dcard_url)
sleep(10)  # 等待頁面載入

# 一次爬幾篇文章
num_articles = 1
head = 0

#------------------ 爬蟲主邏輯 ------------------------------------
for i in range(head, num_articles + head):
    try:
        while True:
            try:
                element_by_data_key = driver.find_element(By.XPATH, f"//div[@data-key='{i}']")
                break
            except Exception:
                driver.execute_script("window.scrollBy(0, 500);")
        
        # 抓取連結和文章 ID
        url = element_by_data_key.find_element(By.CLASS_NAME, "t1gihpsa").get_attribute("href")
        article_ID = url.split('/')[-1]
        
        # 設定爬取日期
        current_date = datetime.now().date()
        
        # 插入數據到 MySQL
        insert = """INSERT IGNORE INTO article_confirm(ID, Date, Url) VALUES(%s, %s, %s)"""
        data = (article_ID, current_date, f"{url}")
        cursor.execute(insert, data)
        connection.commit()
        
        # 頁面向下滾動
        driver.execute_script("arguments[0].scrollIntoView({block:'start'});", element_by_data_key)
        sleep(1)
    except Exception as e:
        logger.error(f"Error occurred during article scraping: {e}")
        continue

#------------------ 關閉連接和瀏覽器 ------------------------------
driver.quit()
cursor.close()
connection.close()
