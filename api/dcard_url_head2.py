from time import sleep
from datetime import datetime
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
import pymysql
import logging
import os
# 檢查並建立儲存螢幕截圖的資料夾
screenshot_dir = os.path.expanduser("~/tasks")
if not os.path.exists(screenshot_dir):
    os.makedirs(screenshot_dir)

#------------------ 日誌配置 ---------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

#---------------------- MySQL --------------------------------------
logger.info("Connecting to MySQL database...")
try:
    connection = pymysql.connect(
        host='IP',      # MySQL 主機
        port=3306,                 # MySQL 端口
        user='XXXX',              # MySQL 使用者名稱
        password='XXXX',      # MySQL 密碼
        database='DCARD',          # 資料庫名稱
        charset='utf8mb4'          # 使用的編碼
    )
    logger.info("Connected to MySQL database.")
except pymysql.MySQLError as e:
    logger.error(f"Failed to connect to MySQL: {e}")
    raise

# 建立游標
cursor = connection.cursor()

#------------------ 瀏覽器初始化 ------------------------------
logger.info("Initializing undetected Chrome browser...")
driver = uc.Chrome()
logger.info("Browser initialized.")

#------------------ Dcard 首頁 ------------------------------
dcard_url = "https://www.dcard.tw/f"
logger.info(f"Navigating to Dcard homepage: {dcard_url}")
driver.get(dcard_url)
sleep(10)  # 等待頁面載入
logger.info("Dcard homepage loaded.")
# 保存進入頁面的螢幕截圖
screenshot_path = os.path.join(screenshot_dir, "dcard_home.png")
driver.save_screenshot(screenshot_path)
print(f"首頁螢幕截圖儲存到: {screenshot_path}")
#------------------ 設定爬取文章數量 ------------------------------
num_articles = 100
head = 0
logger.info(f"Starting to scrape {num_articles} articles...")

#------------------ 爬蟲主邏輯 ------------------------------------
for i in range(head, num_articles + head):
    try:
        logger.info(f"Attempting to locate article with data-key {i}...")
        while True:
            try:
                element_by_data_key = driver.find_element(By.XPATH, f"//div[@data-key='{i}']")
                logger.info(f"Found article with data-key {i}.")
                break
            except Exception:
                driver.execute_script("window.scrollBy(0, 500);")
                logger.info(f"Scrolling down to locate data-key {i}...")

        # 抓取連結和文章 ID
        url = element_by_data_key.find_element(By.CLASS_NAME, "t1gihpsa").get_attribute("href")
        article_ID = url.split('/')[-1]
        logger.info(f"Scraped URL: {url} | Article ID: {article_ID}")

        # 設定爬取日期
        current_date = datetime.now().date()

        # 插入數據到 MySQL
        insert = """INSERT IGNORE INTO article_confirm(ID, Date, Url) VALUES(%s, %s, %s)"""
        data = (article_ID, current_date, url)
        cursor.execute(insert, data)
        connection.commit()
        logger.info(f"Inserted article {article_ID} into database.")

        # 頁面向下滾動
        driver.execute_script("arguments[0].scrollIntoView({block:'start'});", element_by_data_key)
        sleep(1)

    except Exception as e:
        logger.error(f"Error occurred during article scraping for data-key {i}: {e}")
        continue

#------------------ 關閉連接和瀏覽器 ------------------------------
logger.info("Closing database connection and browser...")
driver.quit()
cursor.close()
connection.close()
logger.info("Closed database connection and browser.")
