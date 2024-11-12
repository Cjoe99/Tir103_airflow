#------------------爬蟲--------------------------------------------
from time import sleep
from datetime import datetime
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
#----------------------MySQL--------------------------------------
import pymysql
connection = pymysql.connect(
    host='IP',       # 使用 'localhost' 或 '127.0.0.1' 以連接到本地 MySQL
    port=3306,              # MySQL 默認端口號
    user='XXXX',            # 本地 MySQL 使用者名稱（例如 'root'）
    password='XXXX',  # 您的本地 MySQL 密碼
    database='DCARD',       # 資料庫名稱
    charset='utf8mb4',      # 使用的編碼
)
# 建立游標
cursor = connection.cursor()
#-------------------------------------------------------------------
# 使用 undetected_chromedriver 初始化 Chrome 瀏覽器 (用來爬標題和連結)
driver = uc.Chrome()
# 前往dcard 首頁
dcard_url = "https://www.dcard.tw/f" 
driver.get(dcard_url)
# 等待頁面載入
sleep(10)
# 一次爬幾個文章
num_articles = 100
# 中間會有阻擋的，要跳過(每個版不同)
block = 0
# 每個版的頭不同，data-key不同
head = 0
for i in range(head, num_articles+head):
    # 預防爬的過程出問題，還是可以把爬到的存下來
    try:
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
        # ID的前半部分
        date_str = current_date.strftime('%m%d')
        # 將data insert 到 MySQL
        insert = """insert ignore into article_confirm(ID, Date, Url)
                        values(%s, %s, %s)"""
        data = (article_ID, current_date, f"{url}")
        cursor.execute(insert, data)
        connection.commit()
        # 頁面向下滾動
        driver.execute_script("arguments[0].scrollIntoView({block:'start'});", element_by_data_key)
        sleep(1)
    except Exception:
        continue
driver.quit()
cursor.close()
connection.close()  # 關閉連線
