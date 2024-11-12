import os
import sys
import json
import re
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import requests
from cc.scroller import PageScroller
from cc.kafka_producer import KafkaProducer

# 建立截圖儲存資料夾
screenshot_dir = os.path.expanduser("~/tasks")
if not os.path.exists(screenshot_dir):
    os.makedirs(screenshot_dir)

# 初始化 KafkaProducer 實例
kafka_producer_news = KafkaProducer(
    servers='IP:9092', 
    topic='news-topic'
)

# 初始化 WebDriver 和 PageScroller
driver = webdriver.Chrome()
scroller = PageScroller(driver, url="https://www.setn.com/ViewAll.aspx", max_scrolls=4, scroll_pause=1.5)

# 開啟網頁並滾動
try:
    scroller.open_page()
    scroller.scroll()
    
    # 在滾動完成後進行截圖並儲存
    screenshot_path = os.path.join(screenshot_dir, "initial_view.png")
    driver.save_screenshot(screenshot_path)
    print(f"截圖已儲存: {screenshot_path}")

except Exception as e:
    print(f"滾動或截圖時發生錯誤: {e}")
    driver.quit()
    kafka_producer_news.close()
    sys.exit(1)

# 取得網頁的 HTML
html = driver.page_source
soup = BeautifulSoup(html, 'html.parser')

# 找到所有新聞項目
news_items = soup.find_all(class_='newsItems')

# 當前抓取的時間
crawl_time = datetime.now().strftime('%Y-%m-%d')

# 遍歷每個新聞項目，提取分類、完整的 href 和標題，並抓取詳細內容
for item in news_items:
    try:
        # 提取分類和標題
        news_category = item.find('div', class_='newslabel-tab').a.text
        news_link = 'https://www.setn.com/' + item.find('a', class_='gt')['href']
        news_title = item.find('a', class_='gt').text.strip()

        # 使用 requests 獲取每條新聞的詳細內容
        response = requests.get(news_link)
        news_soup = BeautifulSoup(response.content, 'html.parser')

        # 找到文章內容並提取所有文字
        article_content = news_soup.find('div', id='ckuse')
        if article_content:
            # 提取文章文字
            text_content = article_content.get_text(separator="\n", strip=True)
            
            # 提取發佈時間
            publish_time_tag = news_soup.find('time', class_='page_date')
            publish_time = publish_time_tag.text.strip() if publish_time_tag else "未知"
            if publish_time != "未知":
                parsed_date = datetime.strptime(publish_time, "%Y/%m/%d %H:%M:%S")
                formatted_date_str = parsed_date.strftime("%Y-%m-%d")
            else:
                formatted_date_str = "未知"

            # 使用正規表達式匹配記者姓名
            reporter_match = re.search(r"記者([\u4e00-\u9fa5]{2,4})／", text_content)
            reporter_name = reporter_match.group(1) if reporter_match else "未知"

            # 建立新聞資料字典
            news_data = {
                "title": news_title,
                "url": news_link,
                "reporter": reporter_name,
                "content": text_content,
                "date": formatted_date_str,
                "category": news_category,
                "crawl_time": crawl_time
            }
            json_data = json.dumps(news_data, ensure_ascii=False)

            # 發送新聞資料至 Kafka
            kafka_producer_news.send_message(message=json_data.encode('utf-8'), key=news_link)
            print(f"已傳送文章至 Kafka: {news_title}")

        else:
            print(f"找不到 id='ckuse' 的內容，跳過該新聞：{news_link}")

    except Exception as e:
        print(f"處理新聞項目時發生錯誤：{e}")
        continue

# 清空 Kafka 緩衝區並關閉連接
kafka_producer_news.flush()
kafka_producer_news.close()

# 關閉瀏覽器
driver.quit()