import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
from datetime import datetime
from confluent_kafka import Producer
import json
import re

class YahooNewsScraper:
    # 類別常數定義
    KAFKA_TOPIC = 'news-topic'
    MAX_NEWS_COUNT = 20
    
    def __init__(self, kafka_config=None, topic=None):
        """
        初始化爬蟲類
        :param kafka_config: Kafka配置
        :param topic: Kafka主題, 如果不指定則使用預設主題
        """
        # Kafka配置
        self.kafka_config = kafka_config or {
            'bootstrap.servers': 'IP:9092',
            'max.in.flight.requests.per.connection': 1,
            'error_cb': self.error_cb
        }
        self.topic = topic or self.KAFKA_TOPIC
        self.producer = Producer(self.kafka_config) if kafka_config is not None else None

    def error_cb(self, err):
        """
        Kafka錯誤回調函數
        """
        print(f'Kafka錯誤: {err}')

    def send_to_kafka(self, news_data):
        """
        發送新聞數據到Kafka
        :param news_data: 新聞數據字典
        """
        if self.producer is None:
            return

        try:
            # 將數據轉換為JSON字符串
            message = json.dumps(news_data, ensure_ascii=False)
            # 發送消息
            self.producer.produce(
                self.topic,
                message.encode('utf-8')
            )
            # 立即刷新消息
            self.producer.poll(0)
        except Exception as e:
            print(f'發送到Kafka時發生錯誤: {str(e)}')

    def extract_author(self, article_soup):
        """
        使用正則表示法和多種選擇器抓取記者姓名
        :param article_soup: BeautifulSoup物件
        :return: 記者姓名，如果未找到則返回None
        """
        # 正則表示法模式
        author_patterns = [
            r'記者\s*([^\s／]+)',   # 匹配「記者 姓名」，忽略後續的「／地點報導」
            r'撰文\s*([^\s／]+)',   # 匹配「撰文 姓名」，忽略後續的「／地點報導」
            r'文\s*：\s*([^\s／]+)' # 匹配「文：姓名」，忽略後續的「／地點報導」
            r'CTWANT |\s*([^\s]]+)',   # 匹配「NT | 姓名」，忽略後續的「]地點報導」
        ]
        
        # 要搜尋的 HTML 元素
        author_selectors = [
            '.caas-author-byline',
            '.article-header .author',
            '.author-name',
            '.byline'
        ]
        
        # 先嘗試 HTML 選擇器
        for selector in author_selectors:
            author_element = article_soup.select_one(selector)
            if author_element:
                author_text = author_element.get_text(strip=True)
                # 如果找到作者元素，直接返回
                return author_text
        
        # 如果 HTML 選擇器找不到，使用正則表示法
        article_text = article_soup.get_text()
        for pattern in author_patterns:
            match = re.search(pattern, article_text)
            if match:
                return match.group(1)
        
        return None

    def fetch_yahoo_news(self):
        """
        抓取Yahoo新聞
        """
        yahoo_news_url = "https://tw.news.yahoo.com/"

        try:
            response = requests.get(yahoo_news_url)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                news_items = soup.select('h3.Mb\(5px\) a')
                
                if not news_items:
                    print("未找到新聞項目，嘗試備用選擇器")
                    news_items = soup.select('#stream-content-item a')
                
                if not news_items:
                    print("仍未找到新聞項目，嘗試第二備用選擇器")
                    news_items = soup.select('.js-content-viewer')

                # 使用切片來限制新聞數量，並使用enumerate來追蹤索引
                for index, item in enumerate(news_items[:self.MAX_NEWS_COUNT], 1):
                    try:
                        title_text = item.get_text(strip=True)
                        article_url = urljoin(yahoo_news_url, item.get('href'))

                        print(f'\n處理第 {index} 則新聞:')
                        print(f'標題: {title_text}')
                        print(f'網址: {article_url}')

                        time.sleep(1)

                        article_response = requests.get(article_url)
                        
                        if article_response.status_code == 200:
                            article_soup = BeautifulSoup(article_response.text, 'html.parser')
                            
                            # 獲取時間
                            time_element = article_soup.select_one('time[datetime]')
                            update_time = None
                            if time_element:
                                update_time = time_element.get('datetime')
                                try:
                                    dt = datetime.fromisoformat(update_time.replace('Z', '+00:00'))
                                    formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S')
                                    print(f'發佈時間: {formatted_time}')
                                except ValueError:
                                    formatted_time = update_time
                                    print(f'原始發佈時間: {update_time}')
                            
                            # 獲取內容
                            article_content = article_soup.select_one('.caas-body')
                            if not article_content:
                                article_content = article_soup.select_one('.article-body')
                            if not article_content:
                                article_content = article_soup.select_one('.canvas-body')

                            content_text = ""
                            if article_content:
                                content_text = article_content.get_text(strip=True)
                                print(f'內容: {content_text}') 
                            
                            # 抓取作者
                            author = self.extract_author(article_soup)
                            if author:
                                print(f'作者: {author}')
                            
                            
                            
                            # 準備要發送到Kafka的數據
                            news_data = {
                                'sub_source': 'yahoo',
                                'title': title_text,
                                'url': article_url,
                                'date': formatted_time if 'formatted_time' in locals() else None,
                                'content': content_text,
                                'author': author,
                                'crawl_time': datetime.now().strftime('%Y-%m-%d')
                            }
                            
                            # 發送到Kafka
                            self.send_to_kafka(news_data)
                            
                            print(f'--- 完成第 {index} 則新聞處理 ---')
                        else:
                            print(f"無法存取文章頁面，狀態碼: {article_response.status_code}")
                            
                    except Exception as e:
                        print(f"處理文章時發生錯誤: {str(e)}")
                        continue

            else:
                print(f"無法連接到熱門新聞頁面，狀態碼: {response.status_code}")

        except Exception as e:
            print(f"發生錯誤: {str(e)}")
        
        finally:
            # 確保所有消息都已發送
            if self.producer is not None:
                self.producer.flush()
            print(f"\n爬蟲完成, 共處理 {index if 'index' in locals() else 0} 則新聞")

def run_scraper_Yahoo():
    # Kafka配置
    kafka_config = {
        'bootstrap.servers': '104.155.214.8:9092',
        'max.in.flight.requests.per.connection': 1,
    }
    
    # 創建爬蟲實例，使用預設主題
    scraper = YahooNewsScraper(kafka_config=kafka_config)
    # 執行爬蟲
    scraper.fetch_yahoo_news()

if __name__ == "__main__":
    run_scraper_Yahoo()