import json
import re
from collections import Counter
from datetime import datetime, timezone

import pandas as pd
from ckip_transformers.nlp import CkipNerChunker, CkipPosTagger, CkipWordSegmenter
from confluent_kafka import Consumer, KafkaException
from pymongo import MongoClient, UpdateOne
from transformers import AutoTokenizer

# 初始化 CKIP Transformers 的 NER 模型
print("Loading CKIP model...")
tokenizer = AutoTokenizer.from_pretrained("ckiplab/bert-base-chinese")
model = CkipNerChunker(model_name="ckiplab/bert-base-chinese-ner")
ws_driver = CkipWordSegmenter(model="bert-base")
pos_driver = CkipPosTagger(model="bert-base")
ner_driver = CkipNerChunker(model="bert-base")
print("CKIP model loaded")

# 配置 Kafka Consumer
conf = {
    'bootstrap.servers': 'IP:9092',
    'group.id': '3',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
print("Kafka consumer configured")


def check_mongodb_connection(client):
    """檢查 MongoDB 連接狀態"""
    try:
        client.admin.command('ping')
        print("Successfully connected to MongoDB!")
    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")
        raise


def preprocess_text(text):
    """文本預處理函數"""
    text = re.sub(r"\n+", "\n", text)
    text = re.sub(r"-----\nSent from.*", "", text)
    text = re.sub(r"--+", "", text)
    return text.strip()


def calculate_word_frequency(word_list):
    """計算詞頻"""
    return Counter(word_list)


def process_content_item(item):
    """單篇文章處理函數"""
    value = item["value"]
    text_date_str = value.get("發佈日期", "未知日期")

    try:
        text_date = pd.to_datetime(f"2023/{text_date_str}", format="%Y/%m/%d")
    except ValueError:
        text_date = None
    # 內容預處理
    raw_content = value.get("內容", "")
    preprocessed_content = preprocess_text(raw_content)
    ws_result = ws_driver([preprocessed_content])[0]
    pos_result = pos_driver([ws_result])[0]
    ner_result = ner_driver([preprocessed_content])[0]

    word_frequency = calculate_word_frequency(ws_result)
    word_pos_data = [{"word": word, "pos": pos, "frequency": word_frequency[word]}
                     for word, pos in zip(ws_result, pos_result)]
    # 檢查命名實體格式
    if isinstance(ner_result, list) and all(isinstance(ner, tuple) for ner in ner_result):
        ner_counter = Counter([ner[0] for ner in ner_result])
        ner_data = [{"entity": ner[0], "type": ner[1], "counts": ner_counter[ner[0]], "publish_date": text_date}
                    for ner in ner_result]
    else:
        ner_counter = Counter([ner["entity"] for ner in ner_result])
        ner_data = [{"entity": ner["entity"], "type": ner["type"], "counts": ner_counter[ner["entity"]], "publish_date": text_date}
                    for ner in ner_result]
    # 構建更新操作
    processed_data = {
        "url": item["key"],
        "data": {
            "source": 'ptt',
            "publish_date": text_date,
            "title": value.get("標題"),
            "author": value.get("作者"),
            "content": preprocessed_content,
            "word_pos_frequency": word_pos_data,
            "named_entities": ner_data
        }
    }
    return UpdateOne({"url": item["key"]}, {"$set": processed_data}, upsert=True)


def process_kafka_messages(consumer, collection, message_limit=10):# 設置筆數限制
    """消費 Kafka 消息並處理每條消息"""
    message_count = 0

    # 消費和處理消息
    while message_count < message_limit:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        # 取得消息文本
        text = msg.value().decode('utf-8')
        print(f"\nReceived message: {text}")

        try:
        # 解析 JSON 格式的消息
            news_data = json.loads(text)
            ner_results = model([news_data.get('content', '')])

            entities = [{"word": entity.word, "type": entity.ner} for entity in ner_results[0]]
            for entity in entities:
                print(f"Entity: {entity['word']}, Type: {entity['type']}")

            document = {
                'original_data': news_data,
                'entities': entities,
                'processed_at': datetime.now(timezone.utc),
                'source1': 'news'
            }

            result = collection.insert_one(document)
            print(f"Document inserted with id: {result.inserted_id}")

            message_count += 1
            print(f"Processed {message_count}/{message_limit} messages")

        except json.JSONDecodeError as e:
            print(f"JSON parsing error: {e}")
        except Exception as e:
            print(f"Error processing message: {e}")

    print("Message limit reached")


def main():
    try:
        mongo_client = MongoClient("mongodb://user5:password5@35.189.181.117:28017/admin")
        check_mongodb_connection(mongo_client)

        db = mongo_client['cleandata']
        collection = db['CT1']

        consumer.subscribe(['news-topic'])
        print("Subscribed to news-topic")

        process_kafka_messages(consumer, collection)

    except KeyboardInterrupt:
        print("\nManually interrupted")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        print("\nCleaning up resources...")
        consumer.close()
        mongo_client.close()
        print("Program finished")


if __name__ == "__main__":
    main()
