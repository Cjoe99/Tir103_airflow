from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from utils.slack_webhook_news import send_slack_message, slack_start_callback, slack_failure_callback, slack_success_callback
from datetime import datetime, timedelta
from tasks.SET_C2K import run_scraper_SET
from tasks.UDN_C2K import run_scraper_UDN
from tasks.Money_C2K import run_scraper_Money
from tasks.ETC_up_to_KFK import run_scraper_ETC
from tasks.yahoo_up_to_KFK import run_scraper_Yahoo

# 設置默認參數
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=1),  # 設置為前一天，避免即時觸發大量回補
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack_failure_callback,  # 失敗時發送 Slack 通知
    'on_success_callback': slack_success_callback,  # 成功時發送 Slack 通知
}

# 定義 DAG
with DAG(
    'C2K_dag_News_all',
    default_args=default_args,
    description='news producer',
    schedule_interval=timedelta(hours=2),  # 每兩小時執行一次
    catchup=False,  # 禁用回補
) as dag:
    
    # 程序啟動通知任務
    start_task = PythonOperator(
        task_id='start_task',
        python_callable=slack_start_callback,
        provide_context=True  # 自動將 context 傳入
    )
    # 爬取 Yahoo 文章任務
    run_requests_Yahoo_task = PythonOperator(
        task_id='run_requests_Yahoo',
        python_callable=run_scraper_Yahoo,
        trigger_rule='all_done'  # 任務執行條件：無論前一個任務成功或失敗都執行
    )

    # 爬取 ETC 文章任務
    run_requests_ETC_task = PythonOperator(
        task_id='run_requests_ETC',
        python_callable=run_scraper_ETC,
        trigger_rule='all_done'  # 任務執行條件：無論前一個任務成功或失敗都執行
    )

    # 定義執行 SET 爬蟲的任務
    run_scraper_set_task = PythonOperator(
        task_id='run_scraper_SET',
        python_callable=run_scraper_SET,
        op_args=['IP:9092', 'news-topic', 4, True],  # 傳遞參數
        trigger_rule='all_done'  # 任務執行條件：無論前一個任務成功或失敗都執行
    )

    # 定義執行 UDN 爬蟲的任務
    run_scraper_udn_task = PythonOperator(
        task_id='run_scraper_UDN',
        python_callable=run_scraper_UDN,
        op_args=['IP:9092', 'news-topic', 4, True],  # 傳遞參數
        trigger_rule='all_done'  # 任務執行條件：無論前一個任務成功或失敗都執行
    )

    # 定義執行 Money 爬蟲的任務
    run_scraper_money_task = PythonOperator(
        task_id='run_scraper_Money',
        python_callable=run_scraper_Money,
        op_args=['IP:9092', 'news-topic', 4, True],  # 傳遞參數
        trigger_rule='all_done'  # 任務執行條件：無論前一個任務成功或失敗都執行
    )

    # 設置順序：依次執行
    start_task >> run_requests_Yahoo_task >> run_requests_ETC_task >> run_scraper_set_task >> run_scraper_udn_task >> run_scraper_money_task