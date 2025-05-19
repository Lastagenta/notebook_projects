import requests
from requests.exceptions import ConnectionError
from time import sleep
import pandas as pd
import json
import logging
import io
from clickhouse_driver import Client
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("script.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

CLICKHOUSE_HOST = "u_host"
CLICKHOUSE_PORT = youre_port
CLICKHOUSE_USER = "username"
CLICKHOUSE_PASSWORD = "password"
CLICKHOUSE_DB = "database_name"

ReportsURL = 'https://api.direct.yandex.com/json/v5/reports'
token = 'api_token'
clientLogin = 'account_login'

headers = {
    "Authorization": "Bearer " + token,
    "Client-Login": clientLogin,
    "Accept-Language": "ru",
    "processingMode": "auto",
    "skipReportHeader": "true",
    "skipColumnHeader": "true",
    "skipReportSummary": "true"
}


# Запрос данных из Яндекс Директа
def fetch_data(from_date, to_date):
    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": from_date,
                "DateTo": to_date,
                "Filter": [
                    {
                        "Field": "Clicks",
                        "Operator": "GREATER_THAN",
                        "Values": ["0"]
                    }
                ]
            },
            "FieldNames": [
                "Date", "CampaignName", "CampaignId", "Impressions", "Clicks",
                "Ctr", "Cost", "AvgCpc", "AvgPageviews", "ConversionRate",
                "CostPerConversion", "Conversions"
            ],
            "Goals": ["goals_id_1", "goals_id2", "goalsid_3"],  # Добавлены три цели
            "ReportName": "DetailedReport",
            "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "NO",
            "IncludeDiscount": "NO"
        }
    }

    body = json.dumps(body, indent=4)
    logging.info(f"Запрос данных с {from_date} по {to_date}.")

    while True:
        try:
            req = requests.post(ReportsURL, body, headers=headers)
            req.encoding = 'utf-8'
            if req.status_code == 200:
                logging.info("Отчет успешно получен.")
                return req.text
            elif req.status_code in (201, 202):
                retryIn = int(req.headers.get("retryIn", 60))
                logging.warning(f"Отчет в очереди, повторный запрос через {retryIn} секунд.")
                sleep(retryIn)
            else:
                logging.error(f"Ошибка получения отчета: {req.status_code}, {req.text}")
                return None
        except ConnectionError:
            logging.critical("Ошибка соединения с сервером.")
            return None


# Загрузка данных в ClickHouse
def load_to_clickhouse(data):
    try:
        columns = [
            "Date", "CampaignName", "CampaignId", "Impressions", "Clicks",
            "Ctr", "Cost", "AvgCpc", "AvgPageviews", "ConversionRate",
            "CostPerConversion", "Conversions", "CR1", "CR2"
        ]

        # Чтение данных
        df = pd.read_csv(io.StringIO(data), sep='\t', header=None, names=columns, encoding='utf-8')
        logging.info(f"Данные загружены в Pandas. Всего строк: {len(df)}")

        if df.empty:
            logging.warning("Пустой отчет, пропускаем вставку в ClickHouse.")
            return

        # Обработка пустых значений
        df.replace(["--", ""], "0", inplace=True)

        # Преобразование типов
        integer_columns = ["Impressions", "Clicks", "CampaignId"]
        for col in integer_columns:
            df[col] = df[col].astype(int)

        numeric_columns = ["Ctr", "Cost", "AvgCpc", "AvgPageviews",
                           "ConversionRate", "CostPerConversion"]
        for col in numeric_columns:
            df[col] = df[col].astype(float)

        # Исправление обработки конверсий (округление)
        df["CR1"] = df["CR1"].astype(float).fillna(
            0).round().astype(int)
        df["CR2"] = df["CR2"].astype(float).fillna(
            0).round().astype(int)
        df["Conversions"] = df["Conversions"].astype(float).fillna(
            0).round().astype(int)

        df["Cost"] = df["Cost"] / 1_000_000
        df["AvgCpc"] = df["AvgCpc"] / 1_000_000
        df["CostPerConversion"] = df["CostPerConversion"] / 1_000_000

        df["Date"] = pd.to_datetime(df["Date"], errors="coerce",
                                    format="%Y-%m-%d")
        df = df.dropna(
            subset=["Date"])  # Удаляем строки с неправильными датами
        df["Date"] = df["Date"].dt.date

        # Добавляем столбец ShortName
        df.insert(1, "ShortName", df["CampaignName"].apply(lambda x: x.split(' ')[0].replace("/", "").replace("_", "")))

        df = df.sort_values(by="CampaignName", ascending=True)

        logging.info("Данные подготовлены к загрузке в ClickHouse.")

        # Подключение к ClickHouse
        client = Client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DB
        )

        # Проверка существующих данных в ClickHouse
        existing_data = client.execute("""
            SELECT Date, CampaignId FROM database.db_table 
            WHERE Date >= %(from_date)s
        """, {"from_date": df["Date"].min()})

        existing_df = pd.DataFrame(existing_data, columns=["Date", "CampaignId"])

        # Удаление дубликатов перед вставкой
        df = df.merge(existing_df, on=["Date", "CampaignId"], how="left", indicator=True)
        df = df[df["_merge"] == "left_only"].drop(columns=["_merge"])

        logging.info(f"После удаления дубликатов осталось строк: {len(df)}")

        # Вставка данных в ClickHouse
        if not df.empty:
            logging.info(f"Вставка {len(df)} строк в ClickHouse...")
            client.execute("""
                INSERT INTO yandex_data.direct_api (
                    Date, CampaignName, ShortName, CampaignId, Impressions, Clicks, Ctr,
                    Cost, AvgCpc, AvgPageviews, ConversionRate, CostPerConversion, Conversions, 
                    CR1, CR2
                ) VALUES
            """, df.to_dict("records"))

            logging.info("✅ Данные успешно загружены в ClickHouse.")
        else:
            logging.info("Нет новых данных для вставки.")

    except Exception as e:
        logging.error(f"Ошибка загрузки в ClickHouse: {e}")


if __name__ == "__main__":
    start_date = datetime.now() - timedelta(days=120)
    end_date = datetime.now()

    logging.info(f"Запускаем полную выгрузку за {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}")

    current_start = start_date
    while current_start < end_date:
        current_end = min(current_start + timedelta(days=30), end_date)
        data = fetch_data(current_start.strftime('%Y-%m-%d'), current_end.strftime('%Y-%m-%d'))
        if data:
            load_to_clickhouse(data)

        current_start += timedelta(days=30)
