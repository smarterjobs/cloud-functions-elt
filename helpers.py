from datetime import datetime as dt
import requests


class Logger:
    _log_level = {20: "INFO", 30: "WARNING", 40: "ERROR", 50: "CRITICAL"}
    log_list = []
    error_list = []
    files_attempted = 0
    files_succeeded = 0

    def increment_attempted(self):
        self.files_attempted += 1

    def increment_succeeded(self):
        self.files_succeeded += 1

    def __init__(self) -> None:
        pass

    def log(self, text, level=20):
        now = dt.now().strftime("%Y-%m-%d, %H:%M:%S")

        log_message = f"{self._log_level[level]} {now} {text}"

        print(log_message)

        self.log_list.append(log_message)

        if level > 20:
            self.error_list.append(log_message)


def send_telegram_message(message, token, chat_id):
    url = f"https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={message}"
    print(requests.get(url).json()) # this sends the message



bigquery_to_pandas_types = {
    "STRING": "string",
    "BYTES": "bytes",
    "INTEGER": "int64",
    "INT64": "int64",
    "FLOAT": "float64",
    "FLOAT64": "float64",
    "NUMERIC": "float64",
    "BOOL": "bool",
    "BOOLEAN": "bool",
    "DATETIME": "datetime64",
    "DATE": "datetime64",
    "TIMESTAMP": "datetime64",
    "TIME": "datetime64",
    "GEOGRAPHY": "object",  # Pandas does not have a built-in type for geography
    "ARRAY": "object",  # For arrays, you might want to handle the conversion separately
    "STRUCT": "object",  # For struct, you might want to handle the conversion separately
}
