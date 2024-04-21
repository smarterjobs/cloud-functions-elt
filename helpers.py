from datetime import datetime as dt
class Logger():
    _log_level = {
        20: "INFO",
        30: "WARNING",
        40: "ERROR",
        50: "CRITICAL"
    }
    log_list = []
    error_list = []
    files_attempted = 0;
    files_succeeded = 0;

    def increment_attempted(self):
        self.files_attempted +=1

    def increment_succeeded(self):
        self.files_succeeded +=1

    def __init__(self) -> None:
        pass

    def log(self, text, level=20):
        now = dt.now().strftime("%Y-%m-%d, %H:%M:%S")

        log_message = f"{self._log_level[level]} {now} {text}"

        print(log_message)

        self.log_list.append(log_message)

        if level > 20:
            self.error_list.append(log_message)

