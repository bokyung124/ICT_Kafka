import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import time
from pytz import timezone, utc

class CommonLogger:
    def __init__(self):
        '''
        logger initialize
        '''

        self._set_init = 0
        self._logger_instance = None
        self._formatter_string = '[%(asctime)s][PID:%(process)d][%(levelname)s] %(message)s'
        self.set_console_log = False
        self._log_level = "info"
        self._data_dump = 0
        self._logger_instance = {}



    def set_formatter(self, formatter_string):
        '''
        Setting logger formatting
        > default formmating : [%(asctime)s][PID:%(process)d][%(levelname)s] %(message)s

        :param formatter_string:
        :return formatting string:
        '''

        self._formatter_string = formatter_string

    # 로그 설정
    def set_initialize(self, file_path, level="info", max_size=0, file_count=0, console_log=0, data_dump=0):

        '''
        logger initialize

        :param file_path, level="info", max_size=0, file_count=0
        :return formatting string:
        '''

        type_circular = 1
        if max_size == 0:
            type_circular = 0

        if self._set_init == 1:
            return 0

        self._logger_instance = logging.getLogger(file_path)
        if len(self._logger_instance.handlers) > 1:
            return 0

        if level == "info":
            self._logger_instance.setLevel(logging.INFO)
            self._log_level = "info"
        elif level == "error":
            self._logger_instance.setLevel(logging.ERROR)
            self._log_level = "error"
        elif level == "warn":
            self._logger_instance.setLevel(logging.WARN)
            self._log_level = "warn"
        elif level == "critical":
            self._logger_instance.setLevel(logging.CRITICAL)
            self._log_level = "critical"
        else:
            self._logger_instance.setLevel(logging.DEBUG)
            self._log_level = "debug"

        def custom_time(*args):
            utc_dt = utc.localize(datetime.utcnow())
            my_tz = timezone("Asia/Seoul")
            converted = utc_dt.astimezone(my_tz)
            return converted.timetuple()

        formatter = logging.Formatter(self._formatter_string)
        formatter.converter = custom_time

        if console_log == 1:
            self.set_console_log = "True"
        if self.set_console_log == "True":
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            self._logger_instance.addHandler(stream_handler)

        if data_dump == 1:
            self._data_dump = 1

        if type_circular == 1:
            file_handler = RotatingFileHandler(file_path, maxBytes=int(max_size), backupCount=int(file_count))
        else:
            file_handler = logging.FileHandler(file_path)

        file_handler.setFormatter(formatter)
        self._logger_instance.addHandler(file_handler)

        self._set_init = 1

    def get_log_level(self):
        return self._log_level;

    def info(self, log_string):
        '''
        Log Write Wrapper => level:info
        '''
        self._logger_instance.info(log_string)

    def debug(self, log_string):
        '''
        Log Write Wrapper => level:debug
        '''
        self._logger_instance.debug(log_string)

    def dump(self, log_string):
        if self._data_dump == 1:
            lines = self.set_dump_string(log_string)
            for index, line in enumerate(lines):
                self._logger_instance.debug(f"{(index*16):08x} {self.encode_hex(line)} : {self.decode_bytes(line)}")

    def warning(self, log_string):
        '''
        Log Write Wrapper => level:warning
        '''
        self._logger_instance.warning(log_string)
    def error(self, log_string):
        '''
        Log Write Wrapper => level:error
        '''
        self._logger_instance.error(log_string)
    def critical(self, log_string):
        '''
        Log Write Wrapper => level:critical
        '''
        self._logger_instance.critical(log_string)
    def exception(self, log_string):
        '''
        Log Write Wrapper => level:exception
        '''
        self._logger_instance.exception(log_string)

    def set_dump_string(self, dump_string, n_bytes=16):
        split_lines = []
        try:
            #enc_string = dump_string.encode('utf-8')
            enc_string = dump_string
            for i in range(0, len(enc_string), n_bytes):
                split_lines.append(enc_string[i:i+n_bytes])
        except Exception as err:
            print("Error")
        return split_lines

    # encode bytes to hex
    def encode_hex(self, byte_block):
        hex_ = []
        for x in byte_block:
            hex_.append(f"{x:02x}")
        hex_ = list(zip(hex_[::2], hex_[1::2]))
        bytes_to_hex = ""
        for x in hex_:
            bytes_to_hex += f"{x[0]}{x[1]} "
        return bytes_to_hex

    # decode btye to string
    def decode_bytes(self, data):
        str_from_hex = ""
        for x in data:
            if x > 31 and x < 127:
                str_from_hex += chr(x)
            else:
                str_from_hex += "."
        return str_from_hex

    def xxd(self, lines: list):
        for index, line in enumerate(lines):
            print(f"{(index*16):08x} {self.encode_hex(line)} : {self.decode_bytes(line)}")

