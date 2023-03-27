
class ConfigLogger:

    def __init__(self):
        '''
        logger configuration
        '''
        self._path = None # 로깅 경로
        self._max_size = None # 최대 로깅 사이즈
        self._file_count = None # 로그 파일 카운트
        self._level = "info" # 로그 레벨 (미지정 시, info)
        self._data_dump = 0
        self._console_log = False

        # path
        @property
        def path(self):
            return self._path
        @path.setter
        def path(self, str):
            self._path = str

        #max_size
        @property
        def max_size(self):
            return self._max_size
        @max_size.setter
        def max_size(self, str):
            self._max_size = str

        #file_count
        @property
        def file_count(self):
            return self._file_count
        @file_count.setter
        def file_count(self, str):
            self._file_count = str

        #level
        @property
        def level(self):
            return self._level
        @level.setter
        def level(self, str):
            self._level = str

        #data_dump
        @property
        def data_dump(self):
            return self._data_dump
        @data_dump.setter
        def data_dump(self, str):
            self._data_dump = str

        # console_log
        @property
        def console_log(self):
            return self._console_log

        @console_log.setter
        def console_log(self, str):
            self._console_log = str


class ConfigServer:
    _ip = None
    _port = None

    def __init__(self):
        self._config_server = []


class ConfigInterface:
    _intf_id = None
    _url = None

    def __init__(self):
        '''
        interface configuration
        '''
        self._interface_list = []

    def set_interface_list(self, interface_list):
        self._interface_list = interface_list

    def get_interface_list(self):
        return self._interface_list

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, value):
        self._type = value

    @property
    def intf_id(self):
        return self._intf_id

    @intf_id.setter
    def intf_id(self, value):
        self._intf_id = value

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, value):
        self._url = value

class ConfigInterfaceColumn:
    name = None
    rename = None
    replace = None

    def init(self):
        pass


