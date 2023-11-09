import xml.etree.ElementTree as ET
from common import CommonLogger
from common import ConfigObject

import sys
import random_string
from version import __version__

class ConfigManager:

    interface_list = []
    _mysql_dict = {}
    _kafka_dict = {}
    _db_connection_info = None
    _logger = None

    def __init__(self):
        # load config xml document
        self._rootDocument = None
        self._mysql_dict['host'] = None
        self._logger = CommonLogger.CommonLogger()


    def load_configfile(self, path):
        '''
        load configuration from file
        '''
        self._rootDocument = ET.parse(path)

    def load_config(self, config_path):
        """
            This function reads the configuration values used in class from the config xml file.
        """
        myfunc = sys._getframe().f_code.co_name
        try:
            ## LOAD CONFIG
            self.load_configfile(config_path)

            #logger
            # config - logger
            config_logger = self.get_log_info()
            self._logger.set_initialize(config_logger.path, config_logger.level, config_logger.max_size, config_logger.file_count, config_logger.console_log, config_logger.data_dump)
            self.version()
            self._logger.info(f"[{myfunc}] Config file: ({config_path})")
            self._logger.info(f"[{myfunc}] Log path: {config_logger.path}, level: {config_logger.level}, max size: {config_logger.max_size}, file count: {config_logger.file_count}, console log: {config_logger.console_log}")

            #db
            self._db_connection_info = self.get_db_connection_info()
            self._logger.debug(f"[{myfunc}] DB Connection Config {self._db_connection_info}")

            #interface
            self.interface_list = self.get_interface_info()
            for lst in self.interface_list:
                self._logger.debug(f"[{myfunc}] Interface Config. interface id:{lst.intf_id}")

        except Exception as err:
            print("load config error: {}".format(err))
            raise

    def load_config_by_string(self, xml):
        '''
        load configuration from string
        '''
        self._rootDocument = ET.fromstring(xml)


    def get_attrubute(self, element, attr) :
        '''
        getting attribute from configuration xml
        '''
        if attr in element.attrib :
            return element.get(attr)
        else :
            return 0

    def get_logger(self):
        return self._logger

    def get_db_connection_info(self):

        root_find_path = "./connection/db"
        for element in self._rootDocument.findall(root_find_path):
            host = self.get_attrubute(element, 'host')
            user = self.get_attrubute(element, 'user')
            password = self.get_attrubute(element, 'password')
            database = self.get_attrubute(element, 'database')
            charset = self.get_attrubute(element, 'charset')
            cursorclass = self.get_attrubute(element, 'cursorclass')

            self._mysql_dict['host'] = str(host)
            if user != 0 and user != "":
                self._mysql_dict['user'] = str(user)
            if password != 0 and password != "":
                self._mysql_dict['password'] = str(password)
            if database != 0 and database != "":
                self._mysql_dict['database'] = str(database)
            if charset != 0 and charset != "":
                self._mysql_dict['charset'] = str(charset)
            if cursorclass != 0 and cursorclass != "":
                self._mysql_dict['cursorclass'] = str(cursorclass)
        return self._mysql_dict

    def get_log_info(self):
        '''
        read logger configuration
        '''
        config_logger = ConfigObject.ConfigLogger()
        root_find_path = "./common/logger"

        logger = self._rootDocument.findall(root_find_path)[0]
        path = self.get_attrubute(logger, 'path')
        max_size = self.get_attrubute(logger, 'file_size')
        file_count = self.get_attrubute(logger, 'count')
        level = self.get_attrubute(logger, 'level')
        if level == 0:
            level = "info"
        data_dump = self.get_attrubute(logger, 'data_dump')
        console_log = self.get_attrubute(logger, 'console_log')

        config_logger.path = path
        config_logger.max_size = max_size
        config_logger.file_count = file_count
        config_logger.level = level
        config_logger.console_log = int(console_log)
        config_logger.data_dump = int(data_dump)

        return config_logger

    def get_server_info(self):
        '''
        read server configuration
        '''
        root_find_path = "./common/server"
        config_server = ConfigObject.ConfigServer()
        server = self._rootDocument.findall(root_find_path)[0]
        ip = self.get_attrubute(server, 'ip')
        port = self.get_attrubute(server, 'port')

        config_server.ip = ip
        config_server.port = int(port)

        return config_server

    def get_interface_info(self):
        '''
        read interface configuration
        '''
        config_interface_list = []
        config_interface_column_list = []

        root_find_path = "./interfaces/interface"

        for interface in self._rootDocument.findall(root_find_path):
            config_interface = ConfigObject.ConfigInterface()
            config_interface.intf_type = self.get_attrubute(interface, 'type')
            config_interface.intf_id = self.get_attrubute(interface, 'intf_id')
            config_interface.intf_in = self.get_attrubute(interface, 'in')
            config_interface.intf_out = self.get_attrubute(interface, 'out')
            config_interface.process_type = self.get_attrubute(interface, 'process_type')
            config_interface.poll_time = int(self.get_attrubute(interface, 'poll_time'))
            config_interface.attr1 = self.get_attrubute(interface, 'attr1')
            config_interface_column_list = []
            for col in interface.findall("./columns/column"):
                config_interface_column = ConfigObject.ConfigInterfaceColumn()
                config_interface_column.name = self.get_attrubute(col, 'name')
                config_interface_column.rename = self.get_attrubute(col, 'rename')
                config_interface_column.replace = self.get_attrubute(col, 'replace')
                config_interface_column.type = self.get_attrubute(col, 'type')
                config_interface_column.default = self.get_attrubute(col, 'default')
                config_interface_column.fk = self.get_attrubute(col, 'fk')
                config_interface_column_list.append(config_interface_column)
            config_interface.column = config_interface_column_list
            config_interface_list.append(config_interface)
        #endfor
        return config_interface_list

    def version(self):
        self._logger.info(f"version: {__version__}")






