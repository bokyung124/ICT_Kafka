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

            #kafka
            self._kafka_connection_info = self.get_kafka_connection_info()
            self._logger.debug(f"[{myfunc}] Kafka Connection Config {self._kafka_connection_info}")

            self._commit_timeout = self.get_kafka_commit_timeout()
            self._transaction_timeout = self.get_kafka_transaction_timeout()
            self._logger.debug(f"[{myfunc}] Kafka Config. commit timeout: {self._commit_timeout} sec, transaction timeout: {self._transaction_timeout} sec")

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

    def get_kafka_connection_info(self):

        root_find_path = "./connection/kafka"
        for element in self._rootDocument.findall(root_find_path):
            bootstrap_servers = self.get_attrubute(element, 'bootstrap.servers')
            transactional_id = self.get_attrubute(element, 'transactional.id')
            transaction_timeout_ms = self.get_attrubute(element, 'transaction.timeout.ms')
            client_id = self.get_attrubute(element, 'client.id')
            acks = self.get_attrubute(element, 'acks')
            batch_num_messages = self.get_attrubute(element, 'batch.num.messages')
            batch_size = self.get_attrubute(element, 'batch.size')
            connections_max_idle_ms = self.get_attrubute(element, 'connections.max.idle_ms')
            request_timeout_ms = self.get_attrubute(element, 'request.timeout.ms')
            enable_idempotence = self.get_attrubute(element, 'enable.idempotence')
            queue_buffering_max_messages = self.get_attrubute(element, 'queue.buffering.max.messages')
            queue_buffering_max_kbytes = self.get_attrubute(element, 'queue.buffering.max.kbytes')
            queue_buffering_max_ms = self.get_attrubute(element, 'queue.buffering.max.ms')
            message_send_max_retries = self.get_attrubute(element, 'message.send.max.retries')
            request_required_acks = self.get_attrubute(element, 'request.required.acks')
            message_timeout_ms = self.get_attrubute(element, 'message.timeout.ms')
            partitioner = self.get_attrubute(element, 'partitioner')
            compression_codec = self.get_attrubute(element, 'compression.codec')
            compression_type = self.get_attrubute(element, 'compression.type')
            compression_level = self.get_attrubute(element, 'compression.level')

            debug = self.get_attrubute(element, 'debug')

            ssl_context = self.get_attrubute(element, 'ssl.context')


            self._kafka_dict['bootstrap.servers'] = str(bootstrap_servers)
            if transactional_id != 0 and transactional_id != "":
                appendix = random_string.generate()
                self._kafka_dict['transactional.id'] = str(transactional_id)+appendix
            if transaction_timeout_ms != 0 and transaction_timeout_ms != "":
                self._kafka_dict['transaction.timeout.ms'] = str(transaction_timeout_ms)
            if client_id != 0 and client_id != "":
                self._kafka_dict['client.id'] = str(client_id)
            if acks != 0 and acks != "":
                self._kafka_dict['acks'] = str(acks)
            if batch_size != 0 and batch_size != "":
                self._kafka_dict['batch.size'] = str(batch_size)
            if batch_num_messages != 0 and batch_size != "":
                self._kafka_dict['batch.num.messages'] = str(batch_num_messages)
            if connections_max_idle_ms != 0 and connections_max_idle_ms != "":
                self._kafka_dict['connections.max.idle.ms'] = str(connections_max_idle_ms)
            if request_timeout_ms != 0 and request_timeout_ms != "":
                self._kafka_dict['request.timeout.ms'] = str(request_timeout_ms)
            if ssl_context != 0 and ssl_context != "":
                self._kafka_dict['ssl.context'] = str(ssl_context)
            if enable_idempotence != 0 and enable_idempotence != "":
                self._kafka_dict['enable.idempotence'] = str(enable_idempotence)
            if queue_buffering_max_messages != 0 and queue_buffering_max_messages != "":
                self._kafka_dict['queue.buffering.max.messages'] = str(queue_buffering_max_messages)
            if queue_buffering_max_kbytes != 0 and queue_buffering_max_kbytes != "":
                self._kafka_dict['queue.buffering.max.kbytes'] = str(queue_buffering_max_kbytes)
            if queue_buffering_max_ms != 0 and queue_buffering_max_ms != "":
                self._kafka_dict['queue.buffering.max.ms'] = str(queue_buffering_max_ms)
            if message_send_max_retries != 0 and message_send_max_retries != "":
                self._kafka_dict['message.send.max.retries'] = str(message_send_max_retries)
            if request_required_acks != 0 and request_required_acks != "":
                self._kafka_dict['request.required.acks'] = str(request_required_acks)
            if message_timeout_ms != 0 and message_timeout_ms != "":
                self._kafka_dict['message.timeout.ms'] = str(message_timeout_ms)
            if partitioner != 0 and partitioner != "":
                self._kafka_dict['partitioner'] = str(partitioner)
            if compression_codec != 0 and compression_codec != "":
                self._kafka_dict['compression.codec'] = str(compression_codec)
            if compression_type != 0 and compression_type != "":
                self._kafka_dict['compression.type'] = str(compression_type)
            if compression_level != 0 and compression_level != "":
                self._kafka_dict['compression.level'] = str(compression_level)
            if debug != 0 and debug != "":
                self._kafka_dict['debug'] = str(debug)

        return self._kafka_dict

    def get_kafka_commit_timeout(self):

        root_find_path = "./connection/kafka"
        for element in self._rootDocument.findall(root_find_path):
            commit_timeout = int(int(self.get_attrubute(element, 'internal.commit.timeout'))/1000)

        return commit_timeout

    def get_kafka_transaction_timeout(self):

        root_find_path = "./connection/kafka"
        for element in self._rootDocument.findall(root_find_path):
            transaction_timeout = int(int(self.get_attrubute(element, 'internal.transaction.timeout'))/1000)

        return transaction_timeout

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






