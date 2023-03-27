import sys
import pandas as pd
import json
import copy
import os
import time
import datetime
import socket
from common import ConfigManager, MySQLWrapper, KafkaWrapper, CommonUtil, RestClient

class InterfaceProcess:

    def __init__(self):
        self._kafka_adapter_home = os.getenv('KAFKA_ADAPTER_HOME', '.')
        config_file = os.getenv('KAFKA_ADAPTER_CONFIG', './config/config.xml')

        self._config_manager = ConfigManager.ConfigManager()
        self._mysql_wrapper = MySQLWrapper.MySQLWrapper()
        self._kafka_wrapper = KafkaWrapper.KafkaWrapper()

        self._config_manager.load_config(config_file)
        self._logger = self._config_manager.get_logger()
        self._mysql_wrapper.set_logger(self._logger)
        self._kafka_wrapper.set_logger(self._logger)
        self._interface_list = self._config_manager.interface_list

        self._common_util = CommonUtil.CommonUtil()

    def send_monitor(self, lst, status, error_message):
        try:
            myfunc = sys._getframe().f_code.co_name
            rest_client = RestClient.RestClient()
            monitor_info = {}
            monitor_info['id'] = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
            monitor_info['intf_id'] = lst.intf_id
            monitor_info['intf_name'] = 'DB TO KAFKA'
            monitor_info['host_id'] = socket.gethostname()
            monitor_info['process_dt'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            monitor_info['status'] = status
            monitor_info['error_message'] = error_message

            response = rest_client.restapi_post(lst.attr1, monitor_info)

        except Exception as err:
            self._logger.error(f"[{myfunc}] send_monitor error. err: ({err}), interface id:{lst.intf_id})")
            pass

    def data_get(self):
        try:
            myfunc = sys._getframe().f_code.co_name
            for lst in self._interface_list:

                if lst.intf_type == "DBGET":
                    self._logger.info(f"[{myfunc}] DBGET Interface start. interface id:{lst.intf_id}")
                    self._kafka_wrapper.kafka_connect(self._config_manager.get_kafka_connection_info())
                    self._kafka_wrapper.kafka_init_transaction()
                    self._mysql_wrapper.db_connect(self._config_manager.get_db_connection_info())
                    sql_filename = self._kafka_adapter_home + "/config/" + lst.intf_in
                    f = open(sql_filename + ".pre", "r")
                    pre_sql = f.read()
                    f = open(sql_filename + ".post", "r")
                    post_sql = f.read()
                    f = open(sql_filename, "r")
                    sql = f.read()

                    while True:
                        self._mysql_wrapper.db_execute(pre_sql)
                        result = self._mysql_wrapper.db_select("all", sql)
                        if len(result) > 0:
                            self._kafka_wrapper.kafka_begin_transaction()
                            self._logger.info(f"[{myfunc}] DBGET select success. interface id:{lst.intf_id}. data:[{result}]")

                            self._kafka_wrapper.kafka_put(lst.intf_out, json.dumps(result))
                            self._logger.info(f"[{myfunc}] DBGET kafka put success. interface id:{lst.intf_id}")
                            self._mysql_wrapper.db_execute(post_sql)
                            self._kafka_wrapper.kafka_commit()
                            self._mysql_wrapper.db_commit()

                            self.send_monitor(lst, '00', '')
                        else:
                            self._logger.info(f"[{myfunc}] DBGET Empty. interface id:{lst.intf_id}.")
                            self._mysql_wrapper.db_commit()

                            if lst.process_type == "realtime":
                                time.sleep(lst.poll_time / 1000)

                        if lst.process_type == "realtime":
                            pass
                        else:
                            self._kafka_wrapper.kafka_disconnect()
                            break;

                self._kafka_wrapper.kafka_disconnect()

        except Exception as err:
            self._kafka_wrapper.kafka_rollback()
            self._mysql_wrapper.db_rollback()
            self._logger.error(f"[{myfunc}] data_get error. err: ({err}), interface id:{lst.intf_id})")
            self.send_monitor(lst, '99', err)
            raise

