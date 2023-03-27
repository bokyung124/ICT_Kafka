# pip install random_string pymysql confluent_kafka pandas requests
from common import CommonUtil
import InterfaceProcess


if __name__ == '__main__':

    try:
        common_util = CommonUtil.CommonUtil()

        interface_process = InterfaceProcess.InterfaceProcess()
        interface_process.data_get()

    except Exception as err:
        print(f"{common_util.current_datetime()} process terminated with exception")
        raise SystemExit(-1)


