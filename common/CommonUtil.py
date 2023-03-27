from collections import OrderedDict
import copy, math, datetime

class CommonUtil:
    def __init__(self):
        pass

    def current_datetime(self):
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def make_json_result(self, is_success, result_code, result_message, data, page_info={}):
        json_data = OrderedDict()
        json_data['success'] = is_success
        json_data['resultCode'] = result_code
        json_data['resultMessage'] = result_message
        json_data['data'] = data
        json_data['pageInfo'] = page_info
        return json_data

    def make_page_data(self, result, page, size):
        result_list = []
        if result is None:
            result = []
        result_count = len(result)
        start = size * (page - 1)
        end = size * page
        if size * page == 1:
            end = 1
        for i in range(start, end):
            if i >= result_count:
                break
            if result[i] is not None:
                row_copy = copy.deepcopy(result[i])
                result_list.append(row_copy)

        page_info = OrderedDict()
        page_info['pageNumber'] = page
        page_info['totalPages'] = math.ceil(result_count / size)
        page_info['totalElements'] = result_count
        page_info['size'] = size
        page_info['first'] = True if page == 1 else False
        page_info['last'] = True if (page * size) >= result_count else False

        return result_list, page_info