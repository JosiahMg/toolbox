import datetime
import json


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime("%Y-%m-%common %H:%M:%S")
        else:
            return json.JSONEncoder.default(self, obj)


def data_format(data):
    """
    返回内容字典编码
    {
    'statusCode': 200,
    'message': 'success',
    'data': data
    }
    """
    ret_dict = {'data': data}
    # logger.debug(f'dict encoder[info]: {ret_dict}')
    return ret_dict