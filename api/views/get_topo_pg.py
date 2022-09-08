# -*- coding: utf-8 -*-
# Author: Mh
# Date: 2022/9/8 10:27
# Function:

"""
返回前端表单信息
- project itmes
"""

import json
import traceback
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
from api.views.common.date_encoder import DateEncoder, data_format
from api.views.common.status import Status, INIT_STATUS, STATUS
from api.service.service_get_topo_pg import ServiceGetTopoPG
from common.log_utils import get_logger

logger = get_logger(__name__)


def post_method_proc(request, context):
    try:
        params_dict = request.POST
        if request.content_type == 'application/json':
            params_dict = json.loads(request.body if request.body else '{}')
        context.update(data_format(ServiceGetTopoPG(params_dict).execute()))
    except Exception as e:
        message = '从PG库拉起点表和线表文件失败:{}'.format(e)
        logger.error(traceback.format_exc())
        STATUS[Status.OTHER_ERROR.name]['message'] = message
        context.update(STATUS[Status.OTHER_ERROR.name])
    return HttpResponse(json.dumps(context, cls=DateEncoder, ensure_ascii=False),
                        content_type="application/json; charset=utf-8")


def other_method_proc(request, context):
    logger.debug('get method request')
    context.update(STATUS[Status.BAD_GET.name])
    return HttpResponse(json.dumps(context, cls=DateEncoder, ensure_ascii=False),
                        content_type="application/json; charset=utf-8")


@csrf_exempt
def get_topo_pg(request):
    """ 通过pg库获取topo原始csv文件 """
    context = INIT_STATUS.copy()
    if request.method == 'POST':
        return post_method_proc(request, context)
    else:
        return other_method_proc(request, context)


