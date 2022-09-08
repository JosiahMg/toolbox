from django.urls import path
from django.http import HttpResponse


def index(request):
    return HttpResponse("Hello, world. You're at ToolBox.")


urlpatterns = [
    path('', index, name='index'),
    path('reg-fault-analysis/binary-classification', binary_predict.fault_predict),
]
