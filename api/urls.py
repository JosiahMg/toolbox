from django.urls import path
from django.http import HttpResponse
from api.views import get_topo_pg


def index(request):
    return HttpResponse("Hello, world. You're at ToolBox.")


urlpatterns = [
    path('', index, name='index'),
    path('get-topo-pg', get_topo_pg.get_topo_pg),
]
