from django.contrib import admin
from django.urls import path, include
from . import views
from django.conf.urls.static import static
from django.conf import settings

urlpatterns = [
    path('', views.index, name='home'),
    path('acl', views.acl, name='acl'),
    path('aatp3', views.aatp3, name='aatp3'),
    path('aatp_rezka', views.aatp_rezka, name='aatp_rezka'),
    path('aatp_45', views.aatp_45, name='aatp_45')
]
