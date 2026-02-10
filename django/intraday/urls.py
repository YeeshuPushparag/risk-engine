from django.urls import path
from .views import fx_overview_initial, fx_currency_initial, fx_ticker_initial, equity_overview, equity_manager, equity_ticker, equity_ticker_manager

urlpatterns = [
    path("fx/overview/", fx_overview_initial),
    path("fx/currency/", fx_currency_initial),
    path("fx/ticker/", fx_ticker_initial),
    path("equity/overview/", equity_overview),
    path("equity/manager/", equity_manager),
    path("equity/ticker/", equity_ticker),
    path("equity/ticker_manager/", equity_ticker_manager),
]
