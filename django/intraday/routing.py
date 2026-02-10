from django.urls import path
from .consumers import OverviewConsumer, CurrencyConsumer, TickerConsumer, EquityOverviewConsumer, EquityManagerConsumer, EquityTickerConsumer, EquityTickerManagerConsumer

websocket_urlpatterns = [
    path("ws/fx/overview/", OverviewConsumer.as_asgi()),
    path("ws/fx/currency/<currency>/", CurrencyConsumer.as_asgi()),
    path("ws/fx/ticker/<str:ticker>/", TickerConsumer.as_asgi()),
    path("ws/equity/overview/", EquityOverviewConsumer.as_asgi()),
    path("ws/equity/manager/<str:manager>/", EquityManagerConsumer.as_asgi()),
    path("ws/equity/ticker/<str:ticker>/", EquityTickerConsumer.as_asgi()),
    path("ws/equity/ticker_manager/<str:ticker>/<str:manager>/", EquityTickerManagerConsumer.as_asgi()),
]
