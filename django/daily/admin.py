from django.contrib import admin
from .models import EquityData, FXData, BondData, CommodityData, LoanData, DerivativeData, CollateralData, CollateralModelData


@admin.register(EquityData)
class EquityDataAdmin(admin.ModelAdmin):
    list_display = ("date", "ticker", "asset_manager", "total_value", "daily_pnl")
    list_filter = ("date", "asset_manager", "sector")
    search_fields = ("ticker", "asset_manager", "cusip")


@admin.register(FXData)
class FXDataAdmin(admin.ModelAdmin):
    list_display = ("date", "ticker", "currency_pair", "fx_rate", "fx_pnl", "total_pnl")
    list_filter = ("date", "currency_pair", "sector")
    search_fields = ("ticker", "currency_pair")



@admin.register(BondData)
class BondDataAdmin(admin.ModelAdmin):
    list_display = ("date", "bond_id", "ticker", "credit_rating",
                    "benchmark_yield", "corporate_yield", "credit_spread")
    
    list_filter = ("date", "sector", "credit_rating")
    
    search_fields = ("bond_id", "ticker", "industry", "sector")





@admin.register(CommodityData)
class CommodityDataAdmin(admin.ModelAdmin):
    list_display = ("date", "ticker", "commodity",
                    "mtm_value", "commodity_pnl")
    list_filter = ("date", "commodity", "sector")
    search_fields = ("ticker", "commodity", "asset_manager")




@admin.register(LoanData)
class LoanDataAdmin(admin.ModelAdmin):
    list_display = ("month", "loan_id", "ticker", "credit_rating", "total_pnl")
    list_filter = ("month", "sector", "stage")
    search_fields = ("loan_id", "ticker", "counterparty")


@admin.register(DerivativeData)
class DerivativeAdmin(admin.ModelAdmin):
    list_display = ("date", "trade_id", "ticker", "counterparty", "net_exposure", "pnl")
    list_filter = ("date", "asset_class", "counterparty")
    search_fields = ("trade_id", "ticker", "counterparty")




@admin.register(CollateralData)
class CollateralAdmin(admin.ModelAdmin):
    list_display = ("date","trade_id","counterparty","collateral_value","net_exposure")
    list_filter = ("date","asset_class","counterparty")
    search_fields = ("trade_id","collateral_id","ticker")


@admin.register(CollateralModelData)
class CollateralModelAdmin(admin.ModelAdmin):
    list_display = ("date","asset_class","ticker","counterparty","net_exposure")
    list_filter = ("date","asset_class")
    search_fields = ("ticker","counterparty")