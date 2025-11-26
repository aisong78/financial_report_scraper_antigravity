import akshare as ak
import pandas as pd
import sqlite3
from pathlib import Path

# 模拟 HKShareFetcher 的逻辑获取所有科目
def get_hk_full_fields():
    print("📡 正在获取港股(01810)全量字段...")
    fields = set()
    try:
        for sheet in ["利润表", "资产负债表", "现金流量表"]:
            df = ak.stock_financial_hk_report_em(stock="01810", symbol=sheet, indicator="年度")
            if not df.empty and 'STD_ITEM_NAME' in df.columns:
                fields.update(df['STD_ITEM_NAME'].unique())
    except Exception as e:
        print(f"HK Error: {e}")
    return fields

# 模拟 AShareFetcher 的逻辑获取所有科目
def get_ashare_full_fields():
    print("📡 正在获取A股(600519)全量字段...")
    fields = set()
    try:
        # A股接口通常直接返回宽表，列名即科目
        df = ak.stock_financial_report_sina(stock="600519", symbol="现金流量表")
        if not df.empty:
            fields.update(df.columns)
        df = ak.stock_financial_report_sina(stock="600519", symbol="资产负债表")
        if not df.empty:
            fields.update(df.columns)
        df = ak.stock_financial_report_sina(stock="600519", symbol="利润表")
        if not df.empty:
            fields.update(df.columns)
    except Exception as e:
        print(f"A-Share Error: {e}")
    return fields

def get_current_db_fields():
    # 硬编码我们目前的数据库字段 (除了 id, stock_code 等)
    return {
        'revenue', 'cost_of_revenue', 'gross_profit', 'net_income', 'net_income_parent',
        'eps_basic', 'total_assets', 'total_liabilities', 'total_equity',
        'current_assets', 'current_liabilities', 'non_current_assets', 'non_current_liabilities',
        'cash_equivalents', 'inventory', 'accounts_receivable',
        'cfo_net', 'cfi_net', 'cff_net', 'capex', 'cash_paid_for_dividends',
        'rd_expenses', 'admin_expenses', 'selling_expenses', 'interest_expense'
    }

def analyze():
    hk_fields = get_hk_full_fields()
    ashare_fields = get_ashare_full_fields()
    current_fields = get_current_db_fields()

    print(f"\n{'='*20} 港股数据分析 {'='*20}")
    print(f"📊 AkShare 提供字段总数: {len(hk_fields)}")
    print(f"🗄 我们目前存储字段数: {len(current_fields)}")
    print(f"📉 丢失的字段示例 (Top 10): {list(hk_fields)[:10]}")
    
    print(f"\n{'='*20} A股数据分析 {'='*20}")
    print(f"📊 AkShare 提供字段总数: {len(ashare_fields)}")
    print(f"📉 丢失的字段示例 (Top 10): {list(ashare_fields - current_fields)[:10]}")

if __name__ == "__main__":
    analyze()
