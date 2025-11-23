import akshare as ak
import pandas as pd

stock_code = "600519"
print(f"正在获取 {stock_code} 的原始数据列名...")

try:
    # 利润表
    df_income = ak.stock_financial_report_sina(stock=stock_code, symbol="利润表")
    print("\n=== 利润表列名 ===")
    print(df_income.columns.tolist())
    
    # 资产负债表
    df_balance = ak.stock_financial_report_sina(stock=stock_code, symbol="资产负债表")
    print("\n=== 资产负债表列名 ===")
    print(df_balance.columns.tolist())
    
    # 现金流量表
    df_cash = ak.stock_financial_report_sina(stock=stock_code, symbol="现金流量表")
    print("\n=== 现金流量表列名 ===")
    print(df_cash.columns.tolist())

except Exception as e:
    print(f"获取失败: {e}")
