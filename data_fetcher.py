import akshare as ak
import pandas as pd
import sqlite3
from datetime import datetime
from pathlib import Path
import time
import random

# 数据库路径
DB_PATH = Path(__file__).parent / "finance.db"

class DataFetcher:
    def __init__(self):
        self.db_path = DB_PATH
        
    def fetch_a_stock_financials(self, stock_code):
        """
        抓取 A 股财务数据 (使用 AkShare)
        数据源：新浪财经/东方财富
        """
        print(f"🚀 开始抓取 {stock_code} 的财务数据 (2010年至今)...")
        
        try:
            # 1. 利润表
            print("  -正在获取利润表...")
            df_income = ak.stock_financial_report_sina(stock=stock_code, symbol="利润表")
            
            # 2. 资产负债表
            print("  -正在获取资产负债表...")
            df_balance = ak.stock_financial_report_sina(stock=stock_code, symbol="资产负债表")
            
            # 3. 现金流量表
            print("  -正在获取现金流量表...")
            df_cash = ak.stock_financial_report_sina(stock=stock_code, symbol="现金流量表")
            
            # 4. 数据清洗与合并
            self._process_and_save(stock_code, df_income, df_balance, df_cash)
            
            print(f"✅ {stock_code} 数据抓取完成！")
            return True
            
        except Exception as e:
            print(f"❌ 抓取失败: {e}")
            return False

    def _process_and_save(self, stock_code, df_income, df_balance, df_cash):
        """
        清洗数据并存入数据库
        """
        print(f"  调试: 利润表列名: {df_income.columns[:5]}")
        print(f"  调试: 利润表行数: {len(df_income)}")
        
        # 1. 设置索引：AkShare 返回的数据，第一列是 '报告日'，我们需要把它设为索引
        # 注意：AkShare 返回的数据已经是我们要的格式（行=报告期，列=指标），不需要转置
        
        # 打印一下列名确认
        print(f"  调试: 原始列名: {df_income.columns[:5]}")
        
        try:
            df_income.set_index('报告日', inplace=True)
            df_balance.set_index('报告日', inplace=True)
            df_cash.set_index('报告日', inplace=True)
        except KeyError:
            # 有时候列名可能是 '报表日期'
            if '报表日期' in df_income.columns:
                df_income.set_index('报表日期', inplace=True)
                df_balance.set_index('报表日期', inplace=True)
                df_cash.set_index('报表日期', inplace=True)
        
        print(f"  调试: 设置索引后索引(前5): {df_income.index[:5]}")

        # 2. 统一索引（报告期）
        # 找出所有共同的报告期
        periods = sorted(list(set(df_income.index) & set(df_balance.index) & set(df_cash.index)))
        print(f"  调试: 共同报告期数量: {len(periods)}")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for period in periods:
            # 过滤年份：只保留 2010 年及以后的
            try:
                report_date = datetime.strptime(period, "%Y%m%d")
                if report_date.year < 2010:
                    continue
            except:
                continue
                
            # 格式化日期 20231231 -> 2023-12-31
            report_period_str = report_date.strftime("%Y-%m-%d")
            
            # 判断报告类型
            month = report_date.month
            if month == 3: report_type = 'Q1'
            elif month == 6: report_type = 'S1'
            elif month == 9: report_type = 'Q3'
            elif month == 12: report_type = 'A'
            else: report_type = 'Other'
            
            # 提取数据 (使用 safe_get 避免列名不存在报错)
            def get_val(df, col_name):
                if col_name in df.columns:
                    val = df.loc[period, col_name]
                    try:
                        return float(val) if val else None
                    except:
                        return None
                return None

            # --- 映射字段 (这是最关键的一步) ---
            data = {
                'stock_code': stock_code,
                'report_period': report_period_str,
                'report_type': report_type,
                'currency': 'CNY',
                
                # 利润表
                'revenue': get_val(df_income, '营业总收入') or get_val(df_income, '营业收入'),
                'cost_of_revenue': get_val(df_income, '营业成本'),
                'gross_profit': None, # 稍后计算
                'selling_expenses': get_val(df_income, '销售费用'),
                'admin_expenses': get_val(df_income, '管理费用'),
                'rd_expenses': get_val(df_income, '研发费用'),
                'financial_expenses': get_val(df_income, '财务费用'),
                'investment_income': get_val(df_income, '投资收益'),
                'net_income': get_val(df_income, '净利润'),
                'net_income_parent': get_val(df_income, '归属于母公司所有者的净利润'),
                'net_income_deducted': get_val(df_income, '扣除非经常性损益后的净利润'), # 注意：新浪数据可能有此字段
                
                # 资产负债表
                'total_assets': get_val(df_balance, '资产总计'),
                'total_liabilities': get_val(df_balance, '负债合计'),
                'total_equity': get_val(df_balance, '所有者权益(或股东权益)合计'),
                'cash_equivalents': get_val(df_balance, '货币资金'),
                'accounts_receivable': get_val(df_balance, '应收账款'),
                'inventory': get_val(df_balance, '存货'),
                'fixed_assets': get_val(df_balance, '固定资产'),
                'goodwill': get_val(df_balance, '商誉'),
                'short_term_debt': get_val(df_balance, '短期借款'),
                'long_term_debt': get_val(df_balance, '长期借款'),
                'accounts_payable': get_val(df_balance, '应付账款'),
                
                # 现金流量表
                'cfo_net': get_val(df_cash, '经营活动产生的现金流量净额'),
                'cfi_net': get_val(df_cash, '投资活动产生的现金流量净额'),
                'cff_net': get_val(df_cash, '筹资活动产生的现金流量净额'),
                'capex': get_val(df_cash, '购建固定资产、无形资产和其他长期资产支付的现金'),
                'cash_paid_for_dividends': get_val(df_cash, '分配股利、利润或偿付利息支付的现金')
            }
            
            # 补全计算字段
            if data['revenue'] and data['cost_of_revenue']:
                data['gross_profit'] = data['revenue'] - data['cost_of_revenue']
            
            # 生成 SQL
            columns = ', '.join(data.keys())
            placeholders = ', '.join(['?'] * len(data))
            sql = f'''
            INSERT OR REPLACE INTO financial_reports_raw ({columns})
            VALUES ({placeholders})
            '''
            
            cursor.execute(sql, list(data.values()))
            
        conn.commit()
        conn.close()

if __name__ == "__main__":
    fetcher = DataFetcher()
    # 测试：抓取容百科技
    fetcher.fetch_a_stock_financials("688005")
