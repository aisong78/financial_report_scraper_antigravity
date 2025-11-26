import akshare as ak
import pandas as pd
from datetime import datetime
from .base_fetcher import BaseFetcher

class AShareFetcher(BaseFetcher):
    def fetch_financial_data(self, stock_code: str):
        """
        抓取 A 股财务数据 (使用 AkShare)
        数据源：新浪财经/东方财富
        """
        print(f"🚀 [A股] 开始抓取 {stock_code} 的财务数据 (2010年至今)...")
        
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
            
            # 4. 数据清洗与保存
            self._process_and_save(stock_code, df_income, df_balance, df_cash)
            
            print(f"✅ {stock_code} 数据抓取完成！")
            return True
            
        except Exception as e:
            print(f"❌ 抓取失败: {e}")
            return False

    def _process_and_save(self, stock_code, df_income, df_balance, df_cash):
        """
        清洗数据并调用基类方法保存
        """
        # 1. 设置索引
        try:
            df_income.set_index('报告日', inplace=True)
            df_balance.set_index('报告日', inplace=True)
            df_cash.set_index('报告日', inplace=True)
        except KeyError:
            if '报表日期' in df_income.columns:
                df_income.set_index('报表日期', inplace=True)
                df_balance.set_index('报表日期', inplace=True)
                df_cash.set_index('报表日期', inplace=True)
        
        # 2. 统一索引（报告期）
        periods = sorted(list(set(df_income.index) & set(df_balance.index) & set(df_cash.index)))
        
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
            
            # 提取数据辅助函数
            def get_val(df, col_name):
                if col_name in df.columns:
                    val = df.loc[period, col_name]
                    if pd.isna(val) or val == '' or val == '--':
                        return None
                    try:
                        if isinstance(val, str):
                            val = val.replace(',', '')
                        return float(val)
                    except:
                        return None
                return None

            # --- 映射字段 ---
            data = {
                # 利润表
                'revenue': get_val(df_income, '营业总收入') or get_val(df_income, '营业收入'),
                'cost_of_revenue': get_val(df_income, '营业成本'),
                'gross_profit': None, 
                'selling_expenses': get_val(df_income, '销售费用'),
                'admin_expenses': get_val(df_income, '管理费用'),
                'rd_expenses': get_val(df_income, '研发费用'),
                'financial_expenses': get_val(df_income, '财务费用'),
                'income_tax_expenses': get_val(df_income, '所得税费用'),
                'investment_income': get_val(df_income, '投资收益'),
                'operating_income': get_val(df_income, '营业利润'),
                'total_profit': get_val(df_income, '利润总额'),
                'net_income': get_val(df_income, '净利润'),
                'net_income_parent': get_val(df_income, '归属于母公司所有者的净利润'),
                'net_income_deducted': get_val(df_income, '扣除非经常性损益后的净利润'),
                
                # 资产负债表
                'total_assets': get_val(df_balance, '资产总计'),
                'current_assets': get_val(df_balance, '流动资产合计'),
                'non_current_assets': get_val(df_balance, '非流动资产合计'),
                'total_liabilities': get_val(df_balance, '负债合计'),
                'current_liabilities': get_val(df_balance, '流动负债合计'),
                'non_current_liabilities': get_val(df_balance, '非流动负债合计'),
                'total_equity': get_val(df_balance, '所有者权益(或股东权益)合计'),
                'share_capital': get_val(df_balance, '实收资本(或股本)'),
                'retained_earnings': get_val(df_balance, '未分配利润'),
                'cash_equivalents': get_val(df_balance, '货币资金'),
                'accounts_receivable': get_val(df_balance, '应收账款'),
                'inventory': get_val(df_balance, '存货'),
                'fixed_assets': get_val(df_balance, '固定资产净额') or get_val(df_balance, '固定资产'),
                'intangible_assets': get_val(df_balance, '无形资产'),
                'goodwill': get_val(df_balance, '商誉'),
                'short_term_debt': get_val(df_balance, '短期借款'),
                'long_term_debt': get_val(df_balance, '长期借款'),
                'accounts_payable': get_val(df_balance, '应付账款'),
                'contract_liabilities': get_val(df_balance, '合同负债') or get_val(df_balance, '预收款项'),
                
                # 现金流量表
                'cfo_net': get_val(df_cash, '经营活动产生的现金流量净额'),
                'cfi_net': get_val(df_cash, '投资活动产生的现金流量净额'),
                'cff_net': get_val(df_cash, '筹资活动产生的现金流量净额'),
                'net_cash_flow': get_val(df_cash, '现金及现金等价物净增加额'),
                'capex': get_val(df_cash, '购建固定资产、无形资产和其他长期资产所支付的现金'),
                'cash_paid_for_dividends': get_val(df_cash, '分配股利、利润或偿付利息所支付的现金')
            }
            
            # 补全计算字段
            if data['revenue'] and data['cost_of_revenue']:
                data['gross_profit'] = data['revenue'] - data['cost_of_revenue']
            
            # 调用基类保存方法
            self.save_to_db(stock_code, report_period_str, report_type, data, market='CN', currency='CNY')
