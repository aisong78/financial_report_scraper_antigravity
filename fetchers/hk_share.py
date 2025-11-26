import akshare as ak
import pandas as pd
from datetime import datetime
from .base_fetcher import BaseFetcher

class HKShareFetcher(BaseFetcher):
    def fetch_financial_data(self, stock_code: str):
        """
        抓取港股财务数据 (使用 AkShare stock_financial_hk_report_em 接口)
        获取完整的三大报表数据
        """
        print(f"🚀 [港股] 开始抓取 {stock_code} 的完整财务数据...")
        
        try:
            # 1. 分别获取三张表
            df_income = self._fetch_report(stock_code, "利润表")
            df_balance = self._fetch_report(stock_code, "资产负债表")
            df_cash = self._fetch_report(stock_code, "现金流量表")
            
            if df_income.empty and df_balance.empty:
                print(f"❌ 未获取到 {stock_code} 的任何报表数据")
                return False

            # 2. 数据透视 (Long -> Wide)
            # 索引是 REPORT_DATE, 列是 STD_ITEM_NAME, 值是 AMOUNT
            pivot_income = self._pivot_data(df_income)
            pivot_balance = self._pivot_data(df_balance)
            pivot_cash = self._pivot_data(df_cash)
            
            # 3. 合并数据 (按日期)
            # 使用 outer join 保证数据不丢失
            df_merged = pivot_income.join(pivot_balance, how='outer', rsuffix='_bal').join(pivot_cash, how='outer', rsuffix='_cash')
            
            # 4. 处理每一行并保存
            self._process_and_save(stock_code, df_merged)
            
            print(f"✅ {stock_code} 数据抓取完成！")
            return True
            
        except Exception as e:
            print(f"❌ 抓取失败: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _fetch_report(self, stock_code, symbol):
        """抓取单个报表并处理异常"""
        try:
            # 使用正确的参数名: stock, symbol, indicator
            df = ak.stock_financial_hk_report_em(stock=stock_code, symbol=symbol, indicator="年度")
            return df
        except Exception as e:
            print(f"   ⚠️ 获取 {symbol} 失败: {e}")
            return pd.DataFrame()

    def _pivot_data(self, df):
        """将长格式数据透视为宽格式"""
        if df.empty: return pd.DataFrame()
        
        # 确保日期格式统一
        df['REPORT_DATE'] = pd.to_datetime(df['REPORT_DATE'])
        
        # 去重 (防止同一天有重复项目)
        df = df.drop_duplicates(subset=['REPORT_DATE', 'STD_ITEM_NAME'])
        
        # 透视
        pivot_df = df.pivot(index='REPORT_DATE', columns='STD_ITEM_NAME', values='AMOUNT')
        return pivot_df

    def _process_and_save(self, stock_code, df):
        """清洗并保存数据"""
        # 字段映射字典 (中文科目 -> 数据库字段)
        # 注意：港股科目名称可能不统一，这里列出常见的
        field_map = {
            # --- 利润表 ---
            'revenue': ['营业额', '营业收入', '营业总收入', '收入'],
            'gross_profit': ['毛利'],
            'net_income_parent': ['本公司拥有人应占溢利', '归属于母公司股东的净利润', '归母净利润'],
            'net_income': ['年度溢利', '净利润'],
            'eps_basic': ['基本每股盈利', '基本每股收益'],
            'rd_expenses': ['研究及开发成本', '研发费用'],
            'admin_expenses': ['行政开支', '管理费用'],
            'selling_expenses': ['销售及分销成本', '销售费用'],
            
            # --- 资产负债表 ---
            'total_assets': ['资产总值', '资产合计', '总资产'],
            'total_liabilities': ['负债总额', '负债合计', '总负债'],
            'total_equity': ['本公司拥有人应占权益', '权益合计', '股东权益合计'],
            'current_assets': ['流动资产', '流动资产合计'],
            'current_liabilities': ['流动负债', '流动负债合计'],
            'non_current_assets': ['非流动资产', '非流动资产合计'],
            'non_current_liabilities': ['非流动负债', '非流动负债合计'],
            'cash_equivalents': ['现金及现金等价物', '货币资金'],
            'inventory': ['存货'],
            'accounts_receivable': ['应收账款'],
            
            # --- 现金流量表 ---
            'cfo_net': ['经营业务现金净额', '经营活动产生的现金流量净额'],
            'cfi_net': ['投资业务现金净额', '投资活动产生的现金流量净额'],
            'cff_net': ['融资业务现金净额', '筹资活动产生的现金流量净额'],
            'capex': ['购建固定资产', '购买物业、厂房及设备'], # 需要确认符号，通常是负数
            'cash_paid_for_dividends': ['已付股息', '分配股利、利润或偿付利息支付的现金']
        }

        for date, row in df.iterrows():
            report_period_str = date.strftime("%Y-%m-%d")
            
            # 简单判断报告类型 (目前接口只返回年度)
            report_type = 'A' 
            
            data = {}
            
            # 辅助函数：查找映射值
            def find_val(target_field):
                candidates = field_map.get(target_field, [])
                for cand in candidates:
                    if cand in row:
                        val = row[cand]
                        if pd.notna(val) and val != '':
                            return float(val)
                return None

            # 填充数据
            for db_field in field_map.keys():
                data[db_field] = find_val(db_field)
            
            # 特殊处理：如果没有 net_income，用 net_income_parent 代替
            if data.get('net_income') is None:
                data['net_income'] = data.get('net_income_parent')

            # --- 生成全量数据 JSON ---
            # 将 Series 转换为字典
            raw_dict = row.to_dict()
            # 处理 datetime 对象 (转为字符串)，否则 json.dumps 会报错
            for k, v in raw_dict.items():
                if isinstance(v, (pd.Timestamp, datetime)):
                    raw_dict[k] = v.strftime('%Y-%m-%d')
                # 处理 NaN
                if pd.isna(v):
                    raw_dict[k] = None
            
            import json
            raw_json = json.dumps(raw_dict, ensure_ascii=False)

            # 保存
            self.save_to_db(stock_code, report_period_str, report_type, data, market='HK', currency='HKD', raw_data=raw_json)
