import akshare as ak
import pandas as pd

def find_hk_apis():
    print("🔍 搜索 AkShare 港股财务接口...")
    apis = [x for x in dir(ak) if 'hk' in x and 'financial' in x]
    for api in apis:
        print(f"  - {api}")
    return apis

def test_fetch_xiaomi():
    stock_code = "01810"
    print(f"\n🚀 测试抓取小米集团 ({stock_code}) 数据...")
    
    # 尝试 1: stock_financial_hk_analysis_indicator_em (财务指标分析)
    try:
        print("\n1. 尝试 stock_financial_hk_analysis_indicator_em (symbol='01810')...")
        df = ak.stock_financial_hk_analysis_indicator_em(symbol=stock_code, indicator="年度")
        print(f"   ✅ 成功! 列名 ({len(df.columns)}个):")
        print(df.columns.tolist())
    except Exception as e:
        print(f"   ❌ 失败: {e}")
     # 尝试 4: 资产负债表 (猜测接口名)
    try:
        print("\n4. 尝试 stock_financial_hk_report_em (indicator='资产负债表')...")
        df = ak.stock_financial_hk_report_em(symbol=stock_code, indicator="资产负债表")
        if not df.empty:
            print(f"   ✅ 成功! 列名 ({len(df.columns)}个):")
            print(df.columns.tolist()[:10]) # 只打前10个
        else:
            print("   ⚠️ 返回了空 DataFrame")
    except Exception as e:
        print(f"   ❌ 失败: {e}")

    # 尝试 5: 现金流量表
    try:
        print("\n5. 尝试 stock_financial_hk_report_em (indicator='现金流量表')...")
        df = ak.stock_financial_hk_report_em(symbol=stock_code, indicator="现金流量表")
        if not df.empty:
            print(f"   ✅ 成功! 列名 ({len(df.columns)}个):")
            print(df.columns.tolist()[:10])
        else:
            print("   ⚠️ 返回了空 DataFrame")
    except Exception as e:
        print(f"   ❌ 失败: {e}")

    try:
        print("\n3. 尝试 stock_hk_financial_indicator_em (symbol='01810')...")
        df = ak.stock_hk_financial_indicator_em(symbol=stock_code)
        if not df.empty:
            print(f"   ✅ 成功! 列名: {df.columns.tolist()[:5]}...")
            print(df.head(2).T)
        else:
            print("   ⚠️ 返回了空 DataFrame")
    except Exception as e:
        print(f"   ❌ 失败: {e}")

if __name__ == "__main__":
    find_hk_apis()
    test_fetch_xiaomi()
