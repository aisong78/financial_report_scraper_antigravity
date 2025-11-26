import akshare as ak
import pandas as pd
import time

def test_full_report(stock_code, stock_name):
    print(f"\n{'='*20} 测试 {stock_name} ({stock_code}) {'='*20}")
    
    indicators = ["利润表", "资产负债表", "现金流量表"]
    
    for ind in indicators:
        print(f"\n🔍 尝试获取 [{ind}] ...")
        try:
            # 增加重试机制，防止网络波动
            for i in range(3):
                try:
                    # 文档参数: stock="00700", symbol="资产负债表", indicator="年度"
                    # 注意：AkShare 版本更新可能导致参数名变化，这里严格按照文档尝试
                    df = ak.stock_financial_hk_report_em(stock=stock_code, symbol=ind, indicator="年度")
                    break
                except TypeError:
                    # 如果参数名不对，尝试旧版参数名 (symbol, indicator)
                    try:
                        df = ak.stock_financial_hk_report_em(symbol=stock_code, indicator=ind)
                        break
                    except:
                        raise
                except Exception as e:
                    if i == 2: raise e
                    time.sleep(1)
            
            if not df.empty:
                print(f"   ✅ 成功！获取到 {len(df)} 行数据")
                print(f"   📊 包含 {len(df.columns)} 个字段")
                print(f"   👀 前 20 个字段预览: {df.columns.tolist()[:20]}")
                
                # 打印所有科目名称，方便映射
                if 'STD_ITEM_NAME' in df.columns:
                    items = df['STD_ITEM_NAME'].unique()
                    print(f"   📋 科目列表 ({len(items)}个): {items[:10]} ...")
                    # 打印一些关键科目
                    print(f"   🔍 关键科目检查: {[x for x in items if '研发' in x or '资产' in x or '现金' in x][:5]}")
            else:
                print("   ⚠️ 返回了空 DataFrame (可能该股票无此数据或接口参数不对)")
                
        except Exception as e:
            print(f"   ❌ 报错: {e}")

if __name__ == "__main__":
    # 测试腾讯 (通常数据最全)
    test_full_report("00700", "腾讯控股")
    # 测试小米
    test_full_report("01810", "小米集团")
