from fetchers.hk_share import HKShareFetcher

def run():
    print("🚀 手动运行港股抓取测试...")
    fetcher = HKShareFetcher()
    success = fetcher.fetch_financial_data("01810")
    if success:
        print("✅ 抓取成功！")
    else:
        print("❌ 抓取失败！")

if __name__ == "__main__":
    run()
