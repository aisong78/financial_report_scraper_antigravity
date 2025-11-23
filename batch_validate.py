#!/usr/bin/env python
"""
批量下载 PDF 并验证数据质量
"""
import os
import sqlite3
import json
from pathlib import Path
from pdf_downloader import PDFDownloader
from validator import FinancialDataValidator

DB_PATH = Path(__file__).parent / "finance.db"

def batch_validate(stock_code, gemini_api_key=None):
    """
    批量验证流程：
    1. 检查哪些报告期缺少 PDF
    2. 下载缺失的 PDF
    3. 验证所有报告期
    """
    print(f"📦 开始批量验证 {stock_code} 的数据...")
    print()
    
    # 1. 获取所有报告期
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT report_period, report_type, data_quality
        FROM financial_reports_raw
        WHERE stock_code = ?
        ORDER BY report_period DESC
    ''', (stock_code,))
    
    reports = cursor.fetchall()
    total = len(reports)
    print(f"📊 共有 {total} 个报告期")
    
    # 统计
    unverified = [r for r in reports if r[2] == 'UNVERIFIED']
    verified = [r for r in reports if r[2] == 'VERIFIED']
    conflicts = [r for r in reports if r[2] == 'CONFLICT']
    
    print(f"  ✅ 已验证: {len(verified)}")
    print(f"  ⚠️ 未验证: {len(unverified)}")
    print(f"  ❌ 冲突: {len(conflicts)}")
    print()
    
    conn.close()
    
    if len(unverified) == 0 and len(conflicts) == 0:
        print("🎉 所有数据均已验证！")
        # 即使已验证，也可能想重新跑一遍以更新 validation_details
        # return
    
    # 2. 下载 PDF
    print(f"步骤 1/2: 下载 {stock_code} 的 PDF 文件...")
    downloader = PDFDownloader()
    # 下载近3年的 PDF
    downloader.download(stock_code, lookback_days=365*3)
    print()
    
    # 3. 验证
    print(f"步骤 2/2: 验证数据质量...")
    
    # 从环境变量或参数获取 API Key
    if not gemini_api_key:
        gemini_api_key = os.getenv('GEMINI_API_KEY')
    
    if not gemini_api_key:
        print("⚠️ 未设置 GEMINI_API_KEY，将使用正则表达式验证（准确率较低）")
        use_llm = False
    else:
        use_llm = True
    
    validator = FinancialDataValidator(use_llm=use_llm, gemini_api_key=gemini_api_key)
    
    # 验证未验证的和有冲突的（重新验证以获取详情）
    targets = unverified + conflicts
    # 如果没有未验证的，就验证所有已下载的
    if not targets:
        targets = reports
        
    success_count = 0
    fail_count = 0
    
    for report_period, report_type, _ in targets:
        print(f"  验证 {report_period} ({report_type})...")
        # 检查是否需要验证
        txt_path = validator._get_txt_path(stock_code, report_period)
        if not txt_path:
            print(f"    ❌ PDF/TXT 文件不存在")
            fail_count += 1
            continue
            
        print(f"  🤖 使用 Gemini 提取财务数据...")
        result = validator.validate_report(stock_code, report_period)
        
        if result['status'] == 'VERIFIED':
            success_count += 1
            print(f"    ✅ 通过")
        elif result['status'] == 'CONFLICT':
            success_count += 1  # 虽然有冲突，但也算验证了
            print(f"    ⚠️ 发现冲突")
            if 'details' in result:
                for field, detail in result['details'].items():
                    if detail.get('status') == 'CONFLICT':
                        print(f"       - {field}: AkShare={detail['akshare']}亿, PDF={detail['pdf']}亿, 差异={detail['diff_pct']}%")
        else:
            fail_count += 1
            print(f"    ❌ {result.get('message', '验证失败')}")
    
    validator.close()
    
    print()
    print("=" * 50)
    print(f"✅ 验证完成！")
    print(f"  成功: {success_count}")
    print(f"  失败: {fail_count}")
    print("=" * 50)

if __name__ == "__main__":
    import sys
    
    # 从命令行参数读取
    if len(sys.argv) > 1:
        stock_code = sys.argv[1]
    else:
        stock_code = input("请输入股票代码（如 688005）: ")
    
    # API Key
    api_key = os.getenv('GEMINI_API_KEY')
    if not api_key:
        print("⚠️ 未设置 GEMINI_API_KEY 环境变量")
        print("使用方法: export GEMINI_API_KEY='your_key'")
        print("或者直接运行，将使用正则表达式（准确率较低）")
        print()
    
    batch_validate(stock_code, api_key)
