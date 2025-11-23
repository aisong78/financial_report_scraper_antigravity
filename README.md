# 📊 Antigravity 智能财报分析系统

> 基于 Python + Streamlit + AkShare 构建的全栈财务数据分析工具  
> 支持 A股、港股、美股的财报数据抓取、解析、验证与可视化

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.28-red.svg)](https://streamlit.io/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## ✨ 核心特性

### 📈 数据抓取
- **多市场支持**: A股（AkShare）、港股（巨潮资讯）、美股（SEC Edgar）
- **结构化数据**: 自动抓取利润表、资产负债表、现金流量表
- **PDF 原文下载**: 一键下载并解析财报 PDF，保存为 TXT 供后续分析

### 🧮 智能计算
- **自动化指标计算**: ROE、毛利率、净利率、增长率等 15+ 核心指标
- **同比分析**: 自动计算 YoY 增长率，识别趋势变化
- **质量验证**: 交叉验证 AkShare 数据与 PDF 原文，确保准确性

### 📊 数据可视化
- **全中文界面**: 指标名称、单位、格式化数字一目了然
- **动态筛选**: 按报告类型（年报/季报）筛选数据
- **透视表展示**: 时间为横轴，指标为纵轴，便于纵向对比

### 🛡️ 数据验证与质量控制

本项目实现了严谨的数据质量控制闭环，确保财务数据的准确性。

### 1. 双重数据源验证
*   **Primary Source**: AkShare (新浪财经/东方财富) 接口获取结构化数据。
*   **Secondary Source**: 自动下载财报 PDF 原文，使用 **Google Gemini LLM** 提取关键财务指标。
*   **交叉验证**: 系统自动比对两个来源的数据。
    *   **MATCH**: 差异 < 5%，标记为 ✅ 已验证。
    *   **CONFLICT**: 差异 > 5%，标记为 ❌ 数据冲突。
    *   **AUTO_FILLED**: AkShare 缺失但 PDF 有数据，**自动回填**数据库。

### 2. 冲突可视化与处理
*   **UI 高亮**: 在 Streamlit 界面中，有冲突的单元格会以**淡红色背景+红色文字**高亮显示。
*   **详细报告**: "详细验证状态"面板会列出具体冲突的字段、AkShare 值、PDF 值及差异百分比。

### 3. 手动修正与锁定 (Manual Override)
*   提供 **"✏️ 修正数据"** 功能，允许用户手动修改任意字段。
*   **数据锁定**: 手动修改后的数据会被标记为 `is_locked=1`。
*   **保护机制**: 后续的自动抓取程序会**跳过**已锁定的记录，防止人工修正的数据被覆盖。

### 4. 数据获取策略
*   **范围**: 默认抓取 2010 年至今的所有定期报告（年报/半年报/季报）。
*   **覆盖规则**: 默认全量覆盖旧数据，除非数据被手动锁定。

---

## 🚀 快速开始

### 环境要求
- Python 3.11+
- Conda（推荐）

### 安装步骤

1. **克隆仓库**
```bash
git clone https://github.com/aisong78/financial_report_scraper_antigravity.git
cd financial_report_scraper_antigravity
```

2. **创建 Conda 环境**
```bash
conda create -n finance python=3.11
conda activate finance
```

3. **安装依赖**
```bash
pip install -r requirements.txt
```

4. **初始化数据库**
```bash
python database.py
```

5. **运行应用**
```bash
streamlit run app.py
```

应用会自动在浏览器中打开 `http://localhost:8501`

---

## 📖 使用指南

### 1. 查看财务数据
1. 在侧边栏选择股票代码（默认有茅台、容百科技等）
2. 点击"加载/刷新数据"按钮
3. 在主界面查看：
   - **核心财务指标**：ROE、毛利率、增长率等
   - **原始财务报表**：利润表、资产负债表、现金流量表

### 2. 筛选数据
- 使用"只看哪种报表？"下拉菜单筛选特定类型的财报
- 支持：全部、年报 (A)、三季报 (Q3)、半年报 (S1)、一季报 (Q1)

### 3. 下载财报原文
```python
from pdf_downloader import PDFDownloader

downloader = PDFDownloader()
# 下载容百科技最近 3 年的财报
downloader.download("688005", lookback_days=365*3)
```

### 4. 数据验证
```python
from validator import FinancialDataValidator

validator = FinancialDataValidator()
result = validator.validate_report("688005", "2023-12-31")
print(f"验证状态: {result['status']}")
print(f"详细结果: {result['details']}")
```

---

## 🏗️ 系统架构

```
┌─────────────────┐
│  Streamlit UI   │  ← 用户界面
└────────┬────────┘
         │
    ┌────▼────┐
    │ app.py  │  ← 主应用逻辑
    └────┬────┘
         │
    ┌────▼─────────────────────┐
    │  数据获取层               │
    ├──────────────────────────┤
    │ ▶ data_fetcher.py        │ ← AkShare API 调用
    │ ▶ pdf_downloader.py      │ ← PDF 下载（巨潮/SEC）
    │ ▶ pdf_parser.py          │ ← PDF → TXT 转换
    └────┬─────────────────────┘
         │
    ┌────▼─────────────────────┐
    │  数据处理层               │
    ├──────────────────────────┤
    │ ▶ calculator.py          │ ← 财务指标计算
    │ ▶ validator.py           │ ← 数据交叉验证
    └────┬─────────────────────┘
         │
    ┌────▼─────────────────────┐
    │  数据存储层 (SQLite)      │
    ├──────────────────────────┤
    │ • financial_reports_raw  │ ← 原始财务数据
    │ • financial_indicators_derived │ ← 衍生指标
    │ • financial_reports_files│ ← 文件路径记录
    │ • metric_definitions     │ ← 指标字典
    └──────────────────────────┘
```

---

## 📁 项目结构

```
financial_report_scraper_antigravity/
├── app.py                    # Streamlit 主应用
├── database.py               # 数据库初始化脚本
├── data_fetcher.py           # AkShare 数据抓取器
├── calculator.py             # 财务指标计算器
├── pdf_downloader.py         # PDF 下载器（多市场支持）
├── pdf_parser.py             # PDF → TXT 解析器
├── validator.py              # 数据交叉验证器
├── finance.db                # SQLite 数据库（不跟踪）
├── downloads/                # 下载的 PDF/TXT 文件（不跟踪）
├── requirements.txt          # Python 依赖
├── README.md                 # 本文档
└── CHANGELOG.md              # 开发日志
```

---

## 🗄️ 数据库设计

### 表结构

#### 1. `financial_reports_raw` (原始财务数据)
存储从 AkShare 抓取的原始财务数据。

| 字段 | 类型 | 说明 |
|------|------|------|
| stock_code | TEXT | 股票代码 |
| report_period | TEXT | 报告期 (如 2023-12-31) |
| report_type | TEXT | 报告类型 (Q1/S1/Q3/A) |
| data_quality | TEXT | 数据质量标记 ⭐ |
| revenue | REAL | 营业收入 |
| total_assets | REAL | 总资产 |
| ... | ... | 其他 40+ 财务字段 |

#### 2. `financial_indicators_derived` (衍生指标)
存储计算得出的财务指标。

| 字段 | 说明 |
|------|------|
| roe | 净资产收益率 (%) |
| gross_margin | 毛利率 (%) |
| revenue_yoy | 营收增长率 (%) |
| ... | 其他 15+ 指标 |

#### 3. `financial_reports_files` (文件记录) ⭐ 新增
记录下载的财报文件信息。

| 字段 | 说明 |
|------|------|
| file_path | PDF 文件路径 |
| txt_path | 解析后的 TXT 路径 |
| parse_status | 解析状态 (SUCCESS/PENDING/FAILED) |

---

## 🔬 技术栈

- **前端**: [Streamlit](https://streamlit.io/) - 快速构建数据应用
- **数据源**:
  - [AkShare](https://github.com/akfamily/akshare) - A股数据接口
  - [巨潮资讯](http://www.cninfo.com.cn/) - 官方披露平台
  - [SEC Edgar](https://www.sec.gov/edgar) - 美股财报
- **PDF 处理**: [PyMuPDF](https://pymupdf.readthedocs.io/) - PDF 解析
- **数据库**: SQLite - 轻量级本地存储
- **数据分析**: Pandas、NumPy

---

## 🛣️ Roadmap

### ✅ 已完成 (v1.0)
- [x] A股数据抓取与存储
- [x] 财务指标自动计算
- [x] Streamlit 数据看板
- [x] PDF 下载与解析
- [x] 数据交叉验证引擎

### 🚧 进行中 (v1.1)
- [ ] Streamlit 界面增加"查看原文"按钮
- [ ] 支持港股和美股数据展示
- [ ] 增加数据质量标识显示

### 📋 计划中 (v2.0)
- [ ] LLM 集成：智能解读财报文本
- [ ] 管理层讨论（MD&A）提取与总结
- [ ] 财报智能问答（Q&A）
- [ ] 多股票对比分析
- [ ] 数据导出功能（Excel/CSV）

---

## 🤝 贡献指南

欢迎提交 Issue 和 Pull Request！

### 开发流程
1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

---

## 📄 License

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件

---

## 🙏 致谢

- [AkShare](https://github.com/akfamily/akshare) - 提供免费的 A股数据接口
- [Streamlit](https://streamlit.io/) - 提供优秀的 Web 框架
- [Google Deepmind Antigravity](https://www.deepmind.google/) - AI 辅助开发工具

---

## 📧 联系方式

- GitHub: [@aisong78](https://github.com/aisong78)
- 项目地址: [financial_report_scraper_antigravity](https://github.com/aisong78/financial_report_scraper_antigravity)

---

**⚠️ 免责声明**: 本工具仅供学习和研究使用，不构成任何投资建议。投资有风险，决策需谨慎。
