# 开发日志 (Changelog)

所有项目的重要变更都会记录在此文件中。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/)，
版本号遵循 [Semantic Versioning](https://semver.org/lang/zh-CN/)。

---

## [1.0.0] - 2025-11-23

### 新增 (Added)
- **数据抓取模块**
  - `data_fetcher.py`: 基于 AkShare 的 A股财务数据抓取器
  - 支持利润表、资产负债表、现金流量表的自动抓取
  - 数据从 2010 年开始，按季度/年度存储

- **财务指标计算**
  - `calculator.py`: 自动计算 15+ 核心财务指标
  - 盈利能力：ROE、ROA、毛利率、净利率
  - 成长能力：营收增长率、净利增长率（YoY）
  - 偿债能力：资产负债率、流动比率
  - 运营效率：存货周转天数、应收账款周转天数
  - 现金流：自由现金流、净现比、分红率

- **数据可视化**
  - `app.py`: 基于 Streamlit 的交互式数据看板
  - 全中文界面，指标名称带单位标识
  - 透视表展示（时间为横轴，指标为纵轴）
  - 报告类型筛选（年报/季报/半年报/一季报）
  - 数值格式化（金额自动转换为"亿"，比率显示"%"）

- **PDF 下载与解析**
  - `pdf_downloader.py`: 多市场财报下载器
    - A股：巨潮资讯网接口
    - 港股：巨潮资讯网接口
    - 美股：SEC Edgar 接口
  - `pdf_parser.py`: PDF → TXT 转换器（基于 PyMuPDF）
  - 下载后自动解析，生成同名 TXT 文件

- **数据质量验证**
  - `validator.py`: AkShare 数据与 PDF 原文的交叉验证引擎
  - 关键字段验证：营业收入、净利润、总资产、股东权益
  - 容差设置：允许 2% 的误差（考虑四舍五入）
  - 验证结果自动标记到数据库 (`data_quality` 字段)

- **数据库设计**
  - `database.py`: SQLite 数据库初始化脚本
  - `financial_reports_raw`: 原始财务数据表（含 `data_quality` 字段）
  - `financial_indicators_derived`: 衍生指标表
  - `financial_reports_files`: 文件路径记录表 ⭐ 新增
  - `metric_definitions`: 指标定义字典表

### 改进 (Changed)
- 优化数据抓取逻辑，修复日期处理 bug（AkShare 返回的未来日期问题）
- 改进 Streamlit 界面，移除投资评分功能，聚焦数据展示
- 优化缩进和代码结构，提升可读性

### 修复 (Fixed)
- 修复 `generate_report_name` 函数的日期转换错误
- 修复半年报被误判为年报的问题（优化标题解析逻辑）
- 修复 Streamlit 筛选器交互问题（引入 `session_state`）
- 修复 `df_derived` 表缺少 `report_type` 字段的问题（通过 merge 解决）

---

## [0.1.0] - 2025-11-21

### 初始版本
- 项目创建
- 基础数据库结构设计
- 简单的 Streamlit 原型（包含 mock 数据）
- AkShare 接口调试

---

## 未来计划 (Upcoming)

### [1.1.0] - 计划中
- [ ] Streamlit 界面增加"查看原文"按钮
- [ ] 显示数据质量标识（✅ 已验证 / ⚠️ 待复核）
- [ ] 支持手动触发"重新验证"
- [ ] 增加港股和美股数据的界面展示

### [2.0.0] - 长期规划
- [ ] LLM 集成（Gemini API）
- [ ] 财报智能问答功能
- [ ] 管理层讨论（MD&A）提取与总结
- [ ] 多股票对比分析
- [ ] TTM（滚动 12 个月）指标计算
- [ ] PE/PB 估值分析（结合实时股价）
- [ ] 数据导出功能（Excel/CSV/PDF）

---

## 技术债务 (Technical Debt)

### 数据质量
- [ ] PDF 文本提取的准确性需进一步验证（复杂表格识别）
- [ ] LLM 辅助验证的成本和速度优化

### 性能优化
- [ ] 大批量下载时的网络请求优化（并发控制）
- [ ] 数据库查询优化（索引添加）
- [ ] Streamlit 缓存策略优化

### 代码质量
- [ ] 单元测试覆盖率提升（当前 0%）
- [ ] 异常处理的完善（网络超时、文件损坏等）
- [ ] 日志系统的引入（替代 print）

---

## 贡献者 (Contributors)

- **Sonia (aisong78)**: 项目发起人、产品设计
- **Google Deepmind Antigravity**: AI 辅助开发工具

---

## 变更类型说明

- **新增 (Added)**: 新功能
- **改进 (Changed)**: 现有功能的变更
- **弃用 (Deprecated)**: 即将移除的功能
- **移除 (Removed)**: 已移除的功能
- **修复 (Fixed)**: Bug 修复
- **安全 (Security)**: 安全性修复
