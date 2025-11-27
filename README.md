# 视频数据同步系统

一个基于 Python 3.11+ 的企业级视频数据同步系统，采用 DDD（领域驱动设计）架构，支持从第三方 API 抓取视频元数据，上传到云存储（AWS S3/阿里云OSS），并同步到目标站点。

[![Python Version](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

---

## 🚀 核心特性

### 🏗️ 架构设计

- **分层架构**: Core（核心层）→ Domain（领域层）→ Infrastructure（基础设施层）→ Application（应用层）
- **依赖注入**: 使用自定义 DI 容器实现服务解耦
- **命令模式**: 每个功能独立封装为命令，便于维护和扩展
- **配置管理**: YAML 配置文件，结构清晰，易于管理

### 💻 代码质量

- **类型安全**: 100% 类型注解覆盖，通过 mypy 严格检查
- **代码规范**: 使用 Black + isort + Flake8 保证代码风格统一
- **测试覆盖**: Pytest 单元测试框架
- **PEP 604**: 使用现代化联合类型语法 (`str | None`)

### 📦 业务功能

- 📡 从第三方 API 批量抓取视频元数据
- 💾 MySQL 数据持久化，连接池管理
- ☁️ 支持 AWS S3 和阿里云 OSS 双云存储
- 🔐 AES 加密保护云存储资源路径
- 🌐 多站点数据同步推送
- 🔍 资源存在性检查（origin/index/cover）
- 🔧 失败资源自动修复
- 🏷️ 视频标签数据检查与修复
- 📊 状态持久化，支持断点续传
- 🧵 多线程并发处理，提升效率

---

## 📋 系统要求

- Python 3.11+
- MySQL 5.7+
- AWS S3 账户（可选）
- 阿里云 OSS 账户（可选）

---

## 📁 项目目录结构

```
video-sync-system/
├── src/                          # 源代码目录
│   ├── core/                     # 核心层
│   │   ├── config.py            # 配置管理器
│   │   ├── container.py         # 依赖注入容器
│   │   ├── exceptions.py        # 自定义异常
│   │   ├── logger.py            # 日志管理器
│   │   ├── protocols.py         # 协议接口定义
│   │   └── state.py             # 状态管理器
│   │
│   ├── domain/                   # 领域层
│   │   ├── models/              # 领域模型
│   │   │   ├── video.py         # 视频模型
│   │   │   ├── resource.py      # 资源模型
│   │   │   └── site.py          # 站点模型
│   │   └── services/            # 领域服务
│   │       ├── storage_service.py    # 存储服务
│   │       ├── site_service.py       # 站点同步服务
│   │       └── video_tag_service.py  # 视频标签服务
│   │
│   ├── infrastructure/           # 基础设施层
│   │   ├── database/            # 数据库
│   │   │   ├── pool.py          # 连接池
│   │   │   └── repository.py    # 仓储实现
│   │   ├── http/                # HTTP 客户端
│   │   │   └── client.py        # 带重试的HTTP客户端
│   │   └── storage/             # 云存储适配器
│   │       ├── s3.py            # AWS S3 适配器
│   │       ├── oss.py           # 阿里云 OSS 适配器
│   │       └── factory.py       # 存储工厂
│   │
│   ├── application/              # 应用层
│   │   └── commands/            # 命令集合（17个命令）
│   │       ├── base.py          # 命令基类
│   │       ├── registry.py      # 命令注册器
│   │       ├── scraper.py       # 数据抓取命令
│   │       ├── s3_origin_check.py       # S3 origin 检查
│   │       ├── s3_index_check.py        # S3 index 检查
│   │       ├── s3_cover_check.py        # S3 cover 检查
│   │       ├── s3_origin_fix.py         # S3 origin 修复
│   │       ├── s3_index_fix.py          # S3 index 修复
│   │       ├── s3_cover_fix.py          # S3 cover 修复
│   │       ├── oss_origin_check.py      # OSS origin 检查
│   │       ├── oss_index_check.py       # OSS index 检查
│   │       ├── oss_cover_check.py       # OSS cover 检查
│   │       ├── oss_origin_fix.py        # OSS origin 修复
│   │       ├── oss_index_fix.py         # OSS index 修复
│   │       ├── oss_cover_fix.py         # OSS cover 修复
│   │       ├── site_fix.py              # 站点同步修复
│   │       ├── site_clean.py            # 站点数据清理
│   │       ├── video_tag_check.py       # 标签检查
│   │       └── video_tag_fix.py         # 标签修复
│   │
│   └── utils/                    # 工具函数
│       └── encryption.py        # AES 加密工具
│
├── config/                       # 配置目录
│   └── config.example.yaml      # 配置文件模板
│
├── logs/                         # 日志目录（自动创建）
│   └── YYYYMMDD_HH/             # 按日期和小时分目录
│       └── command_name.log     # 各命令日志
│
├── tests/                        # 测试目录
│   └── test_core.py             # 核心层测试
│
├── main.py                       # 主入口（直接执行命令）
├── run.py                        # 快速启动脚本（交互式菜单）
├── state.json                    # 状态文件（自动生成）
├── state.json.example           # 状态文件模板
├── pyproject.toml               # 项目配置
├── requirements.txt             # 依赖列表
├── mypy.ini                     # Mypy 配置
├── .flake8                      # Flake8 配置
└── README.md                    # 项目文档
```

---

## 🛠️ 安装部署

### 1. 克隆项目

```bash
git clone https://github.com/zhangqingxi/video-sync-system.git
cd video-sync-system
```

### 2. 创建虚拟环境

```bash
# 使用 conda
conda create -n video-sync-system python=3.11
conda activate video-sync-system

# 或使用 venv
python -m venv .venv
.venv\Scripts\activate  # Windows
source .venv/bin/activate  # Linux/Mac
```

### 3. 安装依赖

**推荐方式**（使用 pyproject.toml）：

```bash
# 仅安装运行时依赖
pip install -e .

# 安装开发依赖（包含类型检查、格式化、测试工具）
pip install -e ".[dev]"
```

**传统方式**（使用 requirements.txt）：

```bash
pip install -r requirements.txt
```

### 4. 配置文件

```bash
# 复制配置模板
cp config/config.example.yaml config/config.yaml
cp state.json.example state.json

# 编辑 config/config.yaml 填入真实配置
```

配置文件示例（`config/config.yaml`）：

```yaml
database:
  host: localhost
  port: 3306
  user: root
  password: your_password
  database: video_sync
  video_table_name: your_video_table

storage:
  s3:
    access_key: YOUR_S3_ACCESS_KEY
    secret_key: YOUR_S3_SECRET_KEY
    region: us-east-1
    bucket: your-s3-bucket
    encryption_key: your_32_chars_encryption_key
  
  oss:
    access_key: YOUR_OSS_ACCESS_KEY
    secret_key: YOUR_OSS_SECRET_KEY
    region: oss-cn-hangzhou
    bucket: your-oss-bucket
    encryption_key: your_32_chars_encryption_key

api:
  base_url: https://api.example.com
  username: your_username
  password: your_password

site:
  domains:
    - https://site1.example.com
    - https://site2.example.com
  api_token: your_api_token

threads:
  check: 4   # 检查线程数
  sync: 4    # 同步线程数
```

---

## 📖 使用指南

### 方式一：快速启动（推荐）

使用 `run.py` 交互式菜单：

```bash
python run.py
```

会显示如下菜单：

```
============================================================
       视频同步系统 v3.0 - 快速启动
============================================================

【检查命令】
  1. s3_origin_check    - 检查S3 origin m3u8资源
  2. s3_index_check     - 检查S3 index m3u8资源
  3. s3_cover_check     - 检查S3 cover资源
  4. oss_origin_check   - 检查OSS origin m3u8资源
  5. oss_index_check    - 检查OSS index m3u8资源
  6. oss_cover_check    - 检查OSS cover资源

【同步命令】
  7. scraper            - 从第三方API抓取视频数据

【修复命令】
  8. s3_origin_fix      - 修复失败的S3 origin m3u8资源
  9. s3_index_fix       - 修复失败的S3 index m3u8资源
  10. s3_cover_fix      - 修复失败的S3 cover资源
  11. oss_origin_fix    - 修复失败的OSS origin m3u8资源
  12. oss_index_fix     - 修复失败的OSS index m3u8资源
  13. oss_cover_fix     - 修复失败的OSS cover资源

【站点命令】
  14. site_fix          - 同步失败的视频数据到所有配置的站点
  15. site_clean        - 清理站点数据

【标签命令】
  16. video_tag_check   - 检测视频标签数据
  17. video_tag_fix     - 修复缺失的视频标签数据

============================================================
请选择要执行的命令（输入编号或命令名称）：
============================================================
```

### 方式二：命令行直接执行

```bash
python main.py <command>
```

### 所有可用命令详解

#### 📊 数据抓取

```bash
# 从第三方API抓取视频数据，并自动上传到S3/OSS，同步到站点
python main.py scraper
```

#### 🔍 S3 资源检查

```bash
# 检查 S3 origin.m3u8 文件是否存在
python main.py s3_origin_check

# 检查 S3 index.m3u8 文件是否存在
python main.py s3_index_check

# 检查 S3 cover.jpg 文件是否存在
python main.py s3_cover_check
```

#### 🔍 OSS 资源检查

```bash
# 检查 OSS origin.m3u8 文件是否存在
python main.py oss_origin_check

# 检查 OSS index.m3u8 文件是否存在
python main.py oss_index_check

# 检查 OSS cover.jpg 文件是否存在
python main.py oss_cover_check
```

#### 🔧 S3 资源修复

```bash
# 修复失败的 S3 origin.m3u8 资源
python main.py s3_origin_fix

# 修复失败的 S3 index.m3u8 资源
python main.py s3_index_fix

# 修复失败的 S3 cover.jpg 资源
python main.py s3_cover_fix
```

#### 🔧 OSS 资源修复

```bash
# 修复失败的 OSS origin.m3u8 资源
python main.py oss_origin_fix

# 修复失败的 OSS index.m3u8 资源
python main.py oss_index_fix

# 修复失败的 OSS cover.jpg 资源
python main.py oss_cover_fix
```

#### 🌐 站点同步

```bash
# 同步失败的视频数据到所有配置的站点
python main.py site_fix

# 清理站点数据
python main.py site_clean
```

#### 🏷️ 标签管理

```bash
# 检测视频标签数据（检查哪些视频缺少标签）
python main.py video_tag_check

# 修复缺失的视频标签数据
python main.py video_tag_fix
```

---

## 🔍 代码质量检查

### 类型检查

```bash
# 使用 mypy 进行类型检查
mypy src/ main.py
```

### 代码格式化

```bash
# 使用 black 格式化代码
black src/ tests/ main.py run.py

# 使用 isort 排序导入语句
isort src/ tests/ main.py run.py
```

### 代码规范检查

```bash
# 使用 flake8 检查代码规范
flake8 src/ tests/ main.py run.py
```

### 运行测试

```bash
# 运行所有测试
pytest

# 查看测试覆盖率
pytest --cov=src --cov-report=html
```

---

## 📊 工作流程示例

### 完整的数据同步流程

```bash
# 1. 抓取数据并同步
python main.py scraper

# 2. 检查 S3 上传情况
python main.py s3_origin_check
python main.py s3_index_check
python main.py s3_cover_check

# 3. 检查 OSS 上传情况
python main.py oss_origin_check
python main.py oss_index_check
python main.py oss_cover_check

# 4. 修复失败的资源
python main.py s3_origin_fix
python main.py oss_origin_fix

# 5. 同步到站点
python main.py site_fix

# 6. 检查并修复标签
python main.py video_tag_check
python main.py video_tag_fix
```

---

## 📝 更新日志

### Version 3.0.0 (2025-11-27)

- ✨ 采用 DDD 分层架构重构整个系统
- ✨ 使用依赖注入容器管理服务
- ✨ 升级到 Python 3.11+，使用 PEP 604 联合类型
- ✨ 完整的类型注解覆盖
- ✨ 迁移到 YAML 配置文件
- ✨ 命令模式重构所有功能
- ✨ 新增视频标签检查和修复功能
- ✨ 日志按命令分文件记录
- ✨ 新增快速启动脚本 `run.py`
- ✨ 添加 pyproject.toml 支持
- 🐛 修复已知问题

---

## 👨‍💻 作者

**Qasim**

- Email: 1575078379l@163.com
- GitHub: [@zhangqingxi](https://github.com/zhangqingxi)


