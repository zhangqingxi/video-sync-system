# 视频数据同步系统

一个基于Python 3.11+的企业级视频数据同步系统，支持从第三方API抓取视频元数据，上传到云存储（AWS S3/阿里云OSS），并同步到目标站点。

[![Python Version](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Code Style](https://img.shields.io/badge/code%20style-PEP8-orange.svg)](https://www.python.org/dev/peps/pep-0008/)

## 🚀 核心特性

### Python 3.11+ 企业级最佳实践

1. **现代化类型提示 (PEP 604)**
   - ✅ 使用 `|` 联合类型替代 `Union`
   - ✅ 使用小写 `list`, `dict` 替代 `List`, `Dict`
   - ✅ 使用 `str | None` 替代 `Optional[str]`
   - ✅ 完整的类型注解覆盖

2. **统一日志标准**
   - ✅ 按小时自动分级日志文件
   - ✅ 文件+控制台双输出
   - ✅ 统一格式化输出
   - ✅ 模块级日志记录器

3. **完善的文档注释**
   - ✅ Google风格文档字符串
   - ✅ 完整的函数/类/方法注释
   - ✅ 参数类型和返回值说明
   - ✅ 使用示例和注意事项

4. **显式参数调用**
   - ✅ 所有函数调用使用关键字参数
   - ✅ 提高代码可读性和可维护性
   - ✅ 避免参数位置错误

5. **统一Session管理**
   - ✅ API请求使用Session池化连接
   - ✅ 自动重试和错误处理
   - ✅ 连接池优化配置

### 业务功能特性

- 📡 **API数据抓取**: 从第三方API批量获取视频元数据
- 💾 **数据库存储**: MySQL数据持久化，支持自动重连
- ☁️ **云存储上传**: 支持AWS S3和阿里云OSS双存储
- 🔐 **AES路径加密**: 保护云存储资源路径安全
- 🌐 **站点数据同步**: 多域名站点数据推送
- 🔄 **失败自动修复**: 智能重试机制
- 📊 **状态持久化**: 断点续传支持

## 📋 系统要求

- Python 3.11+
- MySQL 5.7+
- AWS S3 账户（可选）
- 阿里云OSS 账户（可选）

## 📁 项目结构

```bash
video-sync-system/
├── core/ # 核心模块
│ ├── init.py # 模块导出
│ ├── api_handler.py # API请求处理器
│ ├── db_handler.py # 数据库处理器
│ ├── logger_handler.py # 日志处理器
│ ├── oss_handler.py # 阿里云OSS处理器
│ ├── s3_handler.py # AWS S3处理器
│ ├── site_handler.py # 站点同步处理器
│ └── util_handler.py # 工具函数
├── logs/ # 日志目录
│ └── YYYYMMDD/ # 按日期分组
│ ├── 00.log # 按小时存储
│ └── ...
├── main.py # 主入口文件
├── config.ini # 配置文件（需自行创建）
├── config.ini.example # 配置模板
├── state.json # 状态文件（自动生成）
├── state.json.example # 状态模板
├── requirements.txt # 依赖清单
├── README.md # 项目文档
└── .gitignore # Git忽略配置


## 🛠️ 安装部署

### 1. 克隆项目

```bash
git clone https://github.com/yourusername/video-sync-system.git
cd video-sync-system
```

### 2. 创建虚拟环境
```bash
conda create -n video-sync-system python=3.11
```

### 3. 安装依赖
```bash
pip install -r requirements.txt
```

### 4. 配置文件

```bash
# 复制配置模板并填入实际配置：
cp config.ini.example config.ini
cp state.json.example state.json

# 编辑 `config.ini` 填入你的配置信息：
[database]
host = 127.0.0.1
user = your_db_user
password = your_db_password
database = your_database

[api]
base_url = https://your-api.com
username = your_username
password = your_password

[aws_s3]
access_key_id = YOUR_ACCESS_KEY
secret_access_key = YOUR_SECRET_KEY
bucket_name = your-bucket

[oss]
region = cn-hangzhou
access_key_id = YOUR_ACCESS_KEY
secret_access_key = YOUR_SECRET_KEY
bucket_name = your-bucket


## 📖 使用指南
### 命令行接口
```bash
# 抓取API数据并同步
python main.py scraper

# 修复S3上传失败的数据
python main.py s3_fix

# 修复OSS上传失败的数据
python main.py oss_fix

# 修复站点同步失败的数据
python main.py site_fix

# 清理站点数据
python main.py site_clean
```

## 🔍 代码质量保证
### 类型检查
```bash
# 使用mypy进行类型检查
mypy core/ main.py
```

### 代码风格
```bash
# 使用black格式化代码
black core/ main.py

# 使用flake8检查代码规范
flake8 core/ main.py
```


## 📝 更新日志

### Version 2.0.0 (2025-01-14)

- ✨ 升级到Python 3.11+
- ✨ 使用PEP 604联合类型
- ✨ 统一日志系统
- ✨ 完善文档注释
- ✨ 显式参数调用
- ✨ Session统一管理
- 🐛 修复已知问题


## 👨‍💻 作者

**Qasim**

- Email: 1575078379l@163.com
- GitHub: [@zhangqingxi](https://github.com/zhangqingxi)


