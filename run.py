#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
快速启动脚本

提供便捷的命令执行方式。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

import sys
import urllib3
import subprocess
from pathlib import Path

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def print_banner()-> None:
    """打印横幅"""
    print("=" * 60)
    print("       视频同步系统 v3.0 - 快速启动")
    print("=" * 60)
    print()


def print_commands()-> None:
    """打印可用命令"""
    commands: dict[str, list[tuple[str, str]]] = {
        "检查命令": [
            ("s3_origin_check", "检查S3 origin m3u8资源"),
            ("s3_index_check", "检查S3 index m3u8资源"),
            ("s3_cover_check", "检查S3 cover资源"),
            ("oss_origin_check", "检查OSS origin m3u8资源"),
            ("oss_index_check", "检查OSS index m3u8资源"),
            ("oss_cover_check", "检查OSS cover资源"),
        ],
        "同步命令": [
            ("scraper", "从第三方API抓取视频数据"),
        ],
        "修复命令": [
            ("s3_origin_fix", "修复失败的S3 origin m3u8资源"),
            ("s3_index_fix", "修复失败的S3 index m3u8资源"),
            ("s3_cover_fix", "修复失败的S3 cover资源"),
            ("oss_origin_fix", "修复失败的OSS origin m3u8资源"),
            ("oss_index_fix", "修复失败的OSS index m3u8资源"),
            ("oss_cover_fix", "修复失败的OSS cover资源"),
        ],
        "站点命令": [
            ("site_fix", "同步失败的视频数据到所有配置的站点"),
            ("site_clean", "清理站点数据"),
        ],
        "标签命令": [
            ("video_tag_check", "检测视频标签数据"),
            ("video_tag_fix", "修复缺失的视频标签数据"),
        ]
    }

    num: int = 1
    for category, cmds in commands.items():
        print(f"\n【{category}】")
        for cmd, desc in cmds:
            print(f"  {num}. {cmd:20s} - {desc}")
            num += 1


def main() -> int:
    """主函数"""
    print_banner()
    
    # 检查配置文件
    config_file: Path = Path("config/config.yaml")
    if not config_file.exists():
        print("❌ 错误: 配置文件不存在")
        print("请先复制 config/config.example.yaml 为 config/config.yaml")
        print("并填入真实的配置信息")
        return 1
    
    print_commands()
    
    print("\n" + "=" * 60)
    print("请选择要执行的命令（输入编号或命令名称）：")
    print("=" * 60)
    
    choice: str = input("\n> ").strip()
    
    # 命令映射
    command_map: dict[str, str] = {
        "1": "s3_origin_check",
        "2": "s3_index_check",
        "3": "s3_cover_check",
        "4": "oss_origin_check",
        "5": "oss_index_check",
        "6": "oss_cover_check",
        "7": "scraper",
        "8": "s3_origin_fix",
        "9": "s3_index_fix",
        "10": "s3_cover_fix",
        "11": "oss_origin_fix",
        "12": "oss_index_fix",
        "13": "oss_cover_fix",
        "14": "site_fix",
        "15": "site_clean",
        "16": "video_tag_check",
        "17": "video_tag_fix",
    }
    
    # 获取命令
    command: str | None
    if choice in command_map:
        # 如果choice是key，获取对应的value
        command = command_map[choice]
    elif choice in command_map.values():
        # 如果choice已经是value，直接使用
        command = choice
    else:
        print("❌ 无效的选择")
        return 1
    
    print(f"\n🚀 启动命令: {command}")
    print("=" * 60)
    print()
    
    # 执行命令
    try:
        subprocess.run([sys.executable, "main.py", command], check=True)
        return 0
    except subprocess.CalledProcessError as e:
        print(f"\n❌ 命令执行失败: {e}")
        return 1
    except KeyboardInterrupt:
        print("\n\n⚠️  用户中断")
        return 0


if __name__ == '__main__':
    sys.exit(main())
