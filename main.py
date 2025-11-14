"""
项目统一主入口

通过命令行参数分发任务，支持数据抓取、修复、清理等多种操作。

Author: Qasim
Version: 2.0
Python: 3.11+
Date: 2025-01-14
"""

import argparse
import logging
import time
from typing import Any
import urllib3

from core import (
    ApiHandler,
    DatabaseHandler,
    OSSHandler,
    S3Handler,
    SiteHandler,
    load_config,
    load_state,
    save_state,
    setup_logger,
)

# ============================================================================
# 全局配置
# ============================================================================

# 初始化日志系统（必须在最开始执行）
setup_logger()

# 禁用SSL警告（开发环境）
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 模块级日志记录器
logger: logging.Logger = logging.getLogger(__name__)


# ============================================================================
# 核心业务函数
# ============================================================================

def run_scraper(config: Any) -> None:
    """
    执行API数据抓取任务
    
    从第三方API批量抓取视频元数据，保存到数据库，上传到云存储，并同步到站点。
    
    Args:
        config: 项目配置对象
        
    Business Logic:
        1. 读取上次同步的页码
        2. 循环获取API数据并存入数据库
        3. 上传视频文件到OSS/S3
        4. 同步数据到目标站点
        5. 处理Token过期的自动重新登录
        6. 保存同步进度状态
        
    Example:
        >>> from core import load_config
        >>> config = load_config()
        >>> run_scraper(config=config)
    """
    logger.info("=" * 80)
    logger.info("启动API数据抓取脚本")
    logger.info("=" * 80)
    
    # 加载状态
    state: dict[str, Any] = load_state()
    current_page: int = state.get('last_synced_page', 0)
    failed_synced_ids: list[int] = state.get('failed_synced_ids', [])
    failed_site: dict[str, set] = state.get('failed_site', {})
    
    # 从头开始时重置页码
    if current_page == 0:
        current_page = 1
        logger.info("从第1页开始抓取")
    else:
        logger.info(f"从第{current_page}页继续抓取")
    
    # 初始化处理器
    api: ApiHandler = ApiHandler(config=config)
    db: DatabaseHandler = DatabaseHandler(config=config)
    oss_handler: OSSHandler = OSSHandler(config=config)
    site_handler: SiteHandler = SiteHandler(config=config)
    
    # 设置Token
    token: str | None = state.get('api_token')
    if token:
        api.set_token(token=token)
        logger.info("使用缓存的API Token")
    
    try:
        while True:
            # 获取当前页视频列表
            response_data: dict[str, Any] | None = api.fetch_video_page(page_number=current_page)
            
            # 检查请求是否失败
            if response_data is None:
                logger.error(f"API请求失败 (页码: {current_page})，脚本终止")
                break
            
            response_code: int = response_data.get('code')
            
            # 处理Token过期
            if response_code == 402:
                logger.warning("Token已过期或无效，正在尝试重新登录...")
                new_token: str | None = api.login()
                
                if new_token:
                    state['api_token'] = new_token
                    save_state(data=state)
                    logger.info("重新登录成功，将重试请求当前页面")
                    continue  # 重试当前页
                else:
                    logger.error("重新登录失败，脚本终止")
                    break
            
            # 处理正常响应
            elif response_code == 0:
                videos: list[dict[str, Any]] = response_data.get('data', [])
                
                if not videos:
                    logger.info(f"第{current_page}页未获取到数据，抓取结束")
                    break
                
                logger.info(f"第{current_page}页获取到 {len(videos)} 条视频数据")
                
                # 当前页成功处理的视频ID集合
                processed_ids: set[str] = set()
                
                # 处理每个视频
                for video in videos:
                    douban_id: str = video.get('id')
                    title: str = video.get('title', '')
                    
                    # 检查是否已存在
                    if db.video_exists(douban_id=douban_id):
                        logger.info(f"视频已存在，跳过: '{title}' (ID: {douban_id})")
                        continue
                    
                    # 获取视频详情
                    logger.info(f"获取视频详情: '{title}' (ID: {douban_id})")
                    detail_response: dict[str, Any] | None = api.fetch_video_details(
                        douban_id=douban_id
                    )
                    
                    if detail_response and detail_response.get('code') == 0:
                        details: dict[str, Any] | None = detail_response.get('data')
                        
                        if not details:
                            logger.warning(f"视频详情为空，跳过: {douban_id}")
                            continue
                        
                        # 更新视频数据
                        video_list: list[str] = details.get('video_list', [])
                        download_url: str = details.get('download_url', '')
                        cover: str = details.get('cover', '')
                        desc: str = details.get('desc', '') or details.get('c_desc', '')
                        free_watch_episodes: int = details.get('free_watch_episodes', 0)
                        
                        video.update({
                            'title': title,
                            'video_list': video_list,
                            'download_url': download_url,
                            'cover': cover,
                            'desc': desc,
                            'free_watch_episodes': free_watch_episodes
                        })
                    else:
                        logger.warning(f"获取视频详情失败: {douban_id}")
                        continue
                    
                    # 插入数据库
                    if not db.insert_video(video_data=video):
                        logger.error(f"数据库插入失败: {douban_id}")
                        continue
                    
                    processed_ids.add(douban_id)
                    
                    # 上传到OSS
                    try:
                        logger.info(f"开始上传到OSS: '{title}' (ID: {douban_id})")
                        result: bool = oss_handler.process_single_video_sync(
                            douban_id=int(douban_id),
                            title=title,
                            video_list=video_list,
                            cover=cover
                        )
                        
                        if not result:
                            logger.error(f"OSS同步失败: {douban_id}")
                            failed_synced_ids.append(int(douban_id))
                    except Exception as e:
                        logger.error(f"OSS同步异常: {douban_id}, 错误: {e}")
                        failed_synced_ids.append(int(douban_id))
                    
                    # 避免请求过快
                    time.sleep(0.5)
                
                # 同步到站点
                if processed_ids:
                    try:
                        logger.info(f"开始同步 {len(processed_ids)} 个视频到站点")
                        
                        # 从数据库查询视频数据
                        site_videos: list[dict[str, Any]] = db.get_videos_by_ids(
                            douban_ids=list(processed_ids)
                        )
                        
                        if not site_videos:
                            raise Exception("从数据库查询视频数据为空")
                        
                        # 同步到站点
                        sync_failed_ids: dict[str, set[str]] = site_handler.sync_videos_to_site(
                            videos=site_videos
                        )
                        
                        # 更新失败记录
                        if sync_failed_ids:
                            for domain, domain_failed_ids in sync_failed_ids.items():
                                if domain in failed_site:
                                    failed_site[domain] |= domain_failed_ids
                                else:
                                    failed_site[domain] = domain_failed_ids
                                    
                    except Exception as e:
                        logger.error(f"站点同步失败: {e}")
                        
                        # 记录所有视频到所有域名的失败列表
                        domains_str: str = config.get('site', 'domains', fallback='')
                        domains: list[str] = [
                            d.strip() for d in domains_str.split(',') if d.strip()
                        ]
                        
                        for domain in domains:
                            failed_site.setdefault(domain, set()).update(processed_ids)
                    finally:
                        site_handler.close()
                else:
                    logger.info("本页没有需要同步到站点的新视频")
                
                # 保存状态
                state['last_synced_page'] = current_page
                state['failed_synced_ids'] = failed_synced_ids
                state['failed_site'] = {k: list(v) for k, v in failed_site.items()}
                save_state(data=state)
                
                # 处理下一页
                current_page += 1
                time.sleep(1)
                
            else:
                # 其他API错误
                logger.error(f"API返回无法处理的错误 (Code: {response_code})，脚本终止")
                break
                
    finally:
        # 清理资源
        db.close()
        api.close()
        oss_handler.close()
        logger.info("=" * 80)
        logger.info("API数据抓取脚本执行结束")
        logger.info("=" * 80)


def run_oss_fixer(config: Any) -> None:
    """
    执行OSS数据修复任务
    
    重新上传之前失败的视频文件到OSS。
    
    Args:
        config: 项目配置对象
        
    Example:
        >>> from core import load_config
        >>> config = load_config()
        >>> run_oss_fixer(config=config)
    """
    logger.info("=" * 80)
    logger.info("启动OSS数据修复脚本")
    logger.info("=" * 80)
    
    state: dict[str, Any] = load_state()
    fix_ids: list[int] = state.get('failed_synced_ids', [])
    
    if not fix_ids:
        logger.info("没有需要修复的OSS记录")
        return
    
    logger.info(f"需要修复的记录数: {len(fix_ids)}")
    
    # 初始化处理器
    api: ApiHandler = ApiHandler(config=config)
    oss_handler: OSSHandler = OSSHandler(config=config)
    
    # 设置Token
    token: str | None = state.get('api_token')
    if token:
        api.set_token(token=token)
    
    failed_synced_ids: list[int] = []
    
    try:
        for douban_id in fix_ids:
            logger.info(f"开始修复: {douban_id}")
            
            # 最多重试2次
            max_retries: int = 2
            for attempt in range(max_retries):
                # 获取视频详情
                response_data: dict[str, Any] | None = api.fetch_video_details(
                    douban_id=str(douban_id)
                )
                
                if response_data is None:
                    logger.error(f"获取详情失败 (douban_id: {douban_id})")
                    break
                
                response_code: int = response_data.get('code')
                
                if response_code == 0:
                    details: dict[str, Any] | None = response_data.get('data')
                    
                    if not details:
                        logger.warning(f"视频详情为空，跳过: {douban_id}")
                        break
                    
                    # 提取数据
                    video_list: list[str] = details.get('video_list', [])
                    cover: str = details.get('cover', '')
                    title: str = details.get('title', '')
                    
                    # 上传到OSS
                    try:
                        result: bool = oss_handler.process_single_video_sync(
                            douban_id=douban_id,
                            title=title,
                            video_list=video_list,
                            cover=cover
                        )
                        
                        if result:
                            logger.info(f"修复成功: {douban_id}")
                            break
                        else:
                            raise Exception("OSS同步失败")
                            
                    except Exception as e:
                        logger.error(f"OSS同步异常: {e}")
                        failed_synced_ids.append(douban_id)
                        break
                
                elif response_code == 402:
                    # Token过期，重新登录
                    logger.warning(f"Token已过期，尝试重新登录 (第 {attempt + 1} 次)")
                    new_token: str | None = api.login()
                    
                    if new_token:
                        state['api_token'] = new_token
                        save_state(data=state)
                        continue
                    else:
                        logger.error("重新登录失败")
                        failed_synced_ids.append(douban_id)
                        break
                else:
                    logger.error(f"未知API错误 (Code: {response_code})")
                    failed_synced_ids.append(douban_id)
                    break
            
            # 避免请求过快
            time.sleep(1)
        
        # 更新状态
        state['failed_synced_ids'] = failed_synced_ids
        save_state(data=state)
        
    finally:
        api.close()
        oss_handler.close()
        logger.info("=" * 80)
        logger.info("OSS数据修复脚本执行结束")
        logger.info("=" * 80)


def run_s3_fixer(config: Any) -> None:
    """
    执行S3数据修复任务
    
    重新上传之前失败的视频文件到S3。
    
    Args:
        config: 项目配置对象
        
    Example:
        >>> from core import load_config
        >>> config = load_config()
        >>> run_s3_fixer(config=config)
    """
    logger.info("=" * 80)
    logger.info("启动S3数据修复脚本")
    logger.info("=" * 80)
    
    state: dict[str, Any] = load_state()
    fix_ids: list[int] = state.get('failed_synced_ids', [])
    
    if not fix_ids:
        logger.info("没有需要修复的S3记录")
        return
    
    logger.info(f"需要修复的记录数: {len(fix_ids)}")
    
    # 初始化处理器
    api: ApiHandler = ApiHandler(config=config)
    s3_handler: S3Handler = S3Handler(config=config)
    
    # 设置Token
    token: str | None = state.get('api_token')
    if token:
        api.set_token(token=token)
    
    failed_synced_ids: list[int] = []
    
    try:
        for douban_id in fix_ids:
            logger.info(f"开始修复: {douban_id}")
            
            # 最多重试2次
            max_retries: int = 2
            for attempt in range(max_retries):
                # 获取视频详情
                response_data: dict[str, Any] | None = api.fetch_video_details(
                    douban_id=str(douban_id)
                )
                
                if response_data is None:
                    logger.error(f"获取详情失败 (douban_id: {douban_id})")
                    break
                
                response_code: int = response_data.get('code')
                
                if response_code == 0:
                    details: dict[str, Any] | None = response_data.get('data')
                    
                    if not details:
                        logger.warning(f"视频详情为空，跳过: {douban_id}")
                        break
                    
                    # 提取数据
                    video_list: list[str] = details.get('video_list', [])
                    cover: str = details.get('cover', '')
                    title: str = details.get('title', '')
                    
                    # 上传到S3
                    try:
                        result: bool = s3_handler.process_single_video_sync(
                            douban_id=douban_id,
                            title=title,
                            video_list=video_list,
                            cover=cover
                        )
                        
                        if result:
                            logger.info(f"修复成功: {douban_id}")
                            break
                        else:
                            raise Exception("S3同步失败")
                            
                    except Exception as e:
                        logger.error(f"S3同步异常: {e}")
                        failed_synced_ids.append(douban_id)
                        break
                
                elif response_code == 402:
                    # Token过期，重新登录
                    logger.warning(f"Token已过期，尝试重新登录 (第 {attempt + 1} 次)")
                    new_token: str | None = api.login()
                    
                    if new_token:
                        state['api_token'] = new_token
                        save_state(data=state)
                        continue
                    else:
                        logger.error("重新登录失败")
                        failed_synced_ids.append(douban_id)
                        break
                else:
                    logger.error(f"未知API错误 (Code: {response_code})")
                    failed_synced_ids.append(douban_id)
                    break
            
            # 避免请求过快
            time.sleep(1)
        
        # 更新状态
        state['failed_synced_ids'] = failed_synced_ids
        save_state(data=state)
        
    finally:
        api.close()
        s3_handler.close()
        logger.info("=" * 80)
        logger.info("S3数据修复脚本执行结束")
        logger.info("=" * 80)


def run_site_fixer(config: Any) -> None:
    """
    执行站点数据修复任务
    
    重新同步之前失败的视频数据到站点。
    
    Args:
        config: 项目配置对象
        
    Example:
        >>> from core import load_config
        >>> config = load_config()
        >>> run_site_fixer(config=config)
    """
    logger.info("=" * 80)
    logger.info("启动站点数据修复脚本")
    logger.info("=" * 80)
    
    state: dict[str, Any] = load_state()
    db: DatabaseHandler = DatabaseHandler(config=config)
    site_handler: SiteHandler = SiteHandler(config=config)
    fix_sites: dict[str, list[str]] = state.get('failed_site', {})
    failed_site: dict[str, set[str]] = {}
    
    if not fix_sites:
        logger.info("没有需要修复的站点同步记录")
        return
    
    try:
        # 遍历每个域名的失败记录
        for domain, failed_ids in fix_sites.items():
            if not failed_ids:
                continue
            
            logger.info(f"开始修复域名 {domain} 的 {len(failed_ids)} 个失败记录")
            
            # 从数据库查询视频数据
            site_videos: list[dict[str, Any]] = db.get_videos_by_ids(
                douban_ids=list(failed_ids)
            )
            
            if not site_videos:
                logger.warning(f"域名 {domain} 从数据库查询视频数据为空")
                failed_site[domain] = set(failed_ids)
                continue
            
            # 重新同步到站点
            try:
                failed_site_ids: dict[str, set[str]] = site_handler.sync_videos_to_site(
                    videos=site_videos,
                    domain=domain
                )
                
                if failed_site_ids:
                    for d, fids in failed_site_ids.items():
                        failed_site[d] = fids
                        
            except Exception as e:
                logger.error(f"站点修复失败: {e}")
                failed_site[domain] = set(failed_ids)
        
        # 更新状态
        state['failed_site'] = {k: list(v) for k, v in failed_site.items()}
        save_state(data=state)
        
    finally:
        db.close()
        site_handler.close()
        logger.info("=" * 80)
        logger.info("站点数据修复脚本执行结束")
        logger.info("=" * 80)


def run_site_clean(config: Any) -> None:
    """
    执行站点数据清理任务
    
    清理所有配置站点的数据。
    
    Args:
        config: 项目配置对象
        
    Example:
        >>> from core import load_config
        >>> config = load_config()
        >>> run_site_clean(config=config)
    """
    logger.info("=" * 80)
    logger.info("启动站点数据清理脚本")
    logger.info("=" * 80)
    
    site_handler: SiteHandler = SiteHandler(config=config)
    
    try:
        results: dict[str, bool] = site_handler.clean_to_site()
        
        # 输出结果
        for domain, success in results.items():
            status: str = "成功" if success else "失败"
            logger.info(f"{domain}: 清理{status}")
            
    finally:
        site_handler.close()
        logger.info("=" * 80)
        logger.info("站点数据清理脚本执行结束")
        logger.info("=" * 80)


# ============================================================================
# 命令行入口
# ============================================================================

def main() -> None:
    """
    程序主函数
    
    解析命令行参数并路由到相应的处理函数。
    
    Supported Commands:
        - scraper: 抓取API元数据
        - oss_fix: 修复OSS上传失败的数据
        - s3_fix: 修复S3上传失败的数据
        - site_fix: 修复站点同步失败的数据
        - site_clean: 清理站点数据
        
    Example:
        $ python main.py scraper
        $ python main.py oss_fix
        $ python main.py site_clean
    """
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="视频数据同步系统 - Python 3.11+ 企业级项目",
        epilog="Author: Qasim | Version: 2.0"
    )
    
    subparsers = parser.add_subparsers(
        dest='command',
        required=True,
        help='选择要执行的命令'
    )
    
    # 创建子命令
    subparsers.add_parser('scraper', help='抓取API元数据并存入数据库')
    subparsers.add_parser('oss_fix', help='修复OSS上传失败的数据')
    subparsers.add_parser('s3_fix', help='修复S3上传失败的数据')
    subparsers.add_parser('site_fix', help='修复站点同步失败的数据')
    subparsers.add_parser('site_clean', help='清理站点数据')
    
    # 解析参数
    args: argparse.Namespace = parser.parse_args()
    
    # 加载配置
    config: Any = load_config()
    
    # 路由到相应函数
    if args.command == 'scraper':
        run_scraper(config=config)
    elif args.command == 'oss_fix':
        run_oss_fixer(config=config)
    elif args.command == 's3_fix':
        run_s3_fixer(config=config)
    elif args.command == 'site_fix':
        run_site_fixer(config=config)
    elif args.command == 'site_clean':
        run_site_clean(config=config)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
