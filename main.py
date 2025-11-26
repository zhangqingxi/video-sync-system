"""
主入口文件

提供命令行界面，执行各种视频同步任务。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

import sys
import urllib3
from pathlib import Path
from src.core.config import ConfigManager
from src.core.container import DIContainer
from src.core.logger import LoggerManager
from src.application.commands.base import CommandContext, BaseCommand
from src.application.commands.registry import CommandRegistry
from src.core.exceptions import ConfigurationError, ValidationError

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def setup_environment() -> tuple[ConfigManager, DIContainer, LoggerManager]:
    """
    设置运行环境

    Returns:
        tuple: (config, container, logger_manager)
    """
    # 加载配置
    config_file: Path = Path("config/config.yaml")
    if not config_file.exists():
        raise ConfigurationError("配置文件不存在: config/config.yaml")
    
    config: ConfigManager = ConfigManager(config_path=config_file)
    
    # 创建日志管理器
    log_dir: Path = Path("logs")
    logger_manager: LoggerManager = LoggerManager(base_dir=log_dir)
    
    # 创建依赖注入容器
    container: DIContainer = DIContainer()
    
    # 注册核心服务
    from src.core.state import StateManager
    from src.infrastructure.database import DatabasePool, VideoRepositoryImpl
    
    state_file: Path = Path("state.json")
    container.register(
        interface=StateManager,
        factory=lambda: StateManager(state_file=state_file),
        singleton=True
    )
    
    container.register(
        interface=DatabasePool,
        factory=lambda: DatabasePool(config=config.database, logger=logger_manager.get_logger(command_name="system")),
        singleton=True
    )
    
    container.register(
        interface=VideoRepositoryImpl,
        factory=lambda: VideoRepositoryImpl(
            db_pool=container.resolve(interface=DatabasePool),
            config=config.database,
            logger=logger_manager.get_logger(command_name="system")
        ),
        singleton=False
    )
    
    # 注册存储适配器
    from src.infrastructure.storage import OSSAdapter
    container.register(
        interface=OSSAdapter,
        factory=lambda: OSSAdapter(
            config=config.oss_storage,
            constants=config.constants,
            logger=logger_manager.get_logger(command_name="system")
        ),
        singleton=False
    )
    
    return config, container, logger_manager


def register_commands(registry: CommandRegistry) -> None:
    """
    注册所有命令
    
    Args:
        registry: 命令注册器
    """
    # 导入所有命令
    from src.application.commands.scraper import ScraperCommand
    from src.application.commands.oss_origin_check import OSSOriginCheckCommand
    from src.application.commands.oss_index_check import OSSIndexCheckCommand
    from src.application.commands.oss_cover_check import OSSCoverCheckCommand
    from src.application.commands.s3_origin_check import S3OriginCheckCommand
    from src.application.commands.s3_index_check import S3IndexCheckCommand
    from src.application.commands.s3_cover_check import S3CoverCheckCommand
    from src.application.commands.oss_origin_fix import OSSOriginFixCommand
    from src.application.commands.oss_index_fix import OSSIndexFixCommand
    from src.application.commands.oss_cover_fix import OSSCoverFixCommand
    from src.application.commands.s3_origin_fix import S3OriginFixCommand
    from src.application.commands.s3_index_fix import S3IndexFixCommand
    from src.application.commands.s3_cover_fix import S3CoverFixCommand
    from src.application.commands.site_fix import SiteFixCommand
    from src.application.commands.site_clean import SiteCleanCommand
    from src.application.commands.video_tag_check import VideoTagCheckCommand
    from src.application.commands.video_tag_fix import VideoTagFixCommand
    
    # 注册命令
    registry.register(ScraperCommand)
    registry.register(OSSOriginCheckCommand)
    registry.register(OSSIndexCheckCommand)
    registry.register(OSSCoverCheckCommand)
    registry.register(S3OriginCheckCommand)
    registry.register(S3IndexCheckCommand)
    registry.register(S3CoverCheckCommand)
    registry.register(OSSOriginFixCommand)
    registry.register(OSSIndexFixCommand)
    registry.register(OSSCoverFixCommand)
    registry.register(S3OriginFixCommand)
    registry.register(S3IndexFixCommand)
    registry.register(S3CoverFixCommand)
    registry.register(SiteFixCommand)
    registry.register(SiteCleanCommand)
    registry.register(VideoTagCheckCommand)
    registry.register(VideoTagFixCommand)


def main() -> int:
    """主函数"""
    if len(sys.argv) < 2:
        print("用法: python main.py <command>")
        print("使用 'python run.py' 查看可用命令列表")
        return 1
    
    command_name: str = sys.argv[1]
    
    try:
        # 设置环境
        config, container, logger_manager = setup_environment()
        
        # 创建命令上下文
        context: CommandContext = CommandContext(
            container=container,
            config=config,
            logger_manager=logger_manager
        )
        
        # 创建并注册命令
        registry: CommandRegistry = CommandRegistry()
        register_commands(registry=registry)
        
        # 获取并执行命令
        command: BaseCommand = registry.get_command(name=command_name, context=context)
        exit_code: int = command.execute()
        
        # 清理资源
        command.cleanup()
        
        return exit_code
        
    except ValidationError as e:
        print(f"❌ 验证错误: {e}")
        return 1
    except ConfigurationError as e:
        print(f"❌ 配置错误: {e}")
        return 1
    except KeyboardInterrupt:
        print("\n\n⚠️  用户中断")
        return 0
    except Exception as e:
        print(f"❌ 未知错误: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
