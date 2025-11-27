"""应用层 - 命令模块"""

from src.application.commands.base import BaseCommand, CommandContext
from src.application.commands.registry import CommandRegistry
from src.application.commands.s3_origin_check import S3OriginCheckCommand
from src.application.commands.s3_index_check import S3IndexCheckCommand
from src.application.commands.oss_origin_check import OSSOriginCheckCommand
from src.application.commands.oss_index_check import OSSIndexCheckCommand
from src.application.commands.scraper import ScraperCommand
from src.application.commands.s3_origin_fix import S3OriginFixCommand
from src.application.commands.oss_origin_fix import OSSOriginFixCommand
from src.application.commands.s3_index_fix import S3IndexFixCommand
from src.application.commands.oss_index_fix import OSSIndexFixCommand
from src.application.commands.s3_cover_check import S3CoverCheckCommand
from src.application.commands.oss_cover_check import OSSCoverCheckCommand
from src.application.commands.s3_cover_fix import S3CoverFixCommand
from src.application.commands.oss_cover_fix import OSSCoverFixCommand
from src.application.commands.site_fix import SiteFixCommand
from src.application.commands.site_clean import SiteCleanCommand

__all__ = [
    "BaseCommand",
    "CommandContext",
    "CommandRegistry",
    "S3OriginCheckCommand",
    "S3IndexCheckCommand",
    "OSSOriginCheckCommand",
    "OSSIndexCheckCommand",
    "ScraperCommand",
    "S3OriginFixCommand",
    "OSSOriginFixCommand",
    "S3IndexFixCommand",
    "OSSIndexFixCommand",
    "S3CoverCheckCommand",
    "OSSCoverCheckCommand",
    "S3CoverFixCommand",
    "OSSCoverFixCommand",
    "SiteFixCommand",
    "SiteCleanCommand",
]
