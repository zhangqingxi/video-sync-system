"""
应用层 - Site Clean命令

清理站点数据。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

import requests
from typing import Any
from src.application.commands.base import BaseCommand


class SiteCleanCommand(BaseCommand):
    """站点清理命令"""

    @property
    def name(self) -> str:
        return "site_clean"

    @property
    def description(self) -> str:
        return "清理站点冗余数据"

    def execute(self) -> int:
        """执行清理"""
        try:
            self.logger.info("=" * 60)
            self.logger.info("开始站点数据清理")
            self.logger.info("=" * 60)

            # 获取站点配置
            site_config: dict[str, Any] = self.config.raw_config.get("site", {})
            domains: list[str] = site_config.get("domains", [])
            api_token: str = site_config.get("api_token", "")
            clean_endpoint: str = site_config.get("endpoints", {}).get(
                "clean", "/api/clean"
            )

            if not domains:
                self.logger.error("未配置站点域名")
                return 1

            # 对每个站点执行清理
            for domain in domains:
                self.logger.info(f"清理站点: {domain}")

                try:
                    url: str = f"{domain}{clean_endpoint}"
                    headers: dict[str, str] = {"Authorization": f"Bearer {api_token}"}

                    response: requests.Response = requests.post(
                        url=url, headers=headers, timeout=60
                    )

                    response.raise_for_status()

                    if response.status_code == 200:
                        result: dict[str, Any] = response.json()
                        self.logger.info(f"清理成功: {domain}, 结果={result}")
                    else:
                        self.logger.warning(
                            f"清理失败: {domain}, Status={response.status_code}"
                        )

                except Exception as e:
                    self.logger.error(f"清理站点失败: {domain}, Error={e}")

            self.logger.info("=" * 60)
            self.logger.info("站点清理完成")
            self.logger.info("=" * 60)

            return 0

        except Exception as e:
            self.logger.error(f"命令执行失败: {e}", exc_info=True)
            return 1
