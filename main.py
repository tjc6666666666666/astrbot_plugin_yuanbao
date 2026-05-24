"""
Yuanbao adapter — AstrBot plugin entry point.

Provides a Star subclass so that AstrBot's plugin loader can discover
and initialise the plugin.  Importing the platform adapter inside
__init__ triggers the @register_platform_adapter decorator, which
registers YuanbaoPlatformAdapter in the platform registry.
"""

from astrbot.api.star import Context, Star


class YuanbaoPlugin(Star):
    """Zero-config plugin that activates the Yuanbao platform adapter."""

    def __init__(self, context: Context) -> None:
        super().__init__(context)
        # The import triggers @register_platform_adapter
        from .yuanbao_platform_adapter import YuanbaoPlatformAdapter  # noqa: F401
