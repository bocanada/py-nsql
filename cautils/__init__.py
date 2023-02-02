import readline

from rich.console import Console

__version__ = "0.2.1"

APP_NAME = "cautils"

console = Console()

err_console = Console(stderr=True, log_path=True)
