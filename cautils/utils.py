from __future__ import annotations
import json
from pathlib import Path
from typing import Iterable, Optional, TypeAlias

from rich.prompt import Confirm, Prompt
import typer

from cautils import APP_NAME, console


Env: TypeAlias = dict[str, str]
Envs: TypeAlias = dict[str, dict[str, str]]

Creds: TypeAlias = tuple[str, str, str]


def get_config_path() -> Path:
    """
    Returns the full path of the configuration path.
    """
    app_dir = typer.get_app_dir(APP_NAME)
    config_path = Path(app_dir) / "config.json"
    # Create folder and file if it doesn't exist
    config_path.parent.mkdir(exist_ok=True)
    config_path.touch(exist_ok=True)
    return config_path


def get_envs(config_path: Path):
    """
    Gets all envs from the configuration file.
    """
    with config_path.open("r") as cfg:
        try:
            envs: dict[str, dict[str, str]] = json.load(cfg)
        except json.JSONDecodeError:
            envs = {}
    return envs


def ask_for_creds() -> Creds:
    """
    Asks the user for credentials from STDIN.
    """
    (env_url, username), passwd = [
        Prompt.ask(prompt, console=console)
        for prompt in ["Environment URL", "Username"]
    ], Prompt.ask("Password", password=True, console=console)
    return env_url, username, passwd


def update_credentials(name: str, **kwargs: str) -> Envs:
    path = get_config_path()
    envs = get_envs(path)
    envs[name] |= kwargs
    save_envs(envs, path)
    return envs


def save_envs(envs: Envs, config_path: Path):
    """
    Saves `envs` to `config_path`.
    """
    with config_path.open("w") as f:
        json.dump(envs, f, indent=4)


def create_env(envs: Envs, name: str, env_url: str, username: str, passwd: str) -> Envs:
    """
    Creates a new env named `name` on `envs`.
    """
    envs[name] = {"url": env_url, "username": username, "password": passwd}
    return envs


def get_env_creds(env: Optional[str]) -> Creds:
    config_path = get_config_path()
    envs = get_envs(config_path)

    if not env:
        env_url, username, passwd = ask_for_creds()

        if Confirm.ask(
            "Do you want to save this to your config file?", console=console
        ):
            name = Prompt.ask("Name this env")
            envs = create_env(envs, name, env_url, username, passwd)
            save_envs(envs, config_path)
    else:
        try:
            data = envs[env]
        except KeyError:
            raise typer.BadParameter(f"{env} is not one of: {','.join(envs.keys())}")
        env_url, username, passwd = data["url"], data["username"], data["password"]
    return env_url, username, passwd


def complete_env() -> Iterable[str]:
    config_path = get_config_path()
    envs = get_envs(config_path)
    return envs.keys()


def ask():
    """
    Asks if the user wants to continue.
    If not, raise typer.Exit
    """
    if not Confirm.ask("Continue?", console=console, default=True):
        console.log("Bye!")
        raise typer.Abort()
