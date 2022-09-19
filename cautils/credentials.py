from enum import Enum

from rich.panel import Panel
from rich.columns import Columns
from rich.prompt import Confirm
import typer

from cautils import console
from cautils.utils import (
    complete_env,
    create_env,
    get_config_path,
    get_envs,
    save_envs,
    update_credentials,
)


class Credentials(str, Enum):
    username = "username"
    password = "password"
    email = "email"


app = typer.Typer()


@app.command(help="Updates credentials")
def update(
    cred: Credentials = typer.Argument(..., show_choices=True, case_sensitive=False),
    value: str = typer.Argument(...),
    env: str = typer.Option(
        ...,
        "--env",
        "-e",
        help="Environment name.",
        prompt=True,
        autocompletion=complete_env,
    ),
):
    update_credentials(env, **{cred: value})
    console.log(f"Updated env {env} 🚀")


@app.command()
def new(name: str, env_url: str, username: str, password: str):
    """
    Adds a new environment to the application's config file.
    """
    path = get_config_path()
    envs = get_envs(path)
    if name in envs:
        if not Confirm.ask(
            "This will replace an existing env. Are you sure you want to continue?",
            default=True,
        ):
            raise typer.Abort()
    create_env(envs, name, env_url, username, password)
    save_envs(envs, path)
    console.log(f"Saved env {name} -> {env_url}. 🚀")


@app.command(name="list")
def list_envs():
    """
    Lists all environments on the configuration file.
    """

    path = get_config_path()
    envs = get_envs(path)
    console.log(f"Showing {len(envs)} saved envs:")
    console.print(
        Columns(
            [
                Panel(v["url"], style="green", title=env, expand=True)
                for env, v in envs.items()
            ],
            expand=True,
        ),
        justify="center",
    )
