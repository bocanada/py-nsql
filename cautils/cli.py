from pathlib import Path

from rich.prompt import Confirm
from rich.columns import Columns
from rich.panel import Panel
import typer

from cautils import console
from cautils.utils import (
    complete_env,
    create_env,
    get_config_path,
    get_env_creds,
    get_envs,
    save_envs,
    update_credentials,
)
from cautils.queries import queries
from cautils.xog import XOG, Format, Writer, Xml
from cautils import APP_NAME


app = typer.Typer(name=APP_NAME, pretty_exceptions_show_locals=False)

creds = typer.Typer()

creds_update = typer.Typer()

app.add_typer(queries, name="query", help="N/SQL utils")
app.add_typer(creds, name="credentials", help="Manages credentials")
creds.add_typer(creds_update, name="update", help="Updates credentials")


RequiredAutoCompleteENV = typer.Option(
    ...,
    "--env",
    "-e",
    help="Environment name.",
    autocompletion=complete_env,
)


def creds_update_factory():
    for cred in ["url", "username", "password"]:

        @creds_update.command(name=cred, help=f"Updates {cred} on an ENV")
        def _(value: str, env: str = RequiredAutoCompleteENV):
            update_credentials(env, **{cred: value})
            console.log(f"Updated env {env} 🚀")


creds_update_factory()


@creds.command()
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


@creds.command()
def list():
    """
    Lists all environments on the configuration file.
    """

    path = get_config_path()
    envs = get_envs(path)
    console.log(f"Showing {len(envs)} saved envs:")
    console.print(
        Columns([Panel(v["url"], title=env, expand=True) for env, v in envs.items()]),
        justify="center",
    )


@app.command(short_help="Run a XOG")
def xog(
    input_file: Path,
    env: str = typer.Option(
        ...,
        "--env",
        "-e",
        help="Environment name.",
        autocompletion=complete_env,
    ),
    output: typer.FileTextWrite = typer.Option(
        "out.xml",
        "--output",
        "-o",
        help="Output file.",
    ),
    timeout: float = typer.Option(
        120 * 60,
        "--timeout",
        "-t",
        help="XOG client timeout. Pass 0 to disable it.",
    ),
):
    env_url, username, passwd = get_env_creds(env)
    xog = XOG(env_url, username, passwd, timeout=timeout)
    console.log(f"Environment: {env_url}")
    console.log(f"Input file:  {Path(input_file).absolute().as_uri()}")
    console.log(f"Output file: {Path(output.name).absolute().as_uri()}")

    with console.status("Reading XOG..."), input_file.open("r") as f:
        xml = Xml.read(f)
    action = (
        header.get("action", "read")
        if (header := xml.find("Header")) is not None
        else "read"
    )
    with console.status(f"Running {action} XOG..."), xog as client:
        resp = client.send(xml)
    with console.status("Writing output file..."):
        written = Writer(output, Format.json, console).write_xml(resp)
    console.log(f"Wrote {written} bytes")


if __name__ == "__main__":
    app(prog_name=APP_NAME)
