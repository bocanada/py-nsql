from pathlib import Path
from typing import Optional, cast

from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Confirm
import typer

from nsql import console, parser
from nsql.utils import (
    complete_env,
    create_env,
    get_config_path,
    get_env_creds,
    get_envs,
    open_task,
    save_envs,
    track,
    update_credentials,
)
from nsql.xog import Databases, Format, QueryID, Writer, XOG, parse_xml


app = typer.Typer(name="nsql", pretty_exceptions_show_locals=False)

runner = typer.Typer()

creds = typer.Typer()

creds_update = typer.Typer()

creds.add_typer(creds_update, name="update", help="Updates credentials")
app.add_typer(runner, name="run", help="Runs NSQL")
app.add_typer(creds, name="credentials", help="Manages credentials")


RequiredAutoCompleteENV = typer.Option(
    ...,
    "--env",
    "-e",
    help="Environment name.",
    autocompletion=complete_env,
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
    console.log(f"Input file:  {input_file}")
    console.log(f"Output file: {output.name}")
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
        transient=True,
    ) as p:
        with p.open(input_file, "r", description="Reading XOG...") as f:
            xml = parse_xml(f)
        with track(p, xog, "Running XOG...") as client:
            resp = client.send(xml)
        with open_task(p, "Writing output file..."):
            written = Writer(output, Format.json, console).write_xml(resp)
    console.log(f"Wrote {written} bytes")


@app.command(
    short_help="Transpiles SQL to NSQL.",
)
def transpile(
    sql: Path = typer.Argument(..., exists=True, readable=True),
    output: typer.FileTextWrite = typer.Option(
        "-", "--output", "-o", help="Save NSQL to FILENAME.", writable=True
    ),
):
    """
    Converts SQL to NSQL.\n
    Some limitations apply:\n
        - The SQL MUST have a WHERE clause.\n
        - Spaces between the OPENPAREN and OVER on window functions are not allowed.\n
        - CTEs are prohibited. NSQL doesn't permit any kind of code before the SELECT keyword.\n
    """
    with sql.open("r") as f:
        output.write(parser.sql_to_nsql(f))


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


@runner.command(name="id")
def run_with_id(
    query_id: str = typer.Argument(..., help="NSQL Query ID"),
    env: Optional[str] = typer.Option(
        None,
        "--env",
        "-e",
        help="Environment name.",
        autocompletion=complete_env,
    ),
    output: typer.FileTextWrite = typer.Option(
        "-",
        "--output",
        "-o",
        help="Save output to FILENAME.",
        writable=True,
    ),
    limit: Optional[int] = typer.Option(
        None,
        "--limit",
        "-n",
        help="Limit output to n lines.",
        writable=True,
    ),
    format: Format = typer.Option(
        Format.table, "--format", "-f", case_sensitive=False, show_choices=True
    ),
    xog: str = typer.Option(None, hidden=True),
):
    """
    Run a query on the specified ENV and write it to STDOUT or OUTPUT.
    """
    env_url, username, passwd = get_env_creds(env)
    xog_c = cast(XOG, xog)
    xog_c = xog_c or XOG(env_url, username, passwd)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
        transient=True,
    ) as p:
        with track(p, xog_c, "Running query...") as client:
            query_id = QueryID(query_id)
            result = client.run_query(query_id)[:limit]
        with open_task(p, f"Writing {len(result)} lines to {output.name}..."):
            Writer(output, format, console).write(query_id, result)


@runner.command()
def file(
    nsql_path: typer.FileText = typer.Argument(
        ..., help="SQL/NSQL code file path.", exists=True, readable=True
    ),
    db: Databases = typer.Option(
        Databases.niku,
        "--db",
        "-d",
        show_choices=True,
        case_sensitive=False,
        help="Database ID in which the query is supposed to run on.",
    ),
    to_nsql: bool = typer.Option(
        False,
        "--to-nsql",
        "-t",
        help="Transpile to NSQL before running it. True if FILENAME ext is .sql",
    ),
    env: Optional[str] = typer.Option(
        None,
        "--env",
        "-e",
        help="Environment name.",
        autocompletion=complete_env,
    ),
    output: typer.FileTextWrite = typer.Option(
        "-",
        "--output",
        "-o",
        help="Save output to FILENAME.",
        writable=True,
    ),
    format: Format = typer.Option(
        Format.table, "--format", "-f", case_sensitive=False, show_choices=True
    ),
    limit: Optional[int] = typer.Option(
        None,
        "--limit",
        "-n",
        help="Limit output to n lines.",
        writable=True,
    ),
):
    """
    XOG and run a file on ENV and write it to STDOUT or OUTPUT.
    """

    env_url, username, passwd = get_env_creds(env)

    to_nsql = to_nsql or Path(nsql_path.name).match("*.sql")

    if nsql_path.isatty():
        console.print("Copy & paste your NSQL")

    xog = XOG(env_url, username, passwd)
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
        console=console,
    ) as p, xog:
        with open_task(p, description="Reading file..."):
            nsql = parser.sql_to_nsql(nsql_path) if to_nsql else nsql_path.read()

        with open_task(p, description="Uploading query..."):
            query_id = xog.upload_query(nsql, db)
        p.stop()
        run_with_id(query_id, env, output, limit, format, xog=cast(str, xog))
    console.log("Done!")


if __name__ == "__main__":
    app(prog_name="nsql")
