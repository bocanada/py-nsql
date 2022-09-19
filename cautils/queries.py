from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Optional, cast

import typer

from cautils import console, parser
from cautils.utils import ask, complete_env, get_env_creds
from cautils.xog import ContentPackageException, Databases, Format, QueryID, Writer, XOG

queries = typer.Typer()

runner = typer.Typer(no_args_is_help=True)
queries.add_typer(runner, name="run")

NotRequiredAutoENV = typer.Option(
    None,
    "--env",
    "-e",
    help="Environment name.",
    autocompletion=complete_env,
)

DbOption = typer.Option(
    Databases.niku,
    "--db",
    "-d",
    show_choices=True,
    case_sensitive=False,
    help="Database ID.",
)


@queries.command()
def edit(
    query_id: str,
    env: str = NotRequiredAutoENV,
    db: Databases = DbOption,
    once: bool = typer.Option(True, help="Only run once."),
    format: Format = typer.Option(
        Format.table, "--format", "-f", case_sensitive=False, show_choices=True
    ),
    run: bool = typer.Option(True, help="Run the query."),
    limit: int = typer.Option(None, "--limit", "-n", help="Limit the number of rows."),
    output: typer.FileTextWrite = typer.Option("-", hidden=True),
    timeout: int = typer.Option(35),
):
    """
    Edit a query interactively on the environment.
    It will run until you make no changes on the NSQL.
    """
    env_url, username, passwd = get_env_creds(env)
    query_id = QueryID(query_id)

    xog = XOG(env_url, username, passwd, timeout)
    with console.status("Getting query..."):
        nsql = xog.query_get(query_id, db)

    w = Writer(output, format, console)

    last_qry = ""

    assert nsql.text is not None, f"Couldn't get query with id {query_id}"

    with NamedTemporaryFile("w+", suffix=".sql", prefix=query_id) as f:
        f.write(nsql.text)
        # If we don't do this, the file is empty
        f.flush()
        while True:
            if (code := typer.launch(f.name, wait=True)) != 0:
                raise Exception(f"Status code: {code}")
            # Go back to the start of the file, and dump its contents
            f.seek(0)
            if last_qry == (last_qry := f.read()):
                console.log("No changes were made. Exiting...")
                raise typer.Exit(0)
            try:
                with console.status("Uploading query..."):
                    xog.upload_query(last_qry, db, query_id)
            except ContentPackageException:
                console.print_exception()
                ask()
                continue
            if not run:
                console.log(f"Uploaded query {query_id}. Exiting..")
                break

            result = xog.run_query(query_id)[:limit]
            with console.pager(styles=True, links=True):
                w.write(query_id, result)

            if once:
                break

    console.log(f"Uploaded query {query_id}. Exiting..")


@runner.command(name="id")
def run_with_id(
    query_id: str = typer.Argument(..., help="NSQL Query ID"),
    env: Optional[str] = NotRequiredAutoENV,
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

    query_id = QueryID(query_id)
    with xog_c:
        run_and_write(xog_c, query_id, output, format, limit)


@runner.command()
def file(
    nsql_path: typer.FileText = typer.Argument(
        ..., help="SQL/NSQL code file path.", exists=True, readable=True
    ),
    db: Databases = DbOption,
    to_nsql: bool = typer.Option(
        False,
        "--to-nsql",
        "-t",
        help="Transpile to NSQL before running it. True if FILENAME ext is .sql",
    ),
    env: Optional[str] = NotRequiredAutoENV,
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
    Run a query from FILENAME on env.
    """
    env_url, username, passwd = get_env_creds(env)

    to_nsql = to_nsql or Path(nsql_path.name).match("*.sql")

    if nsql_path.isatty():
        console.print("Copy & paste your NSQL")

    xog = XOG(env_url, username, passwd)
    with console.status("Reading file..."):
        nsql = parser.sql_to_nsql(nsql_path) if to_nsql else nsql_path.read()

    with console.status("Uploading query..."):
        query_id = xog.upload_query(nsql, db)

    run_and_write(xog, query_id, output, format, limit)
    console.log("Done!")


@queries.command(
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
    with console.status("Transpiling..."), sql.open("r") as f:
        output.write(parser.sql_to_nsql(f))


def run_and_write(
    xog: XOG,
    query_id: QueryID,
    output: typer.FileTextWrite,
    format: Format,
    limit: Optional[int],
):
    query_id = QueryID(query_id)

    with console.status("Running query..."):
        result = xog.run_query(query_id)[:limit]

    with console.status(f"Writing {len(result)} lines to {output.name}...\n"):
        Writer(output, format, console).write(query_id, result)
