from collections.abc import Iterable
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Optional

import typer

from cautils import console, err_console, opts, parser
from cautils.utils import ask, get_env_creds
from cautils.xog import (
    ContentPackageException,
    Database,
    Filter,
    FilterType,
    Format,
    QUERY_CODE,
    QueryID,
    SortColumn,
    SortDirection,
    Writer,
    XOG,
)

queries = typer.Typer(no_args_is_help=True)

runner = typer.Typer(no_args_is_help=True)
queries.add_typer(runner, name="run", no_args_is_help=True)


@queries.command()
def edit(
    query_id: str,
    env: str = opts.EnvOpt,
    db: Database = opts.DbOpt,
    format: Format = opts.FormatOpt,
    run: bool = typer.Option(True, help="Run the query."),
    limit: int = opts.LimitOpt,
    timeout: int = opts.TimeoutOpt,
    output: typer.FileTextWrite = typer.Option("-", hidden=True),
) -> None:
    """
    Edit a query interactively on the environment.
    It will run until you make no changes on the query.
    """
    env_url, username, passwd = get_env_creds(env)
    query_id = QueryID(query_id)

    xog = XOG(env_url, username, passwd, timeout)
    with err_console.status("Getting query..."):
        nsql = xog.query_get(query_id, db)

    w = Writer(output, format, console)

    last_qry = ""

    with NamedTemporaryFile("w+", suffix=".sql", prefix=query_id) as f:
        f.write(nsql.text)
        # If we don't do this, the file will be empty
        f.flush()
        while True:
            if (code := typer.launch(f.name, wait=True)) != 0:
                raise Exception(f"Status code: {code}")
            # Go back to the start of the file, and dump its contents
            f.seek(0)
            if last_qry == (last_qry := f.read()):
                err_console.log("No changes were made. Exiting...")
                raise typer.Exit(0)
            try:
                with err_console.status("Uploading query..."):
                    xog.upload_query(last_qry, db, query_id)
            except ContentPackageException:
                err_console.print_exception()
                ask()
                continue

            if not run:
                continue

            result = xog.run_query(query_id, [], [], limit)
            with err_console.pager(styles=True, links=True):
                w.write(query_id, result)

    err_console.log(f"Uploaded query {query_id}. Exiting..")


@runner.command(name="id")
def run_with_id(
    query_id: str = typer.Argument(..., help="NSQL Query ID"),
    env: Optional[str] = opts.EnvOpt,
    output: typer.FileTextWrite = opts.OutputOpt,
    limit: Optional[int] = opts.LimitOpt,
    format: Format = opts.FormatOpt,
    # Filters
    eq: list[str] = opts.EqOpt,
    like: list[str] = opts.LikeOpt,
    lt: list[str] = opts.LtOpt,
    gt: list[str] = opts.GtOpt,
    # Sorting cols
    sort: list[str] = opts.SortOpt,
):
    """
    Run a query on the specified ENV and write it to STDOUT or OUTPUT.
    """
    env_url, username, passwd = get_env_creds(env)
    query_id = QueryID(query_id)
    filters = parse_filters(eq, like, lt, gt)
    sort_cols = parse_sort(sort)

    with XOG(env_url, username, passwd) as xog:
        run_and_write(xog, query_id, output, format, limit, filters, sort_cols)


@runner.command()
def file(
    nsql_path: typer.FileText = typer.Argument(
        ..., help="SQL/NSQL code file path.", exists=True, readable=True
    ),
    db: Database = opts.DbOpt,
    to_nsql: bool = typer.Option(
        False,
        "--to-nsql",
        "-t",
        help="Transpile to NSQL before running it.\nDefault: True if FILENAME ext is .sql, else False.",
    ),
    env: Optional[str] = opts.EnvOpt,
    output: typer.FileTextWrite = opts.OutputOpt,
    format: Format = opts.FormatOpt,
    limit: Optional[int] = opts.LimitOpt,
    query_id: str = typer.Option(QUERY_CODE, help="Save query with a specific id."),
    # Filters
    eq: list[str] = opts.EqOpt,
    like: list[str] = opts.LikeOpt,
    lt: list[str] = opts.LtOpt,
    gt: list[str] = opts.GtOpt,
    # Sorting cols
    sort: list[str] = opts.SortOpt,
):
    """
    Run a query from FILENAME on env.
    """
    env_url, username, passwd = get_env_creds(env)

    to_nsql = to_nsql or Path(nsql_path.name).match("*.sql")

    if nsql_path.isatty():
        err_console.log("Reading from stdin...")

    xog = XOG(env_url, username, passwd)
    with err_console.status("Reading file..."):
        nsql = parser.sql_to_nsql(nsql_path) if to_nsql else nsql_path.read()

    with err_console.status("Uploading query..."):
        query_id = xog.upload_query(nsql, db, QueryID(query_id))

    filters = parse_filters(eq, like, lt, gt)
    sort_cols = parse_sort(sort)

    run_and_write(xog, query_id, output, format, limit, filters, sort_cols)
    err_console.log("Done!")


@queries.command(
    short_help="Transpiles SQL to NSQL.",
)
def transpile(
    sql: Path = typer.Argument(..., exists=True, readable=True),
    output: typer.FileTextWrite = opts.OutputOpt,
):
    """
    Converts SQL to NSQL.\n
    Some limitations apply:\n
        - The SQL MUST have a WHERE clause.\n
        - Spaces between the OPENPAREN and OVER on window functions are not allowed.\n
        - CTEs are prohibited. NSQL doesn't permit any kind of code before the SELECT keyword.\n
    """
    with err_console.status("Transpiling..."), sql.open("r") as f:
        output.write(parser.sql_to_nsql(f))


def run_and_write(
    xog: XOG,
    query_id: QueryID,
    output: typer.FileTextWrite,
    format: Format,
    limit: Optional[int],
    filters: Iterable[Filter] = [],
    sort: Iterable[SortColumn] = [],
):
    query_id = QueryID(query_id)

    with err_console.status("Running query..."):
        result = xog.run_query(
            query_id,
            filters,
            sort,
            limit,
        )

    with err_console.status(f"Writing {len(result)} lines to {output.name}...\n"):
        Writer(output, format, console).write(query_id, result)


def parse_sort(sort: Iterable[str]):
    return [SortColumn.from_colon_separated_item(col) for col in sort]


def parse_filters(eq: list[str], like: list[str], lt: list[str], gt: list[str]):
    from itertools import chain

    return chain.from_iterable(
        [
            Filter.from_colon_separated_items(FilterType.eq, eq),
            Filter.from_colon_separated_items(FilterType.like, like),
            Filter.from_colon_separated_items(FilterType.lt, lt),
            Filter.from_colon_separated_items(FilterType.gt, gt),
        ]
    )
