from collections.abc import Iterable
import typer

from cautils.utils import complete_env
from cautils.xog import Database, Format, SortDirection

OutputOpt = typer.Option(
    "-",
    "--output",
    "-o",
    help="Output file to FILENAME.",
    writable=True,
)

EnvOpt = typer.Option(
    None,
    "--env",
    "-e",
    help="Environment name.",
    autocompletion=complete_env,
)

DbOpt = typer.Option(
    Database.niku,
    "--db",
    "-d",
    show_choices=True,
    case_sensitive=False,
    help="Database ID.",
)

FormatOpt = typer.Option(
    Format.table,
    "--format",
    "-f",
    help="Output format",
    case_sensitive=False,
    show_choices=True,
)

LimitOpt = typer.Option(
    None,
    "--limit",
    "-n",
    help="Limit output to n lines.",
)

TimeoutOpt = typer.Option(
    120 * 60,
    "--timeout",
    "-t",
    help="XOG client timeout. 0 to disable it.",
)


EqOpt = typer.Option(
    None, "--equals", "-eq", metavar="COLUMN:VALUE", help="Strict equality filter."
)
LikeOpt = typer.Option(
    None,
    "--like",
    "-like",
    metavar="COLUMN:VALUE",
    help="Wildcard filter.",
)
LtOpt = typer.Option(
    None, "--from", "-lt", metavar="COLUMN:VALUE", help="Less than filter."
)
GtOpt = typer.Option(
    None, "--to", "-gt", metavar="COLUMN:VALUE", help="Greater than filter."
)


def _complete_sort_opt(incomplete: str) -> Iterable[str]:
    members = tuple(f":{member}" for member in SortDirection._member_names_)
    if incomplete.endswith(members):
        return []
    return [f"{incomplete}{member}" for member in members]


SortOpt = typer.Option(
    None,
    metavar=f"COLUMN:[{'|'.join(SortDirection._member_names_)}]",
    autocompletion=_complete_sort_opt,
    help="Sort on multiple columns.",
)
