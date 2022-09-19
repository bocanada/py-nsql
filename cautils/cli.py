from pathlib import Path
from typing import Optional
from rich.columns import Columns

from rich.panel import Panel
from rich.rule import Rule
import typer

from cautils import console
from cautils.utils import (
    complete_env,
    get_env_creds,
)
from cautils.credentials import app as creds
from cautils.queries import queries
from cautils.xog import XOG, Xml
from cautils import APP_NAME


app = typer.Typer(
    name=APP_NAME, pretty_exceptions_show_locals=False, no_args_is_help=True
)


app.add_typer(queries, name="query", help="N/SQL utils")
app.add_typer(creds, name="credentials", help="Manages credentials")


def print_header(env_url: str, input_file: str, output: str):
    panel = Columns(
        [
            Panel.fit(env_url, title="URL"),
            Panel.fit(Path(input_file).absolute().as_uri(), title="Input file"),
            Panel.fit(Path(output).absolute().as_uri(), title="Output file"),
        ],
        expand=True,
        align="center",
    )
    console.print(Rule("XOG"))
    console.print(panel, style="green")


def print_xml_preview(
    xml: Xml, limit: Optional[int] = None, subtitle: Optional[str] = None
):
    if not limit:
        return
    console.print(
        Panel.fit(
            xml.syntax(limit),
            title="Input preview",
            subtitle=subtitle,
            style="green",
        )
    )


@app.command(short_help="Run a XOG")
def xog(
    input_file: typer.FileText = typer.Argument(
        ..., readable=True, dir_okay=False, exists=True
    ),
    env: str = typer.Option(
        None,
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
        help="XOG client timeout. 0 to disable it.",
    ),
    preview_lines: Optional[int] = typer.Option(
        30,
        "--preview-lines",
        "-n",
        help="Preview n lines of the input file.",
    ),
):
    env_url, username, passwd = get_env_creds(env)
    print_header(env_url, input_file.name, output.name)

    with console.status("Reading XOG..."), input_file as f:
        xml = Xml.read(f)

    print_xml_preview(xml, preview_lines, input_file.name)

    action = (
        header[0].get("action", "?") if (header := xml.xpath("//Header")) else "read"
    )

    with console.status(f"Running {action} XOG..."), XOG(
        env_url, username, passwd, timeout=timeout
    ) as client:
        resp = client.send(xml)
    with console.status("Writing output file..."):
        written = resp.write(output)
    console.log(f"Wrote {written} bytes")


if __name__ == "__main__":
    app(prog_name=APP_NAME)
