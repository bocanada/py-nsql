from __future__ import annotations
from collections.abc import Callable
from csv import DictWriter
from dataclasses import dataclass, field
from enum import Enum
from io import StringIO
import json
from pathlib import Path
from typing import Any, Final, NewType, Optional, TextIO, TypeAlias

from httpx import Client
from lxml import etree
from rich.console import Console
from rich.table import Table
from typer import FileTextWrite


NS = {
    "xog": "http://www.niku.com/xog",
    "soap": "http://schemas.xmlsoap.org/soap/envelope/",
}

QueryID = NewType("QueryID", str)

QUERY_CODE: Final[QueryID] = QueryID("query.runner")

QueryResult: TypeAlias = list[dict[str, Any]]
CSV: TypeAlias = str


class InvalidLoginError(Exception):
    ...


class QueryRunnerError(Exception):
    ...


class EmptyQueryResultError(Exception):
    ...


class ContentPackageException(Exception):
    ...


class HTTPError(Exception):
    ...


class XMLError(Exception):
    ...


class Databases(str, Enum):
    dwh = "Datawarehouse"
    niku = "Niku"


class Format(str, Enum):
    json = "json"
    csv = "csv"
    table = "table"


@dataclass
class XOG:
    base_url: str
    username: str
    password: str = field(repr=False)
    timeout: float = field(default=15)

    session_id: str = field(init=False, repr=False)
    c: Client = field(init=False, repr=False)

    def __post_init__(self):
        self.c = Client(base_url=self.base_url, timeout=self.timeout)
        self.login()

    def login(self):
        r = self.c.post(
            "niku/xog",
            headers={"Content-Type": "text/xml; charset=utf-8"},
            content=etree.tostring(
                create_login_envelope(self.username, self.password),
                xml_declaration=True,
            ),
        )
        tree = etree.fromstring(r.text)
        exc = tree.xpath("//Exception/text()")
        if exc:
            raise InvalidLoginError(str(exc[0]))  # type: ignore
        self.session_id = str(tree.xpath("//xog:SessionID/text()", namespaces=NS)[0])  # type: ignore
        return self.session_id

    def logout(self):
        return self.c.post(
            "niku/xog",
            headers={"Content-Type": "text/xml; charset=utf-8"},
            content=etree.tostring(
                create_logout_envelope(self.session_id),
                xml_declaration=True,
            ),
        )

    def wsdl(self, where: str, path: str) -> etree._Element:
        r = self.c.get(f"niku/wsdl/{where}/{path}")
        return etree.fromstring(r.text)

    def query_types(self, query_id: QueryID):
        tree = self.wsdl("Query", query_id)
        p = tree.xpath(f"//*[@name='{query_id}']/*")
        print(p)

    def send(self, body: etree._Element):
        r = self.c.post(
            "niku/xog",
            headers={"Content-Type": "text/xml; charset=utf-8"},
            content=etree.tostring(
                create_session_id_envelope(self.session_id, body), xml_declaration=True
            ),
        )
        if r.is_error:
            raise HTTPError(r.text)
        try:
            tree = etree.fromstring(r.text)
        except etree.XMLSyntaxError as e:
            raise XMLError(r.text) from e
        return tree

    def upload_query(self, nsql: str, db: Databases) -> QueryID:
        tree = self.send(build_content_pack(nsql, db))
        exc = tree.xpath("//Exception/text()")
        if exc:
            raise ContentPackageException(str(exc[0]))  # type: ignore
        return QUERY_CODE

    def run_query(self, query_id: QueryID) -> QueryResult:
        tree = self.send(create_query_body(query_id))
        exc = tree.xpath("//Exception/text()")
        if exc:
            raise QueryRunnerError(str(exc[0]))  # type: ignore
        return get_results(tree)

    def __enter__(self) -> XOG:
        return self

    def __exit__(self, *_, **__):
        if not self.c.is_closed:
            self.logout()
        self.c.close()


class Writer:
    def __init__(self, buff: FileTextWrite, format: Format, console: Console) -> None:
        self.buff = buff
        self.console = console
        self.format = format

    def pretty_print(self):
        return self.buff.isatty()

    def _result(self, query_id: QueryID, items: QueryResult):
        match self.format:
            case Format.json if self.pretty_print():
                return self.to_dict(items)
            case Format.json:
                return self.to_json(items)
            case Format.csv:
                return self.to_csv(items)
            case Format.table if not self.pretty_print():
                return self.to_csv(items, delimiter="\t")
            case Format.table:
                return self.to_table(query_id, items)

    def write_xml(self, result: etree._Element) -> int:
        content = etree.tostring(result, pretty_print=True, encoding="unicode")
        return self.buff.write(content)

    def write(self, query_id: QueryID, items: QueryResult) -> None:
        result = self._result(query_id, items)
        if self.pretty_print():
            self.console.print(result)
        else:
            self.buff.write(result)  # type: ignore

    def to_table(self, query_id: QueryID, items: QueryResult) -> Table:
        if not items:
            raise EmptyQueryResultError()

        table = Table(
            title=query_id,
            caption=f"Got {len(items)} records.",
            show_lines=True,
            highlight=True,
        )
        for column in items[0]:
            table.add_column(column)

        for row in items:
            table.add_row(*row.values())
        return table

    def to_csv(self, items: QueryResult, delimiter=",") -> CSV:
        if not items:
            raise EmptyQueryResultError()
        buff = StringIO()
        dict_writer = DictWriter(buff, items[0].keys(), delimiter=delimiter)
        dict_writer.writeheader()
        dict_writer.writerows(items)
        return buff.getvalue()

    def to_dict(self, items: QueryResult) -> dict[str, QueryResult]:
        if not items:
            raise EmptyQueryResultError()
        return {"records": items}

    def to_json(self, items: QueryResult) -> str:
        return json.dumps(self.to_dict(items), indent=4)


# Functions used by XOG


def build_content_pack(nsql_code: str, db: Databases) -> etree._Element:
    root = etree.Element(
        "NikuDataBus", nsmap={"xsi": "http://www.w3.org/2001/XMLSchema-instance"}
    )
    etree.SubElement(
        root,
        "Header",
        version="8.0",
        externalSource="xog",
        action="write",
        objectType="contentPack",
    )
    content_pack = etree.SubElement(root, "contentPack", update="true")
    queries = etree.SubElement(content_pack, "queries", update="true")
    query = etree.SubElement(
        queries,
        "query",
        code=QUERY_CODE,
        isUserPortletAvailable="0",
        source="customer",
    )
    etree.SubElement(query, "nls", languageCode="en", name=QUERY_CODE)
    nsql = etree.SubElement(query, "nsql", dbId=db.value, dbVendor="all")
    nsql.text = etree.CDATA(nsql_code)
    return root


def create_envelope(
    transform_header: Callable[[etree._Element], Any], payload_root: etree._Element
) -> etree._Element:
    root = etree.Element(
        "{http://schemas.xmlsoap.org/soap/envelope/}Envelope",
        nsmap=NS,
    )
    header = etree.SubElement(root, etree.QName(root.nsmap["soap"], "Header"))
    auth = etree.SubElement(header, etree.QName(root.nsmap["xog"], "Auth"))
    transform_header(auth)

    body = etree.SubElement(root, etree.QName(root.nsmap["soap"], "Body"))
    body.append(payload_root)
    return root


def create_session_id_envelope(session_id: str, payload_root: etree._Element):
    def apply_session_id(auth_header: etree._Element):
        sid = etree.SubElement(
            auth_header, etree.QName(auth_header.nsmap["xog"], "SessionID")
        )
        sid.text = session_id

    return create_envelope(
        apply_session_id,
        payload_root,
    )


def create_logout_envelope(session_id: str):
    root = etree.Element(etree.QName(NS["xog"], "Logout"))
    return create_session_id_envelope(session_id, root)


def create_login_envelope(username: str, password: str):
    login = etree.Element(etree.QName(NS["xog"], "Login"))
    u = etree.SubElement(login, etree.QName(NS["xog"], "Username"))
    p = etree.SubElement(login, etree.QName(NS["xog"], "Password"))
    u.text = username
    p.text = password

    return create_envelope(id, login)


def create_query_body(query_id: str):
    query = etree.Element("Query", nsmap={None: "http://www.niku.com/xog/Query"})  # type: ignore
    code = etree.SubElement(query, "Code")
    code.text = query_id
    return query


def get_results(root: etree._Element) -> QueryResult:
    """
    Converts <Records> into list[dict].
    """
    return [
        {etree.QName(child).localname: child.text for child in node.getchildren()}
        for node in root.xpath(
            "//Query:Record", namespaces={"Query": "http://www.niku.com/xog/Query"}
        )
    ]


def parse_xml(f: TextIO) -> etree._Element:
    et = etree.parse(f)
    return et.getroot()


def read_xml(path: Path) -> etree._Element:
    with path.open("r") as f:
        return parse_xml(f)
