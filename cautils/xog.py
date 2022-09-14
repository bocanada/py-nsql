from __future__ import annotations
from collections.abc import Callable
from csv import DictWriter
from dataclasses import dataclass, field
from enum import Enum
from io import StringIO
import json
from pathlib import Path
from typing import Any, Final, Literal, NewType, Optional, TextIO, TypeAlias

from httpx import Client
from lxml import etree
from rich.console import Console
from rich.live import Live
from rich.pager import SystemPager
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
        self.c = Client(
            base_url=self.base_url,
            timeout=self.timeout,
        )
        self.login()

    def login(self):
        tree = self.send(
            create_login_envelope(self.username, self.password), should_auth=False
        )
        exc = tree.xpath("//Exception/text()", NS)
        if exc:
            raise InvalidLoginError(str(exc[0]))  # type: ignore
        self.session_id = str(tree.xpath("//xog:SessionID/text()", NS)[0])
        if not self.session_id:
            raise InvalidLoginError(str(exc[0]))  # type: ignore
        return self.session_id

    def logout(self):
        """
        Logs out from XOG
        """
        return self.send(create_logout_body())

    def send(self, body: Xml, should_auth: bool = True) -> Xml:
        """
        Sends a XOG.

        Returns the response as an Element.

        If HTTPStatus != 200, it raises an HTTPError.
        If XML is malformed, it raises an XMLError from XMLSyntaxError.
        """
        body = (
            create_session_id_envelope(self.session_id, body) if should_auth else body
        )
        bbody = bytes(body)
        r = self.c.post(
            "niku/xog",
            headers={"Content-Type": "text/xml; charset=utf-8"},
            content=bbody,
        )
        if r.is_error:
            raise HTTPError(r.text)
        try:
            tree = Xml.from_element(etree.fromstring(r.text))
        except etree.XMLSyntaxError as e:
            raise XMLError(r.text) from e
        return tree

    def query_get(self, query_id: QueryID, db: Databases):
        r = self.send(build_query_read_package(query_id, db))
        query = r.xpath(f"//query[@code='{query_id}']")[0]
        nsql = query.find("nsql")
        assert nsql is not None
        return nsql

    def upload_query(
        self, nsql: str, db: Databases, query_id: QueryID = QUERY_CODE
    ) -> QueryID:
        """
        XOGs a query (ContentPackage)
        """
        tree = self.send(build_query_write_package(nsql, db, query_id))
        exc = tree.xpath("//Exception/text()")
        if exc:
            raise ContentPackageException(str(exc[0]))  # type: ignore
        return query_id

    def run_query(self, query_id: QueryID) -> QueryResult:
        """
        Sends a Query XOG
        """
        tree = self.send(build_query_run_xog(query_id))
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

    def write_xml(self, result: Xml) -> int:
        return result.write(self.buff)  # type: ignore

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
            expand=True,
        )
        for column in items[0]:
            table.add_column(column, overflow="fold")

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


def build_content_pack(
    add_body: Callable[[Xml], Xml],
    action: Literal["write"] | Literal["read"] = "write",
    header_attrs: dict[str, str] = {},
) -> Xml:
    root = Xml.create(
        "NikuDataBus", nsmap={"xsi": "http://www.w3.org/2001/XMLSchema-instance"}
    )
    header = root.create_subelement(
        "Header",
        version="8.0",
        externalSource="xog",
        action=action,
        objectType="contentPack",
    )
    for k, v in header_attrs.items():
        header[k] = v
    add_body(root)
    return root


def build_query_read_package(query_id: QueryID, source: Databases):
    def query_query(root: Xml):
        query = root.create_subelement("QueryQuery")
        filter = query.create_subelement("Filter", name="code", criteria="EQUALS")

        filter.text = query_id
        return root

    return build_content_pack(
        query_query, action="read", header_attrs={"externalSource": source}
    )


def build_query_write_package(nsql_code: str, db: Databases, query_id: QueryID):
    def query_run(root: Xml):

        content_pack = root.create_subelement("contentPack", update="true")
        queries = content_pack.create_subelement("queries", update="true")
        query = queries.create_subelement(
            "query",
            code=query_id,
            isUserPortletAvailable="0",
            source="customer",
        )
        query.create_subelement("nls", languageCode="en", name=query_id)
        nsql = query.create_subelement("nsql", dbId=db.value, dbVendor="all")
        nsql.text = etree.CDATA(nsql_code)
        return content_pack

    return build_content_pack(query_run, action="write")


def create_envelope(transform_header: Callable[[Xml], Any], payload_root: Xml) -> Xml:
    root = Xml.create("soap:Envelope", nsmap=NS)
    header = root.create_subelement("Header", ns="soap")
    auth = header.create_subelement("Auth", ns="xog")
    transform_header(auth)

    body = root.create_subelement("Body", ns="soap")
    body.append(payload_root)
    return root


def create_session_id_envelope(session_id: str, payload_root: Xml):
    def apply_session_id(auth_header: Xml):
        sid = auth_header.create_subelement("SessionID", ns="xog")
        sid.text = session_id

    return create_envelope(
        apply_session_id,
        payload_root,
    )


def create_logout_body():
    return Xml.create("xog:Logout", nsmap=NS)


def create_login_envelope(username: str, password: str):
    login = Xml.create("xog:Login", nsmap=NS)
    u = login.create_subelement("Username", ns="xog")
    p = login.create_subelement("Password", ns="xog")
    u.text = username
    p.text = password

    return create_envelope(id, login)


def build_query_run_xog(query_id: str):
    query = Xml.create("Query", nsmap={None: "http://www.niku.com/xog/Query"})  # type: ignore
    code = query.create_subelement("Code")
    code.text = query_id
    return query


def get_results(root: Xml) -> QueryResult:
    """
    Converts <Records> into list[dict].
    """
    return [
        {child.local_name: child.text for child in node}
        for node in root.xpath(
            "//Query:Record", nsmap={"Query": "http://www.niku.com/xog/Query"}
        )
    ]


def parse_xml(f: TextIO) -> etree._Element:
    et = etree.parse(f)
    return et.getroot()


def read_xml(path: Path) -> etree._Element:
    with path.open("r") as f:
        return parse_xml(f)


@dataclass
class Xml:
    __elements: etree._Element = field(init=False)

    @classmethod
    def create(cls, tag: str, *, nsmap: dict[Any, Any], **attrs: str):
        if ":" in tag:
            ns, tag = tag.split(":")
            if ns not in nsmap:
                raise Exception(f"Unknown namespace: {ns}")
            tag = etree.QName(nsmap[ns], tag)  # type: ignore
        return cls.from_element(etree.Element(tag, nsmap=nsmap, **attrs))

    @classmethod
    def from_element(cls, e: etree._Element):
        self = cls()
        self.__elements = e
        return self

    @classmethod
    def read(cls, f: Path | TextIO):
        if isinstance(f, Path):
            e = read_xml(f)
        else:
            e = parse_xml(f)
        return cls.from_element(e)

    def write(self, f: Path | TextIO):
        if isinstance(f, Path):
            with f.open("wb") as fh:
                return fh.write(bytes(self))
        else:
            return f.write(self.dumps())

    def dumps(self, pretty_print: bool = True):
        return etree.tostring(
            self.__elements, encoding="unicode", pretty_print=pretty_print
        )

    @property
    def local_name(self):
        return self.qname().localname

    @property
    def tag(self):
        return self.__elements.tag

    def xpath(self, xpath: str, nsmap: Any = dict()) -> list[Xml]:
        els = self.__elements.xpath(xpath, namespaces=nsmap | self.__elements.nsmap)
        return [self.from_element(e) for e in els]  # type: ignore

    def find(self, child: str, ns: dict[Any, Any] | None = None) -> Xml | None:
        e = self.__elements.find(child, namespaces=ns)
        if e is None:
            return
        return self.from_element(e)

    def get(self, attr: str, default: Optional[str]):
        return self.__elements.get(attr, default)

    def qname(self) -> etree.QName:
        return etree.QName(self.__elements)

    def append(self, e: Xml):
        self.__elements.append(e.__elements)

    def create_subelement(
        self,
        element_name: str,
        ns: Optional[str] = None,
        nsmap: dict[str | None, str] | None = None,
        **attrs: str,
    ):
        qnsmap = nsmap or self.__elements.nsmap
        return self.from_element(
            etree.SubElement(
                self.__elements,
                etree.QName(qnsmap.get(ns), element_name),
                nsmap,  # type: ignore
                **attrs,
            )
        )

    @property
    def text(self) -> str | None:
        return self.__elements.text

    @text.setter
    def text(self, v: str) -> None:
        self.__elements.text = v

    def __iter__(self):
        yield from map(self.from_element, self.__elements)

    def __str__(self) -> str:
        return str(self.__elements)

    def __bytes__(self) -> bytes:
        return etree.tostring(self.__elements, encoding="utf-8", xml_declaration=True)

    def __len__(self) -> int:
        return len(self.__elements)

    def __setitem__(self, attr: str, value: str):
        self.__elements.set(attr, value)

    def __getitem__(self, attr: str, default: Optional[str] = None) -> str | None:
        return self.__elements.get(attr, default)
