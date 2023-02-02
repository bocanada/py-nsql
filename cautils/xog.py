from __future__ import annotations
from collections.abc import Callable, Iterable
from csv import DictWriter
from dataclasses import dataclass, field
from enum import Enum
from io import StringIO
import json
from typing import Any, Final, Literal, NewType, TypeAlias

from httpx import Client
from lxml import etree
from rich.console import Console
from rich.table import Table
from typer import BadParameter, FileTextWrite

from cautils.exceptions import *
from cautils.thin_xml import Xml

NS = {
    "xog": "http://www.niku.com/xog",
    "soap": "http://schemas.xmlsoap.org/soap/envelope/",
}

QueryID = NewType("QueryID", str)

QUERY_CODE: Final[QueryID] = QueryID("query.runner")

QueryResult: TypeAlias = list[dict[str, Any]]
CSV: TypeAlias = str


class SortDirection(str, Enum):
    asc = "asc"
    desc = "desc"


@dataclass
class SortColumn:
    column: str
    direction: SortDirection

    def to_xml_node(self) -> Xml:
        column = Xml.create("Column")
        name = column.create_subelement("Name")
        name.text = self.column
        name = column.create_subelement("Direction")
        name.text = self.direction.value
        return column

    @classmethod
    def from_colon_separated_item(
        cls,
        items: str,
    ) -> SortColumn:
        column, value = items.split(":")
        try:
            return cls(column, SortDirection(value.lower()))
        except ValueError:
            raise BadParameter(
                f"{value!r} is not one of: {', '.join(map(repr, SortDirection._member_names_))}",
                None,
                None,
                "sort",
            )


class FilterType(str, Enum):
    lt = "lt"
    gt = "gt"
    eq = "eq"
    like = "like"


@dataclass
class Filter:
    type: FilterType
    column_name: str
    value: str

    @classmethod
    def from_colon_separated_item(
        cls,
        ty: FilterType,
        items: str,
    ) -> Filter:
        try:
            items_list = items.split(":")
            column, value = items_list
            return cls(ty, column, value)
        except ValueError:
            raise BadParameter(
                f"Expected key:value pairs but found: {items!r}", None, None, ty.name
            )

    @classmethod
    def from_colon_separated_items(
        cls,
        ty: FilterType,
        items: list[str],
    ) -> list[Filter]:
        return [cls.from_colon_separated_item(ty, item) for item in items]

    def to_xml_node(self):
        node = Xml.create(self.tag())
        node.text = self.value
        return node

    def tag(self) -> str:
        if self.type is FilterType.eq:
            return self.column_name
        if self.type is FilterType.like:
            return f"{self.column_name}_wildcard"
        if self.type is FilterType.gt:
            return f"{self.column_name}_from"
        if self.type is FilterType.lt:
            return f"{self.column_name}_to"
        raise ValueError(f"Expected {FilterType!r} but got {self.type!r}")


@dataclass
class Query:
    text: str
    id: QueryID


class Database(str, Enum):
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
        """
        Gets and sets a valid SessionID on the client.
        """
        try:
            tree = self.send(
                create_login_envelope(self.username, self.password), should_auth=False
            )
        except XogException as e:
            raise InvalidLoginError(e.exc) from e
        self.session_id = str(tree.xpath("//xog:SessionID/text()", NS)[0])
        if not self.session_id:
            raise InvalidLoginError("Couldn't get a valid SessionID")
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

        if xpath := tree.xpath("//Exception/text()", NS):
            description = str(tree.xpath("//Description/text()", NS)[0])
            raise XogException(description[:250], exc=str(xpath[0]), raw=tree)
        return tree

    def query_get(self, query_id: QueryID, db: Database) -> Query:
        try:
            r = self.send(build_query_read_package(query_id, db))
        except XogException as e:
            raise NotFoundError(e.exc) from e
        query_path = r.xpath(f"//query[@code='{query_id}']")
        if not query_path:
            raise NotFoundError(f"Query with id = {query_id!r} does not exist.")
        query, *_ = query_path
        nsql = query.find("nsql")
        if nsql is None:
            raise NotFoundError(f"Failed getting <nsql> for {query_id!r}.")
        if not nsql.text:
            raise EmptyError(f"Query {query_id!r} is empty.")
        return Query(nsql.text, query_id)

    def upload_query(
        self, nsql: str, db: Database, query_id: QueryID = QUERY_CODE
    ) -> QueryID:
        """
        XOGs a query (ContentPackage)
        """
        try:
            self.send(build_query_write_package(nsql, db, query_id))
        except XogException as e:
            raise ContentPackageException(e.exc) from e
        return query_id

    def run_query(
        self,
        query_id: QueryID,
        filters: Iterable[Filter],
        sort: Iterable[SortColumn],
        page_size: int | None = None,
    ) -> QueryResult:
        """
        Sends a Query XOG
        """
        try:
            tree = self.send(
                build_query_run_xog(
                    query_id,
                    filters=filters,
                    page_size=page_size,
                    sort=sort,
                )
            )
        except XogException as e:
            raise QueryRunnerError(e.exc) from e
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
        return result.write_to(self.buff)  # type: ignore

    def write(self, query_id: QueryID, items: QueryResult) -> None:
        result = self._result(query_id, items)
        if self.pretty_print():
            self.console.print(result)
        else:
            self.buff.write(result)  # type: ignore

    def to_table(self, query_id: QueryID, items: QueryResult) -> Table:
        if not items:
            raise EmptyQueryResultError(f"{query_id} returned 0 rows")

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

    def to_csv(self, items: QueryResult, delimiter: str = ",") -> CSV:
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


def build_query_read_package(query_id: QueryID, source: Database):
    def query_query(root: Xml):
        query = root.create_subelement("QueryQuery")
        filter = query.create_subelement("Filter", name="code", criteria="EQUALS")

        filter.text = query_id
        return root

    return build_content_pack(
        query_query, action="read", header_attrs={"externalSource": source}
    )


def build_query_write_package(nsql_code: str, db: Database, query_id: QueryID):
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
    # <soap:Envelope>
    # <soap:Header>
    # <xog:Auth>
    # </xog:Auth>
    # </soap:Header>
    # <soap:Body>
    # </soap:Body>
    # </soap:Envelope>
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


def build_query_run_xog(
    query_id: str,
    filters: Iterable[Filter],
    sort: Iterable[SortColumn],
    page_size: int | None = None,
):
    query = Xml.create("Query", nsmap={None: "http://www.niku.com/xog/Query"})
    code = query.create_subelement("Code")
    code.text = query_id
    if filters:
        filter = query.create_subelement("Filter")
        for pred in filters:
            filter.append(pred.to_xml_node())
    if page_size:
        slice = Xml.create("Slice")
        s = slice.create_subelement("Size")
        s.text = str(page_size)
        # TODO: Should we allow pagination?
        n = slice.create_subelement("Number")
        n.text = "0"
        query.append(slice)
    if sort:
        s = query.create_subelement("Sort")
        for clm in sort:
            s.append(clm.to_xml_node())

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
