from dataclasses import dataclass

from cautils.thin_xml import Xml


class InvalidLoginError(Exception):
    ...


@dataclass
class XogException(Exception):
    msg: str
    exc: str
    raw: Xml


class EmptyError(Exception):
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


class NotFoundError(Exception):
    ...
