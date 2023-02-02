from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, TextIO

from lxml import etree
from rich.syntax import Syntax


@dataclass
class Xml:
    __elements: etree._Element = field(init=False)

    @classmethod
    def create(cls, tag: str, *, nsmap: dict[Any, Any] = {}, **attrs: str | bytes):
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

    def write_to(self, f: Path | TextIO):
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
        ns = {k: v for k, v in (nsmap | self.__elements.nsmap).items() if k is not None}
        els = self.__elements.xpath(xpath, namespaces=ns)
        return [self.from_element(e) for e in els]  # type: ignore

    def find(self, child: str, ns: dict[Any, Any] | None = None) -> Xml | None:
        e = self.__elements.find(child, namespaces=ns)
        if e is None:
            return None
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
                **attrs,  # type: ignore
            )
        )

    def syntax(self, lines: int = 30) -> Syntax:
        return Syntax(self.dumps(), "xml", line_range=(None, lines))

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


def parse_xml(f: TextIO) -> etree._Element:
    et = etree.parse(f)
    return et.getroot()


def read_xml(path: Path) -> etree._Element:
    with path.open("rt") as f:
        return parse_xml(f)
