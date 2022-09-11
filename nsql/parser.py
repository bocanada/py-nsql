from collections.abc import Iterable
from typing import TypeGuard
import sqlparse
from io import TextIOWrapper
from itertools import chain
from operator import attrgetter


def is_from_token(token):
    return sqlparse.sql.imt(token, t=sqlparse.tokens.Keyword) and token.match(
        sqlparse.tokens.Keyword, values="FROM"
    )


def is_where_token(token) -> TypeGuard[sqlparse.sql.Where]:
    return sqlparse.sql.imt(token, i=sqlparse.sql.Where)


def _convert(statements: Iterable[sqlparse.sql.Statement]):
    inside_select = True
    is_first = True
    for i, token in enumerate(
        chain.from_iterable(map(attrgetter("tokens"), statements))
    ):
        if isinstance(token, sqlparse.sql.IdentifierList) and inside_select:
            identifiers: list[sqlparse.sql.Identifier] = list(token.get_identifiers())
            for j, id in enumerate(identifiers, 1):
                table = id._get_first_name()
                real_name = id.get_real_name()
                attr = id.get_alias() or real_name
                if is_first:
                    stmt = f"@SELECT:DIM:USER_DEF:IMPLIED:T:{table}.{real_name}:{attr}@"
                    is_first = False
                else:
                    stmt = f"@SELECT:DIM_PROP:USER_DEF:IMPLIED:T:{table}.{real_name}:{attr}@"
                # yield stmt with a comma for all but the last identifier
                # that is, as long as the next token is not the FROM keyword
                yield stmt + ("," if j != len(identifiers) else "") + "\n"
            continue
        elif is_from_token(token):
            inside_select = False
        elif is_where_token(token):
            # This is neccesary or the content package fails @FILTER@
            token.insert_after(
                i, sqlparse.sql.Token(sqlparse.tokens.Comparison, "AND @FILTER@\n")
            )
        yield str(token)


def sql_to_nsql(file: TextIOWrapper):
    """
    Converts a SQL file/str to a NSQL str.
    Constraints:
        - The SQL MUST have a WHERE clause.
        - The SQL should not have any sub-queries/function calls inside SELECT.
        - CTEs are prohibited. NSQL doesn't permit any kind of code before the SELECT keyword.
    """
    return "".join(_convert(sqlparse.parsestream(file)))
