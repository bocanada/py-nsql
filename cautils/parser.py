from collections.abc import Iterable
from typing import TextIO, TypeGuard, cast
import sqlparse
from itertools import chain
from operator import attrgetter


Identifiers = list[sqlparse.sql.Identifier]


def is_from_token(token):
    return sqlparse.sql.imt(token, t=sqlparse.tokens.Keyword) and token.match(
        sqlparse.tokens.Keyword, values="FROM"
    )


def is_where_token(token) -> TypeGuard[sqlparse.sql.Where]:
    return sqlparse.sql.imt(token, i=sqlparse.sql.Where)


def create_select(is_dim: bool, select: str, alias: str):
    return f"@SELECT:{'DIM' + ('' if is_dim else '_PROP')}:USER_DEF:IMPLIED:T:{select}:{alias}@"


def _parse_ts(token: sqlparse.sql.Identifier, ts: tuple[type, ...], acc=""):
    """
    Recursively get [THING](...params) and OVER (...)
    """
    op = token[0]
    inner_token = token.tokens.pop()
    if isinstance(inner_token, sqlparse.sql.Identifier):
        inner_tokens = inner_token
        return _parse_ts(inner_tokens, ts, acc=f"{acc}{op} ")

    if not isinstance(op, ts):
        return acc.rstrip(" ")
    return str(op)


def _parse_identifier(token: sqlparse.sql.Identifier, is_first: bool) -> str:
    table, real_name, alias = (
        token._get_first_name(),
        token.get_real_name(),
        cast(str, token.get_name()),
    )

    tokens: Identifiers = token.tokens
    match type(tokens[0]):
        case sqlparse.sql.Case:
            return create_select(is_first, str(tokens[0]), alias)
        case sqlparse.sql.Function | sqlparse.sql.Operation:
            thing = _parse_ts(token, (sqlparse.sql.Function, sqlparse.sql.Operation))
            return create_select(is_first, thing, alias)
            # Support subqueries
        case sqlparse.sql.Parenthesis:
            parens = tokens[0]
            alias = cast(str, tokens.pop().get_name())
            return create_select(is_first, str(parens), alias)
        case _:
            select = table if table == real_name else f"{table}.{real_name}"
            return create_select(is_first, select, alias)


def _convert(statements: Iterable[sqlparse.sql.Statement]):
    inside_select = True
    is_first = True
    tokens = chain.from_iterable(map(attrgetter("tokens"), statements))
    for token in tokens:
        match type(token):
            # Only one identifier
            case sqlparse.sql.Identifier if inside_select:
                yield _parse_identifier(token, is_first) + "\n"
                continue
            # Many identifiers
            case sqlparse.sql.IdentifierList if inside_select:
                identifiers: Identifiers = list(token.get_identifiers())

                for j, id in enumerate(identifiers, 1):
                    # Fixes issue with columns having the same names as functions
                    if not isinstance(id, sqlparse.sql.Identifier):
                        id = sqlparse.sql.Identifier(
                            [sqlparse.sql.Token(sqlparse.tokens.Name, id.value)]
                        )
                    stmt = _parse_identifier(id, is_first)
                    if is_first:
                        is_first = False
                    # yield stmt with a comma for all but the last identifier
                    # that is, as long as the next token is not the FROM keyword
                    yield stmt + ("," if j != len(identifiers) else "") + "\n"
                continue
            case _ if is_from_token(token):
                inside_select = False
            case _ if is_where_token(token):
                # This is neccesary or the content package fails @FILTER@
                token.tokens.append(
                    sqlparse.sql.Token(sqlparse.tokens.Comparison, "AND @FILTER@\n")
                )
        yield str(token)


def sql_to_nsql(file: TextIO):
    """
    Converts a SQL file/str to a NSQL str.
    Constraints:
        - The SQL MUST have a WHERE clause.
        - The SQL should not have any sub-queries/function calls inside SELECT.
        - CTEs are prohibited. NSQL doesn't permit any kind of code before the SELECT keyword.
    """
    return "".join(_convert(sqlparse.parsestream(file)))
