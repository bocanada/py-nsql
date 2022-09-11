from collections.abc import Iterable
from typing import TypeGuard, cast
import sqlparse
from io import TextIOWrapper
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


def _parse_functions(token: sqlparse.sql.Identifier, acc=""):
    """
    Recursively get FUNCTION(...params) and OVER (...)
    """
    f = token[0]

    if not isinstance(f, sqlparse.sql.Function):
        return acc.rstrip()

    inner_token = token.tokens.pop()
    if isinstance(inner_token, sqlparse.sql.Identifier):
        inner_tokens = inner_token
        return _parse_functions(inner_tokens, f"{acc}{f} ")

    return acc.rstrip()


def _parse_op_with_window_function(token: sqlparse.sql.Identifier, acc=""):
    op = token[0]

    inner_token = token.tokens.pop()
    if isinstance(inner_token, sqlparse.sql.Identifier):
        inner_tokens = inner_token
        return _parse_op_with_window_function(inner_tokens, f"{acc}{op} ")

    if not isinstance(op, sqlparse.sql.Operation):
        return acc.rstrip()
    return acc.rstrip()


def _parse_identifier(token: sqlparse.sql.Identifier, is_first: bool) -> str:
    alias = cast(str, token.get_name())
    table = token._get_first_name()
    real_name = token.get_real_name()

    tokens: Identifiers = token.tokens
    # Functions + window functions
    if isinstance(tokens[0], sqlparse.sql.Function):
        func = _parse_functions(token)
        return create_select(is_first, func, alias)
    # Operations + window functions
    if isinstance(tokens[0], sqlparse.sql.Operation):
        op = _parse_op_with_window_function(token)
        return create_select(is_first, f"{op}", alias)
    # Support subqueries
    if isinstance(tokens[0], sqlparse.sql.Parenthesis):
        parens = tokens[0]
        alias = cast(str, tokens.pop().get_name())
        return create_select(is_first, str(parens), alias)
    select = table if table == real_name else f"{table}.{real_name}"
    return create_select(is_first, select, alias)


def _convert(statements: Iterable[sqlparse.sql.Statement]):
    inside_select = True
    is_first = True
    enumerated_tokens = enumerate(
        chain.from_iterable(map(attrgetter("tokens"), statements))
    )
    for i, token in enumerated_tokens:
        if isinstance(token, sqlparse.sql.Identifier) and inside_select:
            yield _parse_identifier(token, is_first) + "\n"
            continue
        if isinstance(token, sqlparse.sql.IdentifierList) and inside_select:
            identifiers: Identifiers = list(token.get_identifiers())

            for j, id in enumerate(identifiers, 1):
                stmt = _parse_identifier(id, is_first)
                if is_first:
                    is_first = False
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
