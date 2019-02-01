import pyparsing as _pp


def _swap_infix(s, l, t):
    t[0][0], t[0][1] = t[0][1], t[0][0]


_quoted = _pp.QuotedString('"', escChar='\\')
_plain_tag = _pp.Word(_pp.alphas + "_", _pp.alphanums + "_-")
_tag = (_quoted | _plain_tag).setParseAction(lambda s, l, t: [['$', t[0]]])
_const = _pp.Word('01', exact=1).setParseAction(lambda s, l, t: [[t[0]]])
_term = _tag | _const

_expr = _pp.infixNotation(
    _term,
    [
        ("!", 1, _pp.opAssoc.RIGHT),
        ("&", 2, _pp.opAssoc.RIGHT, _swap_infix),
        ("|", 2, _pp.opAssoc.RIGHT, _swap_infix),
    ]
)


def compile(expr):
    try:
        ast = _expr.parseString(expr, parseAll=True).asList()[0]
    except _pp.ParseException as e:
        raise RuntimeError("Error parsing tag expression at \"@@@\": {}".format(e.markInputline("@@@"))) from e

    def evaluate(tags):
        def recurse(ast):
            if ast[0] == '$':
                return ast[1] in tags
            if ast[0] == '0':
                return False
            if ast[0] == '1':
                return True
            if ast[0] == '!':
                return not recurse(ast[1])
            if ast[0] == '&':
                return recurse(ast[1]) and recurse(ast[2])
            if ast[0] == '|':
                return recurse(ast[1]) or recurse(ast[2])

            assert 0

        return recurse(ast)

    return evaluate
