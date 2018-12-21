import click
import difflib


def format_diff(a, b, *, color=True, header=[]):
    IDENTITY = lambda x: x
    EQUAL_STYLE = IDENTITY
    if color:
        HEADER_STYLE = lambda x: click.style(x, fg='yellow')
        SIMPLE_DEL_STYLE = lambda x: click.style(x, fg='red')
        FANCY_DEL_LINE_STYLE = SIMPLE_DEL_STYLE
        FANCY_DEL_SPAN_STYLE = lambda x: (
            click.style(x, reverse=True, reset=False)
            + click.style("", reverse=False, reset=False)
        )
        SIMPLE_INS_STYLE = lambda x: click.style(x, fg='cyan')
        FANCY_INS_LINE_STYLE = SIMPLE_INS_STYLE
        FANCY_INS_SPAN_STYLE = FANCY_DEL_SPAN_STYLE
    else:
        HEADER_STYLE = IDENTITY
        SIMPLE_DEL_STYLE = IDENTITY
        FANCY_DEL_LINE_STYLE = IDENTITY
        FANCY_DEL_SPAN_STYLE = IDENTITY
        SIMPLE_INS_STYLE = IDENTITY
        FANCY_INS_LINE_STYLE = IDENTITY
        FANCY_INS_SPAN_STYLE = IDENTITY

    a = a.splitlines(keepends=True)
    b = b.splitlines(keepends=True)

    def dump(tag, style, lines):
        for line in lines:
            if line[-1:] == "\n":
                yield style('%s %s' % (tag, line))
            else:
                yield style('%s %s\n' % (tag, line))
                yield "\\ No newline at end of file\n"

    def ellipsis():
        yield "...\n"

    # adapted and stripped of comments version of _fancy_replace from Python's difflib
    def fancy_replace(alo, ahi, blo, bhi):
        CUTOFF = 0.75
        best_ratio = 0.74
        sm = difflib.SequenceMatcher()
        eqi, eqj = None, None

        for j in range(blo, bhi):
            bj = b[j]
            sm.set_seq2(bj)
            for i in range(alo, ahi):
                ai = a[i]
                if ai == bj:
                    if eqi is None:
                        eqi, eqj = i, j
                    continue
                sm.set_seq1(ai)
                if(
                        sm.real_quick_ratio() > best_ratio
                        and sm.quick_ratio() > best_ratio
                        and sm.ratio() > best_ratio
                ):
                    best_ratio, best_i, best_j = sm.ratio(), i, j

        if best_ratio < CUTOFF:
            if eqi is None:
                yield from dump('-', SIMPLE_DEL_STYLE, a[alo:ahi])
                yield from dump('+', SIMPLE_INS_STYLE, b[blo:bhi])
                return
            best_i, best_j, best_ratio = eqi, eqj, 1.0
        else:
            eqi = None

        yield from fancy_helper(alo, best_i, blo, best_j)

        aelt, belt = a[best_i], b[best_j]
        if eqi is None:
            sm.set_seqs(aelt, belt)
            a_chunks = []
            b_chunks = []

            for opcode, ai1, ai2, bj1, bj2 in sm.get_opcodes():
                if opcode == 'replace':
                    a_chunks.append(FANCY_DEL_SPAN_STYLE(aelt[ai1:ai2]))
                    b_chunks.append(FANCY_INS_SPAN_STYLE(belt[bj1:bj2]))
                elif opcode == 'delete':
                    a_chunks.append(FANCY_DEL_SPAN_STYLE(aelt[ai1:ai2]))
                elif opcode == 'insert':
                    b_chunks.append(FANCY_INS_SPAN_STYLE(belt[bj1:bj2]))
                elif opcode == 'equal':
                    a_chunks.append(aelt[ai1:ai2])
                    b_chunks.append(belt[bj1:bj2])

            yield from dump('-', FANCY_DEL_LINE_STYLE, [''.join(a_chunks)])
            yield from dump('+', FANCY_INS_LINE_STYLE, [''.join(b_chunks)])
        else:
            yield from dump(' ', EQUAL_STYLE, a[best_i:best_i + 1])

        yield from fancy_helper(best_i + 1, ahi, best_j + 1, bhi)

    def fancy_helper(alo, ahi, blo, bhi):
        if alo < ahi:
            if blo < bhi:
                yield from fancy_replace(alo, ahi, blo, bhi)
            else:
                yield from dump('-', SIMPLE_DEL_STYLE, a[alo:ahi])
        elif blo < bhi:
            yield from dump('+', SIMPLE_INS_STYLE, b[blo, bhi])

    sm = difflib.SequenceMatcher(a=a, b=b)

    last_ahi = 0
    count = 0

    for group in sm.get_grouped_opcodes(n=1):
        for opcode, alo, ahi, blo, bhi in group:
            if count == 0 and header:
                yield from (HEADER_STYLE(i) + "\n" for i in header)

            if alo > last_ahi:
                yield from ellipsis()

            if opcode == 'equal':
                yield from dump(' ', EQUAL_STYLE, a[alo:ahi])
            elif opcode == 'delete':
                yield from dump('-', SIMPLE_DEL_STYLE, a[alo:ahi])
            elif opcode == 'insert':
                yield from dump('+', SIMPLE_INS_STYLE, b[blo:bhi])
            elif opcode == 'replace':
                yield from fancy_replace(alo, ahi, blo, bhi)

            count += 1
            last_ahi = ahi

    if count and last_ahi < len(a):
        yield from ellipsis()
