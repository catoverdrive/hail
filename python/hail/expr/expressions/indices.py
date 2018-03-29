from hail.typecheck import *

class Indices(object):
    @typecheck_method(source=anytype, axes=setof(str))
    def __init__(self, source=None, axes=set()):
        self.source = source
        self.axes = axes

    def __eq__(self, other):
        return isinstance(other, Indices) and self.source is other.source and self.axes == other.axes

    def __ne__(self, other):
        return not self.__eq__(other)

    @staticmethod
    def unify(*indices):
        axes = set()
        src = None
        for ind in indices:
            if src is None:
                src = ind.source
            else:
                if ind.source is not None and ind.source is not src:
                    from . import ExpressionException
                    raise ExpressionException()

            axes = axes.union(ind.axes)

        return Indices(src, axes)

    @property
    def key(self):
        from hail import Table, MatrixTable, struct
        if self.source is None:
            return None
        elif isinstance(self.source, Table):
            if self.source._row_axis in self.axes:
                return self.source.key
            else:
                return None
        else:
            assert(isinstance(self.source, MatrixTable))
            if self.source._row_axis in self.axes:
                if self.source._col_axis in self.axes:
                    return struct(**self.source.row_key, **self.source.col_key)
                else:
                    return self.source.row_key
            elif self.source._col_axis in self.axes:
                return self.source.col_key
            else:
                return None

    def __str__(self):
        return 'Indices(axes={}, source={})'.format(self.axes, self.source)

    def __repr__(self):
        return 'Indices(axes={}, source={})'.format(repr(self.axes), repr(self.source))


class Aggregation(object):
    def __init__(self, *exprs):
        self.exprs = exprs
        from ..expressions import unify_all
        indices, agg, _ = unify_all(*exprs)
        self.indices = indices


class Join(object):
    _idx = 0

    def __init__(self, join_function, temp_vars, uid, exprs):
        self.join_function = join_function
        self.temp_vars = temp_vars
        self.uid = uid
        self.exprs = exprs
        self.idx = Join._idx
        Join._idx += 1
