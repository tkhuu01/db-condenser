# Compatibility exports for existing callers. SQL implementations live behind
# the backend boundary; traversal uses only the graph helpers in this module.
from db_condenser.backends.sql import (  # noqa: F401
    columns_joined,
    columns_to_copy,
    columns_tupled,
    fully_qualified_table,
    mysql_db_name_hack,
    quoter,
    schema_name,
    table_name,
)
from db_condenser.config_reader import get_config


def upstream_filter_match(target, table_columns):
    config = get_config()
    retval = []
    filters = config.upstream_filters
    for filter in filters:
        if filter.table is not None and target == filter.table:
            retval.append(filter.condition)
        if filter.column is not None and filter.column in table_columns:
            retval.append(filter.condition)
    return retval


def redact_relationships(relationships):
    config = get_config()
    breaks = config.dependency_break_set
    retval = [
        r for r in relationships if (r["fk_table"], r["target_table"]) not in breaks
    ]
    return retval


def find(f, seq):
    """Return first item in sequence where f(item) == True."""
    for item in seq:
        if f(item):
            return item


def compute_upstream_tables(target_tables: list[str], order):
    upstream_tables = []
    in_upstream = False
    for strata in order:
        if in_upstream:
            upstream_tables.extend(strata)
        if any([tt in strata for tt in target_tables]):
            in_upstream = True
    return upstream_tables


def compute_upstream_strata(target_tables: list[str], order) -> list[set[str]]:
    strata_list = []
    in_upstream = False
    for stratum in order:
        if in_upstream:
            strata_list.append(set(stratum))
        if any(tt in stratum for tt in target_tables):
            in_upstream = True
    return strata_list


def compute_downstream_tables(passthrough_tables, disconnected_tables, order):
    downstream_tables = []
    for strata in order:
        downstream_tables.extend(strata)
    downstream_tables = list(
        reversed(
            list(
                filter(
                    lambda table: (
                        table not in passthrough_tables
                        and table not in disconnected_tables
                    ),
                    downstream_tables,
                )
            )
        )
    )
    return downstream_tables


def compute_downstream_strata(
    passthrough_tables, disconnected_tables, order
) -> list[set[str]]:
    excluded = set(passthrough_tables) | set(disconnected_tables)
    strata_list = []
    for stratum in order:
        filtered = {t for t in stratum if t not in excluded}
        if filtered:
            strata_list.append(filtered)
    strata_list.reverse()
    return strata_list


def compute_disconnected_tables(
    target_tables: list[str],
    passthrough_tables: list[str],
    all_tables: list[str],
    relationships,
):
    uf = UnionFind()
    for t in all_tables:
        uf.make_set(t)
    for rel in relationships:
        uf.link(rel["fk_table"], rel["target_table"])

    connected_components = set([uf.find(tt) for tt in target_tables])
    connected_components.update([uf.find(pt) for pt in passthrough_tables])
    return [t for t in all_tables if uf.find(t) not in connected_components]


def print_progress(target, idx, count):
    print("Processing {} of {}: {}".format(idx, count, target))


class UnionFind:
    def __init__(self):
        self.elementsToId = dict()
        self.elements = []
        self.roots = []
        self.ranks = []

    def __len__(self):
        return len(self.roots)

    def make_set(self, elem):
        self.id_of(elem)

    def find(self, elem):
        x = self.elementsToId[elem]
        if x is None:
            return None

        rootId = self.find_internal(x)
        return self.elements[rootId]

    def find_internal(self, x):
        x0 = x
        while self.roots[x] != x:
            x = self.roots[x]

        while self.roots[x0] != x:
            y = self.roots[x0]
            self.roots[x0] = x
            x0 = y

        return x

    def id_of(self, elem):
        if elem not in self.elementsToId:
            idx = len(self.roots)
            self.elements.append(elem)
            self.elementsToId[elem] = idx
            self.roots.append(idx)
            self.ranks.append(0)

        return self.elementsToId[elem]

    def link(self, elem1, elem2):
        x = self.id_of(elem1)
        y = self.id_of(elem2)

        xr = self.find_internal(x)
        yr = self.find_internal(y)
        if xr == yr:
            return

        xd = self.ranks[xr]
        yd = self.ranks[yr]
        if xd < yd:
            self.roots[xr] = yr
        elif yd < xd:
            self.roots[yr] = xr
        else:
            self.roots[yr] = xr
            self.ranks[xr] = self.ranks[xr] + 1

    def members_of(self, elem):
        id = self.elementsToId[elem]
        if id is None:
            raise ValueError("tried calling membersOf on an unknown element")

        elemRoot = self.find_internal(id)
        retval = []
        for idx in range(len(self.elements)):
            otherRoot = self.find_internal(idx)
            if elemRoot == otherRoot:
                retval.append(self.elements[idx])

        return retval


def compute_batch_size(column_count: int) -> int:
    target_bytes = 300_000_000
    bytes_per_row = max(column_count * 150, 400)
    batch = target_bytes // bytes_per_row
    return max(100_000, min(batch, 500_000))
