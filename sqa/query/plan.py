import bisect
import math
from typing import NamedTuple, Any, Iterable

import pyarrow
import pyarrow.compute
import pyarrow.dataset
import pyarrow.parquet

from sqa.duckdb import execute_sql
from sqa.layout import Partition
from . import MissingData
from .model import RefRel, Scan, Item, Model, FieldSelection, Table, ScanData, \
    ItemSrcQuery, ColumnName, JoinRel, SubRel
from .schema import ArchiveQuery
from .util import Params, include_columns, json_project, project, join_condition


ReqName = str
ItemName = str
Req = dict


class _ScanQuery(NamedTuple):
    table_name: str
    columns: list[str]
    filter: Any | None = None

    def fetch(self, partition: Partition) -> pyarrow.Table:
        return pyarrow.parquet.read_table(
            partition.get_table_file(self.table_name),
            columns=self.columns,
            filters=self.filter,
            use_threads=False
        )

    def include_columns(self, columns: list[ColumnName]) -> None:
        include_columns(self.columns, columns)


_ScanQueries = dict[ReqName, list[_ScanQuery]]


class _ScanSrcQuery:
    def __init__(self, scan: Scan, idx: int):
        self.req_name = scan.request_name()
        self.key = ['block_number'] + scan.table().primary_key
        self.idx = idx

    def fetch(self, _p: Partition, scan_data: ScanData) -> pyarrow.Table:
        table = scan_data[self.req_name][self.idx]
        return table.select(self.key)


class _RefRelQuery:
    def __init__(self, item: Item, rel: RefRel, idx: int):
        self.req_name = rel.scan.request_name()
        self.idx = idx
        self.scan_key = ['block_number'] + rel.scan_columns
        self.item_key = ['block_number'] + item.table().primary_key
        assert len(self.scan_key) == len(self.item_key)

    def fetch(self, _p: Partition, scan_data: ScanData) -> pyarrow.Table:
        table = scan_data[self.req_name][self.idx]
        table = table.select(self.scan_key)
        return table.rename_columns(self.item_key)


class _JoinRelQuery:
    def __init__(self, item: Item, rel: JoinRel, idx: int):
        self.req_name = rel.scan.request_name()
        self.idx = idx
        self.item_table = item.table().name
        self.params = Params()
        self.sql = (f'WITH "{self.item_table}" AS ('
                    f'SELECT * FROM read_parquet({self.params.new_variable("file")})'
                    f'), '
                    f'selected_items AS ({rel.query}) '
                    f'SELECT block_number, {project(item.table().primary_key)} '
                    f'FROM selected_items')

    def fetch(self, partition: Partition, scan_data: ScanData) -> pyarrow.Table:
        s = scan_data[self.req_name][self.idx]
        self.params.set_var('file', partition.get_table_file(self.item_table))
        return execute_sql(self.sql, self.params.list)


class _SubQuery:
    def __init__(self, item: Item, rel: SubRel, idx: int):
        self.req_name = rel.scan.request_name()
        self.item_table = item.table().name
        self.item_key = ['block_number'] + rel.scan_columns
        self.idx = idx

        group = self.item_key[0:-1]
        a = self.item_key[-1]

        self.params = Params()
        self.sql = (f'WITH items AS (SELECT * FROM read_parquet({self.params.new_variable("file")})), '
                    f'_all AS ('
                    f'SELECT {project(group)}, list({a} ORDER BY {a}) AS _all FROM items GROUP BY {", ".join(group)}'
                    f'), '
                    f'_scanned AS ('
                    f'SELECT {project(group)}, list({a} ORDER BY {a}) AS _scanned FROM s GROUP BY {", ".join(group)}'
                    f') '
                    f'SELECT {project(group, "_all.")}, _all, _scanned '
                    f'FROM _all, _scanned '
                    f'WHERE {join_condition(group, "_all", "_scanned")}')

    def fetch(self, partition: Partition, scan_data: ScanData) -> pyarrow.Table:
        s = scan_data[self.req_name][self.idx].select(self.item_key)

        self.params.set_var('file', partition.get_table_file(self.item_table))
        addresses = execute_sql(self.sql, self.params.list)

        all_addrs = addresses.column('_all').to_pylist()
        scanned_addrs = addresses.column('_scanned').to_pylist()

        selected = pyarrow.array(
            (_select_child_addresses(a, s) for a, s in zip(all_addrs, scanned_addrs)),
            type=pyarrow.list_(pyarrow.list_(pyarrow.int32()))
        )

        group = self.item_key[0:-1]
        a = self.item_key[-1]
        selected_addresses = addresses.select(group).append_column(a, selected)
        return execute_sql(f'SELECT {project(group)}, unnest({a}) AS {a} FROM selected_addresses')


_Address = list[int]


def _select_child_addresses(sorted_addresses: list[_Address], sorted_parents: list[_Address]) -> list[_Address]:
    result = []
    for parent in _tops(sorted_parents):
        i = bisect.bisect_right(sorted_addresses, parent)
        while i < len(sorted_addresses) and _is_child(parent, sorted_addresses[i]):
            result.append(sorted_addresses[i])
            i += 1
    return result


def _tops(sorted_addresses: list[_Address]) -> Iterable[_Address]:
    prev = None
    for a in sorted_addresses:
        if prev is None or not _is_child(prev, a):
            prev = a
            yield a
            if not a:
                return


def _is_child(parent: _Address, child: _Address) -> bool:
    if len(parent) >= len(child):
        return False
    for i in range(len(parent)):
        if parent[i] != child[i]:
            return False
    return True


def _get_weight_exp(item: Item, params: Params, fields: FieldSelection) -> str:
    weight_sum = 0
    weight_components = []
    selected_columns = item.selected_columns(fields)
    column_weights = item.table().column_weights

    for c in selected_columns:
        w = column_weights.get(c, 32)
        if isinstance(w, str):
            weight_components.append(w)
        else:
            weight_sum += w

    weight_components.append(params.new_param(weight_sum) + '::int8')
    return ' + '.join(weight_components)


def _get_missing_field(e: pyarrow.ArrowInvalid):
    if e.args and type(e.args[0]) == str:
        msg: str = e.args[0]
        if msg.startswith('No match for FieldRef'):
            field = msg[msg.find('(') + 1:msg.find(')')]
            return field


class _ItemSelectionQuery:
    def __init__(self, item: Item, fields: FieldSelection, sources: list[ItemSrcQuery]):
        self.table_name = item.table().name
        self.sources = sources
        self.params = Params()

        weight_exp = _get_weight_exp(item, self.params, fields)

        self.sql = (f'SELECT _idx AS idx, block_number, ({weight_exp}) AS weight '
                    f'FROM read_parquet({self.params.new_variable("file")}) i '
                    f'SEMI JOIN keys ON i.block_number = keys.block_number AND ')

        self.sql += join_condition(item.table().primary_key, 'i', 'keys')

    def fetch(self, partition: Partition, scan_data: ScanData) -> pyarrow.Table:
        keys = pyarrow.concat_tables(
            src.fetch(partition, scan_data) for src in self.sources
        )
        self.params.set_var('file', partition.get_table_file(self.table_name))
        return execute_sql(self.sql, self.params.list)


class _BlockHeaderWeightQuery:
    def __init__(self, block_item: Item, fields: FieldSelection):
        self.table_name = block_item.table().name
        self.params = Params()
        weight_exp = _get_weight_exp(block_item, self.params, fields)
        if '+' in weight_exp:
            self.sql = (f'SELECT h.number AS block_number, ({weight_exp} + w.weight) AS weight '
                        f'FROM read_parquet({self.params.new_variable("file")}) AS h, block_weights w '
                        f'WHERE h.number = w.block_number')
        else:
            self.sql = None

    def add_header_weight(self, partition: Partition, block_weights: pyarrow.Table) -> pyarrow.Table:
        if self.sql:
            self.params.set_var('file', partition.get_table_file(self.table_name))
            return execute_sql(self.sql, self.params.list)
        else:
            weight = self.params.list[0]
            return pyarrow.table({
                'block_number': block_weights.column('block_number'),
                'weight': pyarrow.compute.add(block_weights.column('weight'), weight)
            })


class _ItemDataQuery:
    def __init__(self, item: Item, fields: FieldSelection):
        self.table_name = item.table().name
        self.projected_columns = ['block_number'] + item.selected_columns(fields)

        order = ', '.join(item.table().primary_key)

        self.sql = (f'SELECT block_number, to_json(list({item.project(fields)} ORDER BY {order})) AS data '
                    f'FROM items '
                    f'GROUP BY block_number')

    def fetch(self, partition: Partition, index: pyarrow.Array) -> pyarrow.Table:
        items = _ScanQuery(
            table_name=self.table_name,
            columns=self.projected_columns,
            filter=pyarrow.compute.field('_idx').isin(index)
        ).fetch(partition)

        return execute_sql(self.sql)


class _BlockQuery:
    def __init__(self, block_item: Item, fields: FieldSelection, items: list[ItemName]):
        self.items = items
        self.projected_header_columns = block_item.selected_columns(fields)

        props = [
            ('header', block_item.project(fields))
        ]

        join = []

        for i, name in enumerate(items):
            props.append(
                (name, f'coalesce(i{i}.data, to_json(list_value()))')
            )
            join.append(f'LEFT OUTER JOIN i{i} ON b.number = i{i}.block_number ')

        self.sql = (f'SELECT b.number AS block_number, {json_project(props)} AS data '
                    f'FROM blocks AS b {"".join(join)}'
                    f'ORDER BY b.number')

    def fetch(self,
              partition: Partition,
              item_data: dict[ReqName, pyarrow.Table],
              block_numbers
              ) -> pyarrow.Table:

        blocks = _ScanQuery(
            table_name='blocks',
            columns=self.projected_header_columns,
            filter=pyarrow.compute.field('number').isin(block_numbers)
        ).fetch(partition)

        items = [item_data[name] for name in self.items]

        return self._execute(blocks, *items)

    def _execute(self,
                 blocks: pyarrow.Table,
                 i0: pyarrow.Table | None = None,
                 i1: pyarrow.Table | None = None,
                 i2: pyarrow.Table | None = None,
                 i3: pyarrow.Table | None = None,
                 i4: pyarrow.Table | None = None,
                 i5: pyarrow.Table | None = None,
                 i6: pyarrow.Table | None = None,
                 i7: pyarrow.Table | None = None,
                 *items: pyarrow.Table
                 ) -> pyarrow.Table:

        assert len(items) == 0
        return execute_sql(self.sql)


class QueryPlan:
    def __init__(self,
                 model: Model,
                 filelist: list[str],
                 q: ArchiveQuery,
                 size_limit: int = 40_000_000,
                 block_number_type=pyarrow.int32()
                 ):
        builder = _Builder(
            model=model,
            filelist=filelist,
            q=q
        )
        self._scan_queries = builder.scan_queries
        self._item_selection_queries = builder.item_selection_queries
        self._item_data_queries = builder.item_data_queries
        self._block_header_weight_query = builder.block_header_weight_query
        self._block_query = builder.block_query
        self._include_all_blocks = q.get('includeAllBlocks', False)
        self._first_block = q['fromBlock']
        self._last_block = q.get('toBlock', math.inf)
        self._size_limit = size_limit
        self._block_number_type = block_number_type

    def fetch(self, partition: Partition) -> pyarrow.Table:
        try:
            scan_data: ScanData = {}
            for req_name, scan_queries in self._scan_queries.items():
                scan_data[req_name] = [
                    q.fetch(partition) for q in scan_queries
                ]

            selected_items: dict[ItemName, pyarrow.Table] = {}
            for name, q in self._item_selection_queries.items():
                selected_items[name] = q.fetch(partition, scan_data)

            block_numbers = self._get_selected_blocks(partition, selected_items)

            item_data: dict[ItemName, pyarrow.Table] = {}
            for name, selection in selected_items.items():
                selection = selection.filter(pyarrow.compute.field('block_number').isin(block_numbers))
                data_query = self._item_data_queries[name]
                item_data[name] = data_query.fetch(partition, selection.column('idx'))

            return self._block_query.fetch(partition, item_data, block_numbers)
        except pyarrow.ArrowInvalid as e:
            if field := _get_missing_field(e):
                raise MissingData(f'field "{field}" is not available')
            raise e

    def _get_selected_blocks(self,
                             parition: Partition,
                             selected_items: dict[ItemName, pyarrow.Table]
                             ) -> pyarrow.ChunkedArray:

        first_block = max(parition.chunk.first_block, self._first_block)
        last_block = min(parition.chunk.last_block, self._last_block)

        union = []

        for t in selected_items.values():
            items = t.select(['block_number', 'weight'])
            union.append(items)

        if self._include_all_blocks:
            union.append(
                pyarrow.table({
                    'block_number': pyarrow.array(
                        range(first_block, last_block+1),
                        type=self._block_number_type
                    ),
                    'weight': pyarrow.array(
                        (0 for _ in range(last_block - first_block + 1)),
                        type=pyarrow.int64()
                    )
                })
            )
        else:
            union.append(
                pyarrow.table({
                    'block_number': pyarrow.array([first_block, last_block], type=self._block_number_type),
                    'weight': pyarrow.array([0, 0], type=pyarrow.int64())
                })
            )

        blocks: pyarrow.Table
        blocks = pyarrow.concat_tables(union)
        blocks = blocks.group_by('block_number').aggregate([('weight', 'sum')])
        blocks = blocks.rename_columns(['block_number', 'weight'])
        blocks = blocks.filter(
            (pyarrow.compute.field('block_number') >= pyarrow.compute.scalar(first_block)) &
            (pyarrow.compute.field('block_number') <= pyarrow.compute.scalar(last_block))
        )

        blocks = self._block_header_weight_query.add_header_weight(parition, blocks)
        blocks = blocks.sort_by('block_number')

        response_size = pyarrow.compute.cumulative_sum(blocks.column('weight'))
        cutoff_index = pyarrow.compute.greater(response_size, self._size_limit).index(True)

        block_numbers = blocks.column('block_number')
        if cutoff_index.as_py() >= 0:
            block_numbers = block_numbers.slice(0, cutoff_index.as_py() + 1)

        return block_numbers


class _Builder:
    scan_queries: _ScanQueries
    item_selection_queries: dict[ReqName, _ItemSelectionQuery]
    item_data_queries: dict[ItemName, _ItemDataQuery]

    def __init__(self,
                 model: Model,
                 q: ArchiveQuery,
                 filelist: list[str],
                 ):
        self.model = model
        self.q = q
        self.filelist = filelist

        self.scan_queries = {}
        self.item_selection_queries = {}
        self.item_data_queries = {}

        self._build_scan_queries()
        self._build_item_queries()

        block_item = self._find_block_item()
        fields = self.q.get('fields', {})
        self.block_header_weight_query = _BlockHeaderWeightQuery(block_item, fields)
        self.block_query = _BlockQuery(block_item, fields, list(self.item_data_queries.keys()))

    def _build_scan_queries(self) -> None:
        for s in self.model:
            if isinstance(s, Scan):
                self._add_scan(s)

    def _add_scan(self, scan: Scan) -> None:
        req_name = scan.request_name()
        queries: list[_ScanQuery] = []

        for i, req in enumerate(self.q.get(req_name, [])):
            where = [c for c in scan.where(req) if c is not None]
            where.append(pyarrow.compute.field('block_number') >= self.q['fromBlock'])
            if 'toBlock' in self.q:
                where.append(pyarrow.compute.field('block_number') <= self.q['toBlock'])

            if where:
                filter_ = where[0]
                for c in where[1:]:
                    filter_ = filter_ & c
            else:
                filter_ = None

            q = _ScanQuery(
                scan.table().name,
                ['block_number'] + scan.table().primary_key,
                filter_
            )

            queries.append(q)

        if queries:
            self._check_table(scan.table())
            self.scan_queries[req_name] = queries

    def _check_table(self, table: Table) -> None:
        file = f'{table.name}.parquet'
        if file not in self.filelist:
            raise MissingData(
                f'"{table.name}" data is not supported by this archive on requested block range'
            )

    def _find_block_item(self) -> Item:
        ls = [item for item in self.model if isinstance(item, Item) and item.name() == 'blocks']
        assert len(ls) == 1
        block_item = ls[0]
        assert block_item.table().name == 'blocks'
        return block_item

    def _build_item_queries(self) -> None:
        for item in self.model:
            if isinstance(item, Item) and item.name() != 'blocks':
                self._add_item(item)

    def _add_item(self, item: Item) -> None:
        queries: list[ItemSrcQuery] = []

        for src in item.sources:
            if isinstance(src, Scan):
                assert src.table() == item.table()
                req_name = src.request_name()
                for i, _ in enumerate(self.q.get(req_name, [])):
                    assert self.scan_queries[req_name][i]
                    q = _ScanSrcQuery(src, i)
                    queries.append(q)
            elif isinstance(src, RefRel) or isinstance(src, JoinRel) or isinstance(src, SubRel):
                req_name = src.scan.request_name()
                for i, req in enumerate(self.q.get(req_name, [])):
                    if req.get(src.include_flag_name):
                        if src.scan_columns:
                            self.scan_queries[req_name][i].include_columns(src.scan_columns)
                        if isinstance(src, RefRel):
                            q = _RefRelQuery(item, src, i)
                        elif isinstance(src, JoinRel):
                            q = _JoinRelQuery(item, src, i)
                        else:
                            q = _SubQuery(item, src, i)
                        queries.append(q)
            else:
                for q in src.get_queries(self.q, self.scan_queries):
                    queries.append(q)

        if not queries:
            return

        self._check_table(item.table())

        fields = self.q.get('fields', {})

        self.item_selection_queries[item.name()] = _ItemSelectionQuery(
            item,
            fields,
            queries
        )

        self.item_data_queries[item.name()] = _ItemDataQuery(
            item,
            fields
        )
