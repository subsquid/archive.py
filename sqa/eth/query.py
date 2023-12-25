import itertools
from typing import TypedDict, Iterable

import marshmallow as mm
from pyarrow.dataset import Expression

from sqa.query.model import JoinRel, RefRel, Table, Item, FieldSelection, Scan, SubRel
from sqa.query.schema import BaseQuerySchema
from sqa.query.util import to_snake_case, json_project, get_selected_fields, field_in, remove_camel_prefix


class BlockFieldSelection(TypedDict, total=False):
    number: bool
    hash: bool
    parentHash: bool
    timestamp: bool
    transactionsRoot: bool
    receiptsRoot: bool
    stateRoot: bool
    logsBloom: bool
    sha3Uncles: bool
    extraData: bool
    miner: bool
    nonce: bool
    mixHash: bool
    size: bool
    gasLimit: bool
    gasUsed: bool
    difficulty: bool
    totalDifficulty: bool
    baseFeePerGas: bool


TxFieldSelection = TypedDict('TxFieldSelection', {
    'transactionIndex': bool,
    'hash': bool,
    'nonce': bool,
    'from': bool,
    'to': bool,
    'input': bool,
    'value': bool,
    'gas': bool,
    'gasPrice': bool,
    'maxFeePerGas': bool,
    'maxPriorityFeePerGas': bool,
    'v': bool,
    'r': bool,
    's': bool,
    'yParity': bool,
    'chainId': bool,
    'sighash': bool,
    'contractAddress': bool,
    'gasUsed': bool,
    'cumulativeGasUsed': bool,
    'effectiveGasPrice': bool,
    'type': bool,
    'status': bool
}, total=False)


class LogFieldSelection(TypedDict, total=False):
    logIndex: bool
    transactionIndex: bool
    transactionHash: bool
    address: bool
    data: bool
    topics: bool


class TraceFieldSelection(TypedDict, total=False):
    traceAddress: bool
    subtraces: bool
    transactionIndex: bool
    type: bool
    error: bool
    revertReason: bool
    createFrom: bool
    createValue: bool
    createGas: bool
    createInit: bool
    createResultGasUsed: bool
    createResultCode: bool
    createResultAddress: bool
    callFrom: bool
    callTo: bool
    callValue: bool
    callGas: bool
    callInput: bool
    callSighash: bool
    callType: bool
    callResultGasUsed: bool
    callResultOutput: bool
    suicideAddress: bool
    suicideRefundAddress: bool
    suicideBalance: bool
    rewardAuthor: bool
    rewardValue: bool
    rewardType: bool


class StateDiffFieldSelection(TypedDict, total=False):
    transactionIndex: bool
    address: bool
    key: bool
    kind: bool
    prev: bool
    next: bool


class LogRequest(TypedDict, total=False):
    address: list[str]
    topic0: list[str]
    topic1: list[str]
    topic2: list[str]
    topic3: list[str]
    transaction: bool
    transactionTraces: bool
    transactionLogs: bool


TxRequest = TypedDict('TxRequest', {
    'from': list[str],
    'to': list[str],
    'sighash': list[str],
    'logs': bool,
    'traces': bool,
    'stateDiffs': bool
}, total=False)


class TraceRequest(TypedDict, total=False):
    type: list[str]
    createFrom: list[str]
    callFrom: list[str]
    callTo: list[str]
    callSighash: list[str]
    suicideRefundAddress: list[str]
    rewardAuthor: list[str]
    transaction: bool
    subtraces: bool
    parents: bool


class StateDiffRequest(TypedDict, total=False):
    address: list[str]
    key: list[str]
    kind: list[str]
    transaction: bool


def _field_map_schema(typed_dict):
    return mm.fields.Dict(
        mm.fields.Str(validate=lambda k: k in typed_dict.__optional_keys__),
        mm.fields.Boolean(),
        required=False
    )


class _FieldSelectionSchema(mm.Schema):
    block = _field_map_schema(BlockFieldSelection)
    transaction = _field_map_schema(TxFieldSelection)
    log = _field_map_schema(LogFieldSelection)
    trace = _field_map_schema(TraceFieldSelection)
    stateDiff = _field_map_schema(StateDiffFieldSelection)


class _LogRequestSchema(mm.Schema):
    address = mm.fields.List(mm.fields.Str())
    topic0 = mm.fields.List(mm.fields.Str())
    topic1 = mm.fields.List(mm.fields.Str())
    topic2 = mm.fields.List(mm.fields.Str())
    topic3 = mm.fields.List(mm.fields.Str())
    transaction = mm.fields.Boolean()
    transactionTraces = mm.fields.Boolean()
    transactionLogs = mm.fields.Boolean()


_TxRequestSchema = mm.Schema.from_dict({
    'from': mm.fields.List(mm.fields.Str()),
    'to': mm.fields.List(mm.fields.Str()),
    'sighash': mm.fields.List(mm.fields.Str()),
    'logs': mm.fields.Boolean(),
    'traces': mm.fields.Boolean(),
    'stateDiffs': mm.fields.Boolean()
})


class _TraceRequestSchema(mm.Schema):
    type = mm.fields.List(mm.fields.Str())
    createFrom = mm.fields.List(mm.fields.Str())
    callFrom = mm.fields.List(mm.fields.Str())
    callTo = mm.fields.List(mm.fields.Str())
    callSighash = mm.fields.List(mm.fields.Str())
    suicideRefundAddress = mm.fields.List(mm.fields.Str())
    rewardAuthor = mm.fields.List(mm.fields.Str())
    transaction = mm.fields.Boolean()
    subtraces = mm.fields.Boolean()
    parents = mm.fields.Boolean()


class _StateDiffRequestSchema(mm.Schema):
    address = mm.fields.List(mm.fields.Str())
    key = mm.fields.List(mm.fields.Str())
    kind = mm.fields.List(mm.fields.Str())
    transaction = mm.fields.Boolean()


class _QuerySchema(BaseQuerySchema):
    fields = mm.fields.Nested(_FieldSelectionSchema())

    logs = mm.fields.List(mm.fields.Nested(_LogRequestSchema()))

    transactions = mm.fields.List(mm.fields.Nested(_TxRequestSchema()))

    traces = mm.fields.List(mm.fields.Nested(_TraceRequestSchema()))

    stateDiffs = mm.fields.List(mm.fields.Nested(_StateDiffRequestSchema()))


QUERY_SCHEMA = _QuerySchema()


_blocks_table = Table(
    name='blocks',
    primary_key=[],
    column_weights={
        'logs_bloom': 512,
        'extra_data': 'extra_data_size'
    }
)


_tx_table = Table(
    name='transactions',
    primary_key=['transaction_index'],
    column_weights={
        'input': 'input_size'
    }
)


_logs_table = Table(
    name='logs',
    primary_key=['log_index'],
    column_weights={
        'data': 'data_size'
    }
)


_traces_table = Table(
    name='traces',
    primary_key=['transaction_index', 'trace_address'],
    column_weights={
        'create_init': 'create_init_size',
        'create_result_code': 'create_result_code_size',
        'call_input': 'call_input_size',
        'call_result_output': 'call_result_output_size'
    }
)


_statediffs_table = Table(
    name='statediffs',
    primary_key=['transaction_index', 'address', 'key'],
    column_weights={
        'prev': 'prev_size',
        'next': 'next_size'
    }
)


class _BlockItem(Item):
    def table(self) -> Table:
        return _blocks_table

    def name(self) -> str:
        return 'blocks'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('block'), ['number', 'hash', 'parentHash'])

    def project(self, fields: FieldSelection) -> str:
        def rewrite_timestamp(f: str):
            if f == 'timestamp':
                return 'timestamp', f'epoch(timestamp)'
            else:
                return f

        return json_project(
            map(rewrite_timestamp, self.get_selected_fields(fields))
        )


class _TxScan(Scan):
    def table(self) -> Table:
        return _tx_table

    def request_name(self) -> str:
        return 'transactions'

    def where(self, req: TxRequest) -> Iterable[Expression | None]:
        yield field_in('to', req.get('to'))
        yield field_in('from', req.get('from'))
        yield field_in('sighash', req.get('sighash'))


class _TxItem(Item):
    def table(self) -> Table:
        return _tx_table

    def name(self) -> str:
        return 'transactions'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('transaction'), ['transactionIndex'])

    def project(self, fields: FieldSelection) -> str:
        def rewrite_chain_id(f: str):
            if f == 'chainId':
                return 'chainId', f'IF(chain_id > 9007199254740991, to_json(chain_id::text), to_json(chain_id))'
            else:
                return f

        return json_project(
            map(rewrite_chain_id, self.get_selected_fields(fields))
        )


class _LogScan(Scan):
    def table(self) -> Table:
        return _logs_table

    def request_name(self) -> str:
        return 'logs'

    def where(self, req: LogRequest) -> Iterable[Expression | None]:
        yield field_in('address', req.get('address'))
        yield field_in('topic0', req.get('topic0'))
        yield field_in('topic1', req.get('topic1'))
        yield field_in('topic2', req.get('topic2'))
        yield field_in('topic3', req.get('topic3'))


class _LogItem(Item):
    def table(self) -> Table:
        return _logs_table

    def name(self) -> str:
        return 'logs'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('log'), ['logIndex', 'transactionIndex'])

    def selected_columns(self, fields: FieldSelection) -> list[str]:
        columns = []
        for name in self.get_selected_fields(fields):
            if name == 'topics':
                columns.append('topic0')
                columns.append('topic1')
                columns.append('topic2')
                columns.append('topic3')
            else:
                columns.append(to_snake_case(name))
        return columns

    def project(self, fields: FieldSelection) -> str:
        def rewrite_topics(f: str):
            if f == 'topics':
                return 'topics', f'[topic for topic in list_value(topic0, topic1, topic2, topic3) ' \
                                 f'if topic is not null]'
            else:
                return f

        return json_project(
            map(rewrite_topics, self.get_selected_fields(fields))
        )


class _TraceScan(Scan):
    def table(self) -> Table:
        return _traces_table

    def request_name(self) -> str:
        return 'traces'

    def where(self, req: TraceRequest) -> Iterable[Expression | None]:
        yield field_in('type', req.get('type'))
        yield field_in('create_from', req.get('createFrom'))
        yield field_in('call_from', req.get('callFrom'))
        yield field_in('call_to', req.get('callTo'))
        yield field_in('call_sighash', req.get('callSighash'))
        yield field_in('suicide_refund_address', req.get('suicideRefundAddress'))
        yield field_in('reward_author', req.get('rewardAuthor'))


class _TraceItem(Item):
    def table(self) -> Table:
        return _traces_table

    def name(self) -> str:
        return 'traces'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('trace'), ['transactionIndex', 'traceAddress', 'type'])

    def project(self, fields: FieldSelection) -> str:
        selected = self.get_selected_fields(fields)

        create_result_fields = [f for f in selected if f.startswith('createResult')]
        create_action_fields = [f for f in selected if f.startswith('create') and not f.startswith('createResult')]
        call_result_fields = [f for f in selected if f.startswith('callResult')]
        call_action_fields = [f for f in selected if f.startswith('call') and not f.startswith('callResult')]
        suicide_fields = [f for f in selected if f.startswith('suicide')]
        reward_fields = [f for f in selected if f.startswith('reward')]

        all_action_fields = set(itertools.chain(
            create_result_fields,
            create_action_fields,
            call_result_fields,
            call_action_fields,
            suicide_fields,
            reward_fields
        ))

        rest_fields = [f for f in selected if f not in all_action_fields]

        topics = []

        if create_action_fields or create_result_fields:
            topics.append(('create', create_action_fields, create_result_fields))

        if call_action_fields or call_result_fields:
            topics.append(('call', call_action_fields, call_result_fields))

        if suicide_fields:
            topics.append(('suicide', suicide_fields, []))

        if reward_fields:
            topics.append(('reward', reward_fields, []))

        if topics:
            cases = []

            for topic, action_fields, result_fields in topics:
                ext = []

                if action_fields:
                    ext.append(('action', _trace_topic_projection(topic, action_fields)))

                if result_fields:
                    ext.append(('result', _trace_topic_projection(f'{topic}Result', result_fields)))

                proj = itertools.chain(rest_fields, ext)

                cases.append(
                    f"WHEN type='{topic}' THEN {json_project(proj)}"
                )

            when_exps = ' '.join(cases)
            exp = f'CASE {when_exps} ELSE {json_project(rest_fields)} END'
            if create_result_fields or call_result_fields:
                return f"list_transform([{exp}], o -> " \
                       "CASE len(json_keys(o, '$.result')) " \
                       "WHEN 0 THEN json_merge_patch(o, '{\"result\": null}') " \
                       "ELSE o END" \
                       ")[1]"
            else:
                return exp
        else:
            return json_project(rest_fields)


def _trace_topic_projection(topic: str, topic_fields: list[str]) -> str:
    assert topic_fields
    components = []
    for f in topic_fields:
        alias = remove_camel_prefix(f, topic)
        ref = f'"{to_snake_case(f)}"'
        components.append(
            f"CASE WHEN {ref} is null THEN "
            "'{}'::json ELSE "
            f"json_object('{alias}', {ref}) END"
        )
    if len(components) == 1:
        return components[0]
    else:
        return f'json_merge_patch({", ".join(components)})'


class _StateDiffScan(Scan):
    def table(self) -> Table:
        return _statediffs_table

    def request_name(self) -> str:
        return 'stateDiffs'

    def where(self, req: StateDiffRequest) -> Iterable[Expression | None]:
        yield field_in('address', req.get('address'))
        yield field_in('key', req.get('key'))
        yield field_in('kind', req.get('kind'))


class _StateDiffItem(Item):
    def table(self) -> Table:
        return _statediffs_table

    def name(self) -> str:
        return 'stateDiffs'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(
            fields.get('stateDiff'),
            ['transactionIndex', 'address', 'key', 'kind']
        )


def _build_model():
    tx_scan = _TxScan()
    log_scan = _LogScan()
    trace_scan = _TraceScan()
    state_diff_scan = _StateDiffScan()

    block_item = _BlockItem()
    tx_item = _TxItem()
    log_item = _LogItem()
    trace_item = _TraceItem()
    state_diff_item = _StateDiffItem()

    state_diff_item.sources.extend([
        state_diff_scan,
        JoinRel(
            scan=tx_scan,
            include_flag_name='stateDiffs',
            scan_columns=[],
            query='SELECT * FROM statediffs i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.transaction_index'
        )
    ])

    trace_item.sources.extend([
        trace_scan,
        JoinRel(
            scan=tx_scan,
            include_flag_name='traces',
            query='SELECT * FROM traces i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.transaction_index'
        ),
        JoinRel(
            scan=log_scan,
            include_flag_name='transactionTraces',
            scan_columns=['transaction_index'],
            query='SELECT * FROM traces i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.transaction_index'
        ),
        SubRel(
            scan=trace_scan,
            scan_columns=['transaction_index', 'trace_address'],
            include_flag_name='subtraces'
        ),
        JoinRel(
            scan=trace_scan,
            include_flag_name='parents',
            query='SELECT * FROM traces s, ('
                  'WITH RECURSIVE selected_traces(block_number, transaction_index, trace_address) AS ('
                  'SELECT block_number, transaction_index, array_pop_back(trace_address) AS trace_address '
                  'FROM s WHERE length(s.trace_address) > 0 '
                  'UNION ALL '
                  'SELECT block_number, transaction_index, array_pop_back(trace_address) AS trace_address '
                  'FROM selected_traces WHERE length(trace_address) > 0'
                  ') SELECT DISTINCT * FROM selected_traces'
                  ') AS ss '
                  'WHERE '
                  'i.block_number = ss.block_number AND '
                  'i.transaction_index = ss.transaction_index AND '
                  'i.trace_address = ss.trace_address'
        )
    ])

    log_item.sources.extend([
        log_scan,
        JoinRel(
            scan=tx_scan,
            include_flag_name='logs',
            query='SELECT * FROM logs i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.transaction_index'
        ),
        JoinRel(
            scan=log_scan,
            include_flag_name='transactionLogs',
            scan_columns=['transaction_index'],
            query='SELECT * FROM logs i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.transaction_index'
        ),
    ])

    tx_item.sources.extend([
        tx_scan,
        RefRel(
            scan=log_scan,
            include_flag_name='transaction',
            scan_columns=['transaction_index']
        ),
        RefRel(
            scan=trace_scan,
            include_flag_name='transaction',
            scan_columns=['transaction_index']
        ),
        RefRel(
            scan=state_diff_scan,
            include_flag_name='transaction',
            scan_columns=['transaction_index']
        )
    ])

    return [
        tx_scan,
        log_scan,
        trace_scan,
        state_diff_scan,
        block_item,
        tx_item,
        log_item,
        trace_item,
        state_diff_item
    ]


MODEL = _build_model()
