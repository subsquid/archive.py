import itertools
from typing import TypedDict

import marshmallow as mm

from sqa.query.model import Table, Builder, JoinRel, RefRel
from sqa.query.schema import BaseQuerySchema
from sqa.query.util import json_project, remove_camel_prefix, to_snake_case


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


class FieldSelection(TypedDict, total=False):
    block: BlockFieldSelection
    transaction: TxFieldSelection
    log: LogFieldSelection
    trace: TraceFieldSelection
    stateDiff: StateDiffFieldSelection


class LogRequest(TypedDict, total=False):
    address: list[str]
    topic0: list[str]
    topic1: list[str]
    topic2: list[str]
    topic3: list[str]
    transaction: bool


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


class _BlocksTable(Table):
    def table_name(self):
        return 'blocks'

    def selection_name(self) -> str:
        return 'block'

    def key(self):
        return 'number', 'hash', 'parentHash'

    def project(self, fields: dict, prefix=''):
        def rewrite_timestamp(f: str):
            if f == 'timestamp':
                return 'timestamp', f'epoch({prefix}timestamp)'
            else:
                return f

        return json_project(
            map(rewrite_timestamp, self.get_selected_fields(fields)),
            prefix=prefix
        )

    def field_weights(self) -> dict[str, int]:
        return {
            'logsBloom': 10
        }


class _TransactionsTable(Table):
    def table_name(self):
        return 'transactions'

    def selection_name(self) -> str:
        return 'transaction'

    def primary_key(self):
        return ('transactionIndex',)

    def field_weights(self):
        return {
            'input': 4
        }

    def where(self, builder: Builder, req: TxRequest):
        yield builder.in_condition('"to"', req.get('to'))
        yield builder.in_condition('"from"', req.get('from'))
        yield builder.in_condition('sighash', req.get('sighash'))


class _LogsTable(Table):
    def table_name(self):
        return 'logs'

    def selection_name(self) -> str:
        return 'log'

    def primary_key(self):
        return ('logIndex',)

    def key(self):
        return 'logIndex', 'transactionIndex'

    def field_weights(self):
        return {
            'data': 4,
            'input': 4
        }

    def project(self, fields: dict, prefix: str = ''):
        def rewrite_topics(f: str):
            if f == 'topics':
                p = prefix
                return 'topics', f'[topic for topic in list_value({p}topic0, {p}topic1, {p}topic2, {p}topic3) ' \
                                 f'if topic is not null]'
            else:
                return f

        return json_project(
            map(rewrite_topics, self.get_selected_fields(fields)),
            prefix=prefix
        )

    def where(self, builder: Builder, req: LogRequest):
        yield builder.in_condition('address', req.get('address'))
        yield builder.in_condition('topic0', req.get('topic0'))
        yield builder.in_condition('topic1', req.get('topic1'))
        yield builder.in_condition('topic2', req.get('topic2'))
        yield builder.in_condition('topic3', req.get('topic3'))


class _TracesTable(Table):
    def table_name(self):
        return 'traces'

    def selection_name(self) -> str:
        return 'trace'

    def primary_key(self):
        return 'transactionIndex', 'traceAddress'

    def key(self):
        return 'transactionIndex', 'traceAddress', 'type'

    def where(self, builder: Builder, req: TraceRequest):
        yield builder.in_condition('type', req.get('type'))
        yield builder.in_condition('create_from', req.get('createFrom'))
        yield builder.in_condition('call_from', req.get('callFrom'))
        yield builder.in_condition('call_to', req.get('callTo'))
        yield builder.in_condition('call_sighash', req.get('callSighash'))
        yield builder.in_condition('suicide_refund_address', req.get('suicideRefundAddress'))
        yield builder.in_condition('reward_author', req.get('rewardAuthor'))

    def project(self, fields: dict, prefix: str = ''):
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
                    ext.append(('action', _trace_topic_projection(prefix, topic, action_fields)))

                if result_fields:
                    ext.append(('result', _trace_topic_projection(prefix, f'{topic}Result', result_fields)))

                proj = itertools.chain(rest_fields, ext)

                cases.append(
                    f"WHEN {prefix}type='{topic}' THEN {json_project(proj, prefix)}"
                )

            when_exps = ' '.join(cases)
            exp = f'CASE {when_exps} ELSE {json_project(rest_fields, prefix)} END'
            if create_result_fields or call_result_fields:
                return f"list_transform([{exp}], o -> " \
                       "CASE len(json_keys(o, '$.result')) " \
                       "WHEN 0 THEN json_merge_patch(o, '{\"result\": null}') " \
                       "ELSE o END" \
                       ")[1]"
            else:
                return exp
        else:
            return json_project(rest_fields, prefix)


def _trace_topic_projection(prefix: str, topic: str, topic_fields: list[str]) -> str:
    assert topic_fields
    components = []
    for f in topic_fields:
        alias = remove_camel_prefix(f, topic)
        ref = f'{prefix}"{to_snake_case(f)}"'
        components.append(
            f"CASE WHEN {ref} is null THEN "
            "'{}'::json ELSE "
            f"json_object('{alias}', {ref}) END"
        )
    if len(components) == 1:
        return components[0]
    else:
        return f'json_merge_patch({", ".join(components)})'


class _StatediffsTable(Table):
    def table_name(self):
        return 'statediffs'

    def request_name(self):
        return 'stateDiffs'

    def selection_name(self) -> str:
        return 'stateDiff'

    def primary_key(self):
        return 'transactionIndex', 'address', 'key'

    def key(self):
        return 'transactionIndex', 'address', 'key', 'kind'

    def where(self, builder: Builder, req: StateDiffRequest):
        yield builder.in_condition('address', req.get('address'))
        yield builder.in_condition('key', req.get('key'))
        yield builder.in_condition('kind', req.get('kind'))


def _build_model():
    blocks = _BlocksTable()
    transactions = _TransactionsTable()
    logs = _LogsTable()
    traces = _TracesTable()
    statediffs = _StatediffsTable()

    statediffs.relations.extend([
        JoinRel(
            table=transactions,
            include_flag_name='stateDiffs',
            join_condition='s.transaction_index = r.transaction_index'
        )
    ])

    traces.relations.extend([
        JoinRel(
            table=transactions,
            include_flag_name='traces',
            join_condition='s.transaction_index = r.transaction_index'
        ),
        JoinRel(
            table=traces,
            include_flag_name='subtraces',
            join_condition='s.transaction_index = r.transaction_index AND '
                           'len(s.trace_address) > len(r.trace_address) AND '
                           's.trace_address[1:len(r.trace_address)] = r.trace_address'
        ),
        JoinRel(
            table=traces,
            include_flag_name='parents',
            join_condition='s.transaction_index = r.transaction_index AND '
                           'len(s.trace_address) < len(r.trace_address) AND '
                           's.trace_address = r.trace_address[1:len(s.trace_address)]'
        )
    ])

    logs.relations.extend([
        JoinRel(
            table=traces,
            include_flag_name='logs',
            join_condition='s.transaction_index = r.transaction_index'
        )
    ])

    transactions.relations.extend([
        RefRel(
            table=logs,
            include_flag_name='transaction',
            key=['transaction_index']
        ),
        RefRel(
            table=traces,
            include_flag_name='transaction',
            key=['transaction_index']
        ),
        RefRel(
            table=statediffs,
            include_flag_name='transaction',
            key=['transaction_index']
        )
    ])

    return [
        blocks,
        transactions,
        logs,
        traces,
        statediffs
    ]


MODEL = _build_model()
