from typing import Iterable, TypedDict
from pyarrow.dataset import Expression
import marshmallow as mm
from sqa.query.schema import field_map_schema

from sqa.query.model import FieldSelection, Item, JoinRel, Model, RefRel, Scan, Table
from sqa.query.schema import BaseQuerySchema
from sqa.query.util import field_gte, field_in, field_lte, get_selected_fields, json_project


class StarknetBlockFieldSelection(TypedDict, total=False):
    number: bool
    hash: bool
    parentHash: bool

    status: bool
    newRoot: bool
    timestamp: bool
    sequencerAddress: bool

class StarknetTxFieldSelection(TypedDict, total=False):
    transactionIndex: bool
    transactionHash: bool

    contractAddress: bool
    entryPointSelector: bool
    calldata: bool
    maxFee: bool
    type: bool
    senderAddress: bool
    version: bool
    signature: bool
    nonce: bool
    classHash: bool
    compiledClassHash: bool
    contractAddressSalt: bool
    constructorCalldata: bool

class StarknetEventFieldSelection(TypedDict, total=False):
    eventIndex: bool

    fromAddress: bool
    keys: bool
    data: bool

class TxRequest(TypedDict, total=False):
    contractAddress: list[str]
    type: list[str]
    firstNonce: int
    lastNonce: int

class EventRequest(TypedDict, total=False):
    fromAddress: list[str]

class _FieldSelectionSchema(mm.Schema):
    block = field_map_schema(StarknetBlockFieldSelection)
    transaction = field_map_schema(StarknetTxFieldSelection)
    event = field_map_schema(StarknetEventFieldSelection)


class _StarknetTxRequestSchema(mm.Schema):
    contractAddress = mm.fields.List(mm.fields.Str())
    type = mm.fields.List(mm.fields.Str())
    senderAddress = mm.fields.List(mm.fields.Str())
    firstNonce = mm.fields.Integer(
        strict=True,
        validate=mm.validate.Range(min=0, min_inclusive=True)
    ),
    lastNonce = mm.fields.Integer(
        strict=True,
        validate=mm.validate.Range(min=0, min_inclusive=True)
    )
    # TODO: traces = mm.fields.Boolean()
    #stateDiffs = mm.fields.Boolean()

class _StarknetEventRequestSchema(mm.Schema):
    fromAddress = mm.fields.List(mm.fields.Str())


class _QuerySchema(BaseQuerySchema):
    fields = mm.fields.Nested(_FieldSelectionSchema())
    transactions = mm.fields.List(mm.fields.Nested(_StarknetTxRequestSchema()))
    events = mm.fields.List(mm.fields.Nested(_StarknetEventRequestSchema()))


QUERY_SCHEMA = _QuerySchema()


# NOTE: In eth column weights were used
_blocks_table = Table(
    name='blocks',
    primary_key=['number'],
)

_tx_table = Table(
    name='transactions',
    primary_key=['transaction_index'],
)

_events_table = Table(
    name='events',
    primary_key=['transaction_index', 'event_index'],
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
        yield field_in('contract_address', req.get('contractAddress'))
        yield field_in('sender_address', req.get('contractAddress'))
        yield field_in('type', req.get('type'))
        yield field_gte('nonce', req.get('firstNonce'))
        yield field_lte('nonce', req.get('lastNonce'))


class _TxItem(Item):
    def table(self) -> Table:
        return _tx_table

    def name(self) -> str:
        return 'transactions'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('transaction'), ['_idx', 'transactionIndex'])


class _EventScan(Scan):
    def table(self) -> Table:
        return _events_table

    def request_name(self) -> str:
        return 'events'

    def where(self, req: EventRequest) -> Iterable[Expression | None]:
        yield field_in('from_address', req.get('fromAddress'))


class _EventItem(Item):
    def table(self) -> Table:
        return _events_table

    def name(self) -> str:
        return 'events'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('event'), ['_idx', 'transactionIndex', 'eventIndex'])


def _build_model() -> Model:
    tx_scan = _TxScan()
    event_scan = _EventScan()

    block_item = _BlockItem()
    tx_item = _TxItem()
    event_item = _EventItem()
    # TODO: tracescan
    # TODO: receipts

    event_item.sources.extend([
        event_scan,
        JoinRel(
            scan=tx_scan,
            include_flag_name='events',
            query='SELECT * FROM events i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_hash = s.transaction_hash'
        )
    ])

    tx_item.sources.extend([
        tx_scan,
        RefRel(
            scan=event_scan,
            include_flag_name='transactions',
            scan_columns=['transaction_hash']
        ),
        # TODO: RefRel for traces and statediffs
    ])

    return [tx_scan, event_scan, block_item, tx_item, event_item]

MODEL = _build_model()