# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: messages.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0emessages.proto\x12\x08messages\x1a\x1bgoogle/protobuf/empty.proto\"\xac\x03\n\x08\x45nvelope\x12\x1e\n\x04ping\x18\x01 \x01(\x0b\x32\x0e.messages.PingH\x00\x12\x1e\n\x04pong\x18\x02 \x01(\x0b\x32\x0e.messages.PongH\x00\x12 \n\x05query\x18\x05 \x01(\x0b\x32\x0f.messages.QueryH\x00\x12-\n\x0cquery_result\x18\x06 \x01(\x0b\x32\x15.messages.QueryResultH\x00\x12+\n\rdataset_state\x18\x07 \x01(\x0b\x32\x12.messages.RangeSetH\x00\x12\x33\n\x0fquery_submitted\x18\x08 \x01(\x0b\x32\x18.messages.QuerySubmittedH\x00\x12\x31\n\x0equery_finished\x18\t \x01(\x0b\x32\x17.messages.QueryFinishedH\x00\x12)\n\nquery_logs\x18\n \x01(\x0b\x32\x13.messages.QueryLogsH\x00\x12\x31\n\x0fget_next_seq_no\x18\x0b \x01(\x0b\x32\x16.google.protobuf.EmptyH\x00\x12\x15\n\x0bnext_seq_no\x18\x0c \x01(\rH\x00\x42\x05\n\x03msg\"#\n\x05Range\x12\r\n\x05\x62\x65gin\x18\x01 \x01(\r\x12\x0b\n\x03\x65nd\x18\x02 \x01(\r\"+\n\x08RangeSet\x12\x1f\n\x06ranges\x18\x01 \x03(\x0b\x32\x0f.messages.Range\"\x89\x01\n\x0bWorkerState\x12\x35\n\x08\x64\x61tasets\x18\x01 \x03(\x0b\x32#.messages.WorkerState.DatasetsEntry\x1a\x43\n\rDatasetsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12!\n\x05value\x18\x02 \x01(\x0b\x32\x12.messages.RangeSet:\x02\x38\x01\"\x9c\x01\n\x04Ping\x12\x11\n\tworker_id\x18\x01 \x01(\t\x12\x12\n\nworker_url\x18\x02 \x01(\t\x12$\n\x05state\x18\x03 \x01(\x0b\x32\x15.messages.WorkerState\x12\r\n\x05pause\x18\x04 \x01(\x08\x12\x14\n\x0cstored_bytes\x18\x05 \x01(\x04\x12\x0f\n\x07version\x18\x06 \x01(\t\x12\x11\n\tsignature\x18\x07 \x01(\x0c\"\xdf\x01\n\x04Pong\x12\x11\n\tping_hash\x18\x01 \x01(\x0c\x12\x30\n\x0enot_registered\x18\x02 \x01(\x0b\x32\x16.google.protobuf.EmptyH\x00\x12\x35\n\x13unsupported_version\x18\x03 \x01(\x0b\x32\x16.google.protobuf.EmptyH\x00\x12(\n\x06jailed\x18\x04 \x01(\x0b\x32\x16.google.protobuf.EmptyH\x00\x12\'\n\x06\x61\x63tive\x18\x05 \x01(\x0b\x32\x15.messages.WorkerStateH\x00\x42\x08\n\x06status\"L\n\x05Query\x12\x10\n\x08query_id\x18\x01 \x01(\t\x12\x0f\n\x07\x64\x61taset\x18\x02 \x01(\t\x12\r\n\x05query\x18\x03 \x01(\t\x12\x11\n\tprofiling\x18\x04 \x01(\x08\"z\n\x0bQueryResult\x12\x10\n\x08query_id\x18\x01 \x01(\t\x12 \n\x02ok\x18\x02 \x01(\x0b\x32\x12.messages.OkResultH\x00\x12\x15\n\x0b\x62\x61\x64_request\x18\x03 \x01(\tH\x00\x12\x16\n\x0cserver_error\x18\x04 \x01(\tH\x00\x42\x08\n\x06result\">\n\x08OkResult\x12\x0c\n\x04\x64\x61ta\x18\x01 \x01(\x0c\x12\x16\n\texec_plan\x18\x02 \x01(\x0cH\x00\x88\x01\x01\x42\x0c\n\n_exec_plan\"|\n\x0eQuerySubmitted\x12\x11\n\tclient_id\x18\x01 \x01(\t\x12\x11\n\tworker_id\x18\x02 \x01(\t\x12\x10\n\x08query_id\x18\x03 \x01(\t\x12\x0f\n\x07\x64\x61taset\x18\x04 \x01(\t\x12\r\n\x05query\x18\x05 \x01(\t\x12\x12\n\nquery_hash\x18\x06 \x01(\x0c\"\xe6\x01\n\rQueryFinished\x12\x11\n\tclient_id\x18\x01 \x01(\t\x12\x11\n\tworker_id\x18\x02 \x01(\t\x12\x10\n\x08query_id\x18\x03 \x01(\t\x12\x14\n\x0c\x65xec_time_ms\x18\x04 \x01(\r\x12#\n\x02ok\x18\x05 \x01(\x0b\x32\x15.messages.SizeAndHashH\x00\x12\x15\n\x0b\x62\x61\x64_request\x18\x06 \x01(\tH\x00\x12\x16\n\x0cserver_error\x18\x07 \x01(\tH\x00\x12)\n\x07timeout\x18\x08 \x01(\x0b\x32\x16.google.protobuf.EmptyH\x00\x42\x08\n\x06result\"\xf0\x01\n\rQueryExecuted\x12\x11\n\tclient_id\x18\x01 \x01(\t\x12\x11\n\tworker_id\x18\x02 \x01(\t\x12\x1e\n\x05query\x18\x03 \x01(\x0b\x32\x0f.messages.Query\x12\x12\n\nquery_hash\x18\x05 \x01(\x0c\x12\x14\n\x0c\x65xec_time_ms\x18\x06 \x01(\r\x12&\n\x02ok\x18\x07 \x01(\x0b\x32\x18.messages.InputAndOutputH\x00\x12\x15\n\x0b\x62\x61\x64_request\x18\x08 \x01(\tH\x00\x12\x16\n\x0cserver_error\x18\t \x01(\tH\x00\x12\x0e\n\x06seq_no\x18\n \x01(\rB\x08\n\x06result\">\n\tQueryLogs\x12\x31\n\x10queries_executed\x18\x01 \x03(\x0b\x32\x17.messages.QueryExecuted\"P\n\x0eInputAndOutput\x12\x17\n\x0fnum_read_chunks\x18\x01 \x01(\r\x12%\n\x06output\x18\x02 \x01(\x0b\x32\x15.messages.SizeAndHash\"-\n\x0bSizeAndHash\x12\x0c\n\x04size\x18\x01 \x01(\r\x12\x10\n\x08sha3_256\x18\x02 \x01(\x0c\x62\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'messages_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _WORKERSTATE_DATASETSENTRY._options = None
  _WORKERSTATE_DATASETSENTRY._serialized_options = b'8\001'
  _ENVELOPE._serialized_start=58
  _ENVELOPE._serialized_end=486
  _RANGE._serialized_start=488
  _RANGE._serialized_end=523
  _RANGESET._serialized_start=525
  _RANGESET._serialized_end=568
  _WORKERSTATE._serialized_start=571
  _WORKERSTATE._serialized_end=708
  _WORKERSTATE_DATASETSENTRY._serialized_start=641
  _WORKERSTATE_DATASETSENTRY._serialized_end=708
  _PING._serialized_start=711
  _PING._serialized_end=867
  _PONG._serialized_start=870
  _PONG._serialized_end=1093
  _QUERY._serialized_start=1095
  _QUERY._serialized_end=1171
  _QUERYRESULT._serialized_start=1173
  _QUERYRESULT._serialized_end=1295
  _OKRESULT._serialized_start=1297
  _OKRESULT._serialized_end=1359
  _QUERYSUBMITTED._serialized_start=1361
  _QUERYSUBMITTED._serialized_end=1485
  _QUERYFINISHED._serialized_start=1488
  _QUERYFINISHED._serialized_end=1718
  _QUERYEXECUTED._serialized_start=1721
  _QUERYEXECUTED._serialized_end=1961
  _QUERYLOGS._serialized_start=1963
  _QUERYLOGS._serialized_end=2025
  _INPUTANDOUTPUT._serialized_start=2027
  _INPUTANDOUTPUT._serialized_end=2107
  _SIZEANDHASH._serialized_start=2109
  _SIZEANDHASH._serialized_end=2154
# @@protoc_insertion_point(module_scope)
