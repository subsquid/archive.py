# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: messages.proto
# Protobuf Python Version: 4.25.0
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0emessages.proto\x12\x08messages\x1a\x1bgoogle/protobuf/empty.proto\"\xe8\x02\n\x08\x45nvelope\x12\x1e\n\x04pong\x18\x03 \x01(\x0b\x32\x0e.messages.PongH\x00\x12\x1e\n\x04ping\x18\x04 \x01(\x0b\x32\x0e.messages.PingH\x00\x12 \n\x05query\x18\x05 \x01(\x0b\x32\x0f.messages.QueryH\x00\x12-\n\x0cquery_result\x18\x06 \x01(\x0b\x32\x15.messages.QueryResultH\x00\x12\x33\n\x0fquery_submitted\x18\x08 \x01(\x0b\x32\x18.messages.QuerySubmittedH\x00\x12\x31\n\x0equery_finished\x18\t \x01(\x0b\x32\x17.messages.QueryFinishedH\x00\x12)\n\nquery_logs\x18\x0b \x01(\x0b\x32\x13.messages.QueryLogsH\x00\x12\x31\n\x0elogs_collected\x18\x0c \x01(\x0b\x32\x17.messages.LogsCollectedH\x00\x42\x05\n\x03msg\"#\n\x05Range\x12\r\n\x05\x62\x65gin\x18\x01 \x01(\r\x12\x0b\n\x03\x65nd\x18\x02 \x01(\r\"+\n\x08RangeSet\x12\x1f\n\x06ranges\x18\x01 \x03(\x0b\x32\x0f.messages.Range\"\x89\x01\n\x0bWorkerState\x12\x35\n\x08\x64\x61tasets\x18\x01 \x03(\x0b\x32#.messages.WorkerState.DatasetsEntry\x1a\x43\n\rDatasetsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12!\n\x05value\x18\x02 \x01(\x0b\x32\x12.messages.RangeSet:\x02\x38\x01\"=\n\rDatasetRanges\x12\x0b\n\x03url\x18\x01 \x01(\t\x12\x1f\n\x06ranges\x18\x02 \x03(\x0b\x32\x0f.messages.Range\"\xbd\x01\n\x04Ping\x12\x16\n\tworker_id\x18\x01 \x01(\tH\x00\x88\x01\x01\x12\x14\n\x07version\x18\x02 \x01(\tH\x01\x88\x01\x01\x12\x19\n\x0cstored_bytes\x18\x03 \x01(\x04H\x02\x88\x01\x01\x12.\n\rstored_ranges\x18\x04 \x03(\x0b\x32\x17.messages.DatasetRanges\x12\x11\n\tsignature\x18\x05 \x01(\x0c\x42\x0c\n\n_worker_idB\n\n\x08_versionB\x0f\n\r_stored_bytes\"\xf8\x01\n\x04Pong\x12\x11\n\tping_hash\x18\x01 \x01(\x0c\x12\x30\n\x0enot_registered\x18\x02 \x01(\x0b\x32\x16.google.protobuf.EmptyH\x00\x12\x35\n\x13unsupported_version\x18\x03 \x01(\x0b\x32\x16.google.protobuf.EmptyH\x00\x12/\n\tjailed_v1\x18\x04 \x01(\x0b\x32\x16.google.protobuf.EmptyB\x02\x18\x01H\x00\x12\'\n\x06\x61\x63tive\x18\x05 \x01(\x0b\x32\x15.messages.WorkerStateH\x00\x12\x10\n\x06jailed\x18\x06 \x01(\tH\x00\x42\x08\n\x06status\"\xda\x01\n\x05Query\x12\x15\n\x08query_id\x18\x01 \x01(\tH\x00\x88\x01\x01\x12\x14\n\x07\x64\x61taset\x18\x02 \x01(\tH\x01\x88\x01\x01\x12\x12\n\x05query\x18\x03 \x01(\tH\x02\x88\x01\x01\x12\x16\n\tprofiling\x18\x04 \x01(\x08H\x03\x88\x01\x01\x12\x1e\n\x11\x63lient_state_json\x18\x05 \x01(\tH\x04\x88\x01\x01\x12\x11\n\tsignature\x18\x06 \x01(\x0c\x42\x0b\n\t_query_idB\n\n\x08_datasetB\x08\n\x06_queryB\x0c\n\n_profilingB\x14\n\x12_client_state_json\"\xab\x01\n\x0bQueryResult\x12\x10\n\x08query_id\x18\x01 \x01(\t\x12 \n\x02ok\x18\x02 \x01(\x0b\x32\x12.messages.OkResultH\x00\x12\x15\n\x0b\x62\x61\x64_request\x18\x03 \x01(\tH\x00\x12\x16\n\x0cserver_error\x18\x04 \x01(\tH\x00\x12/\n\rno_allocation\x18\x05 \x01(\x0b\x32\x16.google.protobuf.EmptyH\x00\x42\x08\n\x06result\">\n\x08OkResult\x12\x0c\n\x04\x64\x61ta\x18\x01 \x01(\x0c\x12\x16\n\texec_plan\x18\x02 \x01(\x0cH\x00\x88\x01\x01\x42\x0c\n\n_exec_plan\"|\n\x0eQuerySubmitted\x12\x11\n\tclient_id\x18\x01 \x01(\t\x12\x11\n\tworker_id\x18\x02 \x01(\t\x12\x10\n\x08query_id\x18\x03 \x01(\t\x12\x0f\n\x07\x64\x61taset\x18\x04 \x01(\t\x12\r\n\x05query\x18\x05 \x01(\t\x12\x12\n\nquery_hash\x18\x06 \x01(\x0c\"\x97\x02\n\rQueryFinished\x12\x11\n\tclient_id\x18\x01 \x01(\t\x12\x11\n\tworker_id\x18\x02 \x01(\t\x12\x10\n\x08query_id\x18\x03 \x01(\t\x12\x14\n\x0c\x65xec_time_ms\x18\x04 \x01(\r\x12#\n\x02ok\x18\x05 \x01(\x0b\x32\x15.messages.SizeAndHashH\x00\x12\x15\n\x0b\x62\x61\x64_request\x18\x06 \x01(\tH\x00\x12\x16\n\x0cserver_error\x18\x07 \x01(\tH\x00\x12)\n\x07timeout\x18\x08 \x01(\x0b\x32\x16.google.protobuf.EmptyH\x00\x12/\n\rno_allocation\x18\t \x01(\x0b\x32\x16.google.protobuf.EmptyH\x00\x42\x08\n\x06result\"\xd5\x02\n\rQueryExecuted\x12\x11\n\tclient_id\x18\x01 \x01(\t\x12\x11\n\tworker_id\x18\x02 \x01(\t\x12\x1e\n\x05query\x18\x03 \x01(\x0b\x32\x0f.messages.Query\x12\x12\n\nquery_hash\x18\x05 \x01(\x0c\x12\x19\n\x0c\x65xec_time_ms\x18\x06 \x01(\rH\x01\x88\x01\x01\x12&\n\x02ok\x18\x07 \x01(\x0b\x32\x18.messages.InputAndOutputH\x00\x12\x15\n\x0b\x62\x61\x64_request\x18\x08 \x01(\tH\x00\x12\x16\n\x0cserver_error\x18\t \x01(\tH\x00\x12\x13\n\x06seq_no\x18\n \x01(\x04H\x02\x88\x01\x01\x12\x19\n\x0ctimestamp_ms\x18\x0b \x01(\x04H\x03\x88\x01\x01\x12\x11\n\tsignature\x18\x0c \x01(\x0c\x42\x08\n\x06resultB\x0f\n\r_exec_time_msB\t\n\x07_seq_noB\x0f\n\r_timestamp_ms\">\n\tQueryLogs\x12\x31\n\x10queries_executed\x18\x01 \x03(\x0b\x32\x17.messages.QueryExecuted\"i\n\x0eInputAndOutput\x12\x1c\n\x0fnum_read_chunks\x18\x01 \x01(\rH\x00\x88\x01\x01\x12%\n\x06output\x18\x02 \x01(\x0b\x32\x15.messages.SizeAndHashB\x12\n\x10_num_read_chunks\";\n\x0bSizeAndHash\x12\x11\n\x04size\x18\x01 \x01(\rH\x00\x88\x01\x01\x12\x10\n\x08sha3_256\x18\x02 \x01(\x0c\x42\x07\n\x05_size\"\x8f\x01\n\rLogsCollected\x12\x46\n\x10sequence_numbers\x18\x01 \x03(\x0b\x32,.messages.LogsCollected.SequenceNumbersEntry\x1a\x36\n\x14SequenceNumbersEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x04:\x02\x38\x01\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'messages_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_WORKERSTATE_DATASETSENTRY']._options = None
  _globals['_WORKERSTATE_DATASETSENTRY']._serialized_options = b'8\001'
  _globals['_PONG'].fields_by_name['jailed_v1']._options = None
  _globals['_PONG'].fields_by_name['jailed_v1']._serialized_options = b'\030\001'
  _globals['_LOGSCOLLECTED_SEQUENCENUMBERSENTRY']._options = None
  _globals['_LOGSCOLLECTED_SEQUENCENUMBERSENTRY']._serialized_options = b'8\001'
  _globals['_ENVELOPE']._serialized_start=58
  _globals['_ENVELOPE']._serialized_end=418
  _globals['_RANGE']._serialized_start=420
  _globals['_RANGE']._serialized_end=455
  _globals['_RANGESET']._serialized_start=457
  _globals['_RANGESET']._serialized_end=500
  _globals['_WORKERSTATE']._serialized_start=503
  _globals['_WORKERSTATE']._serialized_end=640
  _globals['_WORKERSTATE_DATASETSENTRY']._serialized_start=573
  _globals['_WORKERSTATE_DATASETSENTRY']._serialized_end=640
  _globals['_DATASETRANGES']._serialized_start=642
  _globals['_DATASETRANGES']._serialized_end=703
  _globals['_PING']._serialized_start=706
  _globals['_PING']._serialized_end=895
  _globals['_PONG']._serialized_start=898
  _globals['_PONG']._serialized_end=1146
  _globals['_QUERY']._serialized_start=1149
  _globals['_QUERY']._serialized_end=1367
  _globals['_QUERYRESULT']._serialized_start=1370
  _globals['_QUERYRESULT']._serialized_end=1541
  _globals['_OKRESULT']._serialized_start=1543
  _globals['_OKRESULT']._serialized_end=1605
  _globals['_QUERYSUBMITTED']._serialized_start=1607
  _globals['_QUERYSUBMITTED']._serialized_end=1731
  _globals['_QUERYFINISHED']._serialized_start=1734
  _globals['_QUERYFINISHED']._serialized_end=2013
  _globals['_QUERYEXECUTED']._serialized_start=2016
  _globals['_QUERYEXECUTED']._serialized_end=2357
  _globals['_QUERYLOGS']._serialized_start=2359
  _globals['_QUERYLOGS']._serialized_end=2421
  _globals['_INPUTANDOUTPUT']._serialized_start=2423
  _globals['_INPUTANDOUTPUT']._serialized_end=2528
  _globals['_SIZEANDHASH']._serialized_start=2530
  _globals['_SIZEANDHASH']._serialized_end=2589
  _globals['_LOGSCOLLECTED']._serialized_start=2592
  _globals['_LOGSCOLLECTED']._serialized_end=2735
  _globals['_LOGSCOLLECTED_SEQUENCENUMBERSENTRY']._serialized_start=2681
  _globals['_LOGSCOLLECTED_SEQUENCENUMBERSENTRY']._serialized_end=2735
# @@protoc_insertion_point(module_scope)
