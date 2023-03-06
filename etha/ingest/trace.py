from etha.ingest.model import Trace


def extract_trace_fields(trace: Trace) -> dict:
    if trace['type'] == 'create':
        fields = {
            'create_from': trace['action']['from'],
            'create_gas': trace['action']['gas'],
            'create_init': trace['action']['init'],
            'create_value': trace['action']['value'],
        }
        if 'error' not in trace:
            fields.update({
                'create_address': trace['result']['address'],
                'create_code': trace['result']['code'],
                'create_gas_used': trace['result']['gasUsed'],
            })
        return fields
    elif trace['type'] == 'call':
        fields = {
            'call_from': trace['action']['from'],
            'call_type': trace['action']['callType'],
            'call_gas': trace['action']['gas'],
            'call_input': trace['action']['input'],
            'call_to': trace['action']['to'],
            'call_value': trace['action']['value'],
        }
        if 'error' not in trace:
            fields.update({
                'call_gas_used': trace['result']['gasUsed'],
                'call_output': trace['result']['output'],
            })
        return fields
    elif trace['type'] == 'suicide':
        return {
            'suicide_address': trace['action']['address'],
            'suicide_refund_address': trace['action']['refundAddress'],
            'suicide_balance': trace['action']['balance'],
        }
    elif trace['type'] == 'reward':
        return {
            'reward_author': trace['action']['author'],
            'reward_type': trace['action']['rewardType'],
            'reward_value': trace['action']['value'],
        }
    else:
        raise ValueError(f"unhandled trace type '{trace['type']}'")


def parse_action(trace: dict):
    if trace['type'] == 'create':
        return {
            'from': trace['action']['from'],
            'gas': int(trace['action']['gas'], 0),
            'init': trace['action']['init'],
            'value': int(trace['action']['value'], 0),
        }
    elif trace['type'] == 'call':
        return {
            'from': trace['action']['from'],
            'callType': trace['action']['callType'],
            'gas': int(trace['action']['gas'], 0),
            'input': trace['action']['input'],
            'to': trace['action']['to'],
            'value': int(trace['action']['value'], 0),
        }
    elif trace['type'] == 'suicide':
        return {
            'address': trace['action']['address'],
            'refundAddress': trace['action']['refundAddress'],
            'balance': int(trace['action']['balance'], 0),
        }
    elif trace['type'] == 'reward':
        return {
            'author': trace['action']['author'],
            'rewardType': trace['action']['rewardType'],
            'value': int(trace['action']['value'], 0),
        }
    else:
        raise ValueError(f"unhandled trace type '{trace['type']}'")


def parse_result(trace: dict):
    if trace['type'] == 'create':
        if 'error' not in trace:
            return {
                'address': trace['result']['address'],
                'code': trace['result']['code'],
                'gasUsed': int(trace['result']['gasUsed'], 0),
            }
    elif trace['type'] == 'call':
        if 'error' not in trace:
            return {
                'gasUsed': int(trace['result']['gasUsed'], 0),
                'output': trace['result']['output'],
            }
    elif trace['type'] == 'suicide':
        return None
    elif trace['type'] == 'reward':
        return None
    else:
        raise ValueError(f"unhandled trace type '{trace['type']}'")
