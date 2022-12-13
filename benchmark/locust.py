from locust import HttpUser, task, constant


class SquidUser(HttpUser):
    wait_time = constant(1)

    @task
    def query(self):
        last_block = None
        while True:
            response = self.client.post('/query/czM6Ly9ldGhhLW1haW5uZXQtc2lh', json={
                'fromBlock': 0,
                'toBlock': last_block,
                'fields': {
                    'block': {'timestamp': True},
                },
                'logs': [{'address': ['0x8d12a197cb00d4747a1fe03395095ce2a5cc6819']}]
            })
            last_block_ = int(response.headers['x-sqd-last-processed-block'])
            if last_block == last_block_:
                break
            last_block = last_block_
