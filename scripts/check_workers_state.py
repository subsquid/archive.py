import subprocess
from sqa.worker.state.dataset import dataset_decode
import requests


def main():
    workers = [
        ('worker-0', '148.113.153.164'),
        ('lm01', '141.95.97.109'),
        ('lm02', '162.19.169.196'),
        ('lm03', '162.19.169.199'),
        ('lm04', '162.19.169.200'),
        ('lm05', '162.19.169.201'),
        ('rb01', '37.59.18.185'),
        ('rb02', '141.95.172.117'),
        ('rb03', '57.128.97.235'),
        ('rb04', '37.59.18.186'),
        ('rb05', '37.59.18.187'),
        ('rb06', '162.19.99.159'),
        ('rbx01', '51.77.21.115'),
        ('rbx02', '51.77.21.116'),
        ('rbx03', '51.77.21.117'),
        ('rbx04', '51.77.21.118'),
        ('rbx05', '54.38.113.22'),
        ('rbx06', '57.128.116.86'),
        ('rbx07', '141.94.197.162'),
        ('rbx08', '141.94.241.94'),
        ('sb01', '141.94.142.12'),
        ('gr01', '135.125.67.218'),
        ('gr02', '135.125.67.219'),
        ('gr03', '135.125.67.217')
    ]
    for (name, address) in workers:
        print('processing', name)
        res = subprocess.run(f'ssh sa@{address} "ls /var/sqa/data/"', shell=True, check=True, text=True, capture_output=True)
        datasets = []
        for line in res.stdout.splitlines():
            dataset = dataset_decode(line)
            datasets.append(dataset)

        response = requests.get(f'https://{name}.sqd-archive.net/worker/status')
        data = response.json()
        available = list(data['state']['available'].keys())

        datasets.sort()
        available.sort()
        assert datasets == available, name


if __name__ == '__main__':
    main()
