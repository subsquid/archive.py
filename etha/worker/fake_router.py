from wsgiref.simple_server import make_server

import falcon


class PingResource:
    def on_post(self, req: falcon.Request, res: falcon.Response):
        res.media = {
            's3://ethereum-mainnet': [
                (9492320, 9509459),
                (16254600, 16272699),
                (17090380, 17098719)
            ],
            's3://binance-mainnet': [
                (5748140, 5756519)
            ]
        }


def main():
    app = falcon.App()
    app.add_route('/ping', PingResource())
    with make_server('', 5555, app) as server:
        print(f'listening on port {server.server_port}')
        server.serve_forever()


if __name__ == '__main__':
    main()
