from wsgiref.simple_server import make_server

import falcon


class PingResource:
    def on_post(self, req: falcon.Request, res: falcon.Response):
        res.media = {
            'desired_state': {
                's3://etha-mainnet-sia': [(16143005, 16149151)]
            }
        }


def main():
    app = falcon.App()
    app.add_route('/ping', PingResource())
    with make_server('', 5555, app) as server:
        print(f'listening on port {server.server_port}')
        server.serve_forever()


if __name__ == '__main__':
    main()
