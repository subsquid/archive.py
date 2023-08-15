from wsgiref.simple_server import make_server


def app(environ, start_response):
    with open('fake_state.temp.json', 'rb') as f:
        payload = f.read()

    start_response("200 OK", [
        ('Content-Type','application/json; charset=utf-8')
    ])

    return [payload]


def main():
    with make_server('', 5555, app) as server:
        print(f'listening on port {server.server_port}')
        server.serve_forever()


if __name__ == '__main__':
    main()
