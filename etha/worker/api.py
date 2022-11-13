import falcon.asgi as fa

from .state_manager import StateManager


class StatusResource:
    def __init__(self, sm: StateManager):
        self.sm = sm

    async def on_get(self, req: fa.Request, res: fa.Response):
        res.status = '200 OK'
        res.media = self.sm.get_status()


def create_app(sm: StateManager) -> fa.App:
    app = fa.App()
    app.add_route('/status', StatusResource(sm))
    return app
