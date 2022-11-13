import asyncio

import uvicorn

from .state_manager import StateManager
from .api import create_app


async def main():
    sm = StateManager(
        worker_id='1',
        worker_url='http://localhost:8000',
        data_dir='data',
        router_url='http://localhost:3000'
    )

    app = create_app(sm)
    conf = uvicorn.Config(app)
    server = uvicorn.Server(conf)

    server_task = asyncio.create_task(server.serve(), name='server')
    sm_task = asyncio.create_task(sm.run())

    await asyncio.wait([server_task, sm_task], return_when=asyncio.FIRST_COMPLETED)

    ex = None

    if server_task.done():
        ex = server_task.exception()
        sm_task.cancel()
    else:
        server.should_exit = True
        try:
            await server_task
        except:
            pass

    if sm_task.done():
        ex = sm_task.exception()

    if ex:
        raise ex


if __name__ == '__main__':
    asyncio.run(main())
