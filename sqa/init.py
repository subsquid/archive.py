
def init_logging():
    from sqa.util.log import init_logging
    init_logging()


def init_uvloop():
    import asyncio
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
