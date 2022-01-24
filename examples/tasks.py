import io

import arrlio
import invoke


@arrlio.task
async def hello_world():
    return "Hello World!"


@arrlio.task(name="foo")
async def foo():
    arrlio.logger.info("Hello from task 'foo'!")


@arrlio.task(bind=True)
async def bind(self):
    arrlio.logger.info(self.data.task_id)
    arrlio.logger.info(self)


@arrlio.task
async def exception():
    raise ZeroDivisionError


@arrlio.task
async def add_one(value: str, **kwds):
    return int(value) + 1


@arrlio.task
async def bash(cmd, stdin: str = None, **kwds):
    in_stream=io.StringIO(stdin)
    out_stream=io.StringIO()
    result = invoke.run(cmd, in_stream=in_stream, out_stream=out_stream)
    return result.stdout
