import io

import invoke

import arrlio


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
def xrange(count):
    for x in range(count):
        yield x


@arrlio.task
async def add_one(value: str):
    return int(value) + 1


@arrlio.task
async def bash(cmd, stdin: str = None):
    in_stream = io.StringIO(stdin)
    out_stream = io.StringIO()
    result = invoke.run(cmd, in_stream=in_stream, out_stream=out_stream)
    return result.stdout
