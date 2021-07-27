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
