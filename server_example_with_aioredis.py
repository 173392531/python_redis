import asyncio
import json
import logging
import aioredis
from aioredis.errors import ReplyError
from aioredis.commands import TransactionsCommandsMixin, StringCommandsMixin, ListCommandsMixin, PubSubCommandsMixin
from aioredis.connection import create_connection
from aioredis.pool import create_pool


logger = logging.getLogger(__name__)


class CommandError(Exception): pass


class RedisProtocol(asyncio.Protocol, TransactionsCommandsMixin, StringCommandsMixin, ListCommandsMixin, PubSubCommandsMixin):
    def __init__(self, password=None):
        self.password = password
        self.transport = None
        self.connection = None

    def connection_made(self, transport):
        self.transport = transport
        self.connection = create_connection(transport)
        if self.password:
            self.connection.auth(self.password)

    def data_received(self, data):
        self.connection.feed_data(data)

    def eof_received(self):
        self.connection.close()
        logger.info('Client went away')

    def connection_lost(self, exc):
        self.connection.close()
        logger.info('Client went away')


class RedisServer:
    def __init__(self, host='127.0.0.1', port=6379, password=None, db=0, pool_size=10):
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        self.pool_size = pool_size
        self.server = None
        self.pool = None

    async def get_connection(self):
        if self.pool is None:
            self.pool = await create_pool(
                (self.host, self.port),
                db=self.db,
                password=self.password,
                minsize=1,
                maxsize=self.pool_size)
        return await self.pool.acquire()

    async def release_connection(self, connection):
        self.pool.release(connection)

    async def handle_client(self, reader, writer):
        logger.info('Client connected')
        protocol = RedisProtocol(self.password)
        try:
            while True:
                data = await reader.readline()
                if not data:
                    break
                protocol.data_received(data)
                try:
                    result = await protocol.connection.read_response()
                except ReplyError as exc:
                    writer.write(f"-{str(exc)}\r\n".encode())
                else:
                    writer.write(f"+{json.dumps(result)}\r\n".encode())
                await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            protocol.connection_lost(None)

    async def run(self):
        self.server = await asyncio.start_server(self.handle_client, self.host, self.port)
        logger.info(f'Server started on {self.host}:{self.port}')
        async with self.server:
            await self.server.serve_forever()


class RedisClient:
    def __init__(self, host='127.0.0.1', port=6379, password=None, db=0):
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        self.connection = None

    async def connect(self):
        self.connection = await aioredis.create_redis(
            (self.host, self.port),
            db=self.db,
            password=self.password)

    async def execute(self, command, *args):
        if not self.connection:
            await self.connect()
        return await getattr(self.connection, command)(*args)

    async def close(self):
        if self.connection:
            self.connection.close()
            await self.connection.wait_closed()


async def main():
    server = RedisServer()
    await server.run()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())