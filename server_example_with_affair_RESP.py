import asyncio
import json
import logging
import os
import pickle
import time


logger = logging.getLogger(__name__)

logger.debug(f'init')

class ProtocolHandler:
    def __init__(self, transport):
        self.transport = transport
        self.buffer = b''
        self.handlers = {
            b'+': self.handle_simple_string,
            b'-': self.handle_error,
            b':': self.handle_integer,
            b'$': self.handle_bulk_string,
            b'*': self.handle_array,
        }

    def data_received(self, data):
        logger.debug(f'Received data: {data}')
        self.buffer += data
        while self.buffer:
            prefix, handler = self.read_prefix()
            if prefix is None:
                break
            message = handler()
            if message is not None:
                self.transport.write(self.encode_message(message))

    def read_prefix(self):
        for prefix, handler in self.handlers.items():
            if self.buffer.startswith(prefix):
                self.buffer = self.buffer[len(prefix):]
                return prefix, handler
        return None, None

    def handle_simple_string(self):
        return self.read_line()

    def handle_error(self):
        return Exception(self.read_line())

    def handle_integer(self):
        return int(self.read_line())

    def handle_bulk_string(self):
        length = int(self.read_line())
        if length == -1:
            return None
        data = self.read(length)
        self.read_line()  # Discard trailing CRLF
        return data

    def handle_array(self):
        length = int(self.read_line())
        if length == -1:
            return None
        return [self.handle_request() for _ in range(length)]

    def handle_request(self):
        prefix, handler = self.read_prefix()
        if prefix is None:
            raise ValueError('Invalid request')
        command = handler()
        if isinstance(command, list):
            return self.transport.server.handle_command(command[0].decode(), *[arg.decode() for arg in command[1:]])
        return command

    def read_line(self):
        index = self.buffer.find(b'\r\n')
        if index == -1:
            raise ValueError('Incomplete request')
        line = self.buffer[:index]
        self.buffer = self.buffer[index+2:]
        return line.decode()

    def read(self, length):
        if len(self.buffer) < length:
            raise ValueError('Incomplete request')
        data = self.buffer[:length]
        self.buffer = self.buffer[length:]
        return data

    def encode_message(self, message):
        logger.debug(f'Sending response: {message}')
        if message is None:
            return b'$-1\r\n'
        elif isinstance(message, str):
            return f'+{message}\r\n'.encode()
        elif isinstance(message, int):
            return f':{message}\r\n'.encode()
        elif isinstance(message, bytes):
            # return f'${len(message)}\r\n{message}\r\n'.encode()
            return f'${len(message)}\r\n{message.decode()}\r\n'.encode()
        elif isinstance(message, list):
            return f'*{len(message)}\r\n'.encode() + b''.join(self.encode_message(item) for item in message)
        elif isinstance(message, Exception):
            return f'-{message}\r\n'.encode()
        else:
            raise ValueError(f'Invalid message type: {type(message)}')


class RedisServer:
    def __init__(self, host='127.0.0.1', port=6379, password=None, db=0, rdb_file='dump.rdb', aof_file='appendonly.aof', replication_id=None):
        print('begin init')
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        self.data = {}
        self.expires = {}
        self.watching = {}
        self.pubsub_channels = {}
        self.pubsub_patterns = {}
        self.rdb_file = rdb_file
        self.aof_file = aof_file
        self.aof_buffer = []
        self.replication_id = replication_id or str(int(time.time()))
        self.replication_offset = 0
        self.slaves = set()
        self.load_data()

    async def handle_client(self, reader, writer):
        protocol = ProtocolHandler(writer)
        logger.info('Client connected')
        try:
            while True:
                data = await reader.read(1024)
                if not data:
                    break
                protocol.data_received(data)
        except ConnectionResetError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()
            logger.info('Client disconnected')
            asyncio.get_event_loop().stop()

    def run(self):
        # loop = asyncio.get_running_loop()
        loop = asyncio.new_event_loop()
        # asyncio.set_event_loop(loop)
        coro = asyncio.start_server(self.handle_client, self.host, self.port)
        server = loop.run_until_complete(coro)
        logger.info(f'Serving on {server.sockets[0].getsockname()}')
        try:
            logger.info(f'looping')
            loop.run_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.close()
            loop.run_until_complete(server.wait_closed())
            loop.close()

    def handle_command(self, command, *args):
        print('Handling command: {command} {args}')
        logger.debug(f'Executing command: {command} {args}')
        if not hasattr(self, f'handle_{command}'):
            logger.error(f'Unknown command: {command}')
            raise Exception(f'ERR unknown command `{command}`')
        result = getattr(self, f'handle_{command}')(*args)
        logger.debug(f'Command result: {result}')
        return result

    def handle_ping(self, *args):
        if len(args) > 1:
            raise Exception('ERR wrong number of arguments for `ping` command')
        if len(args) == 0:
            return 'PONG'
        else:
            return args[0]

    def handle_echo(self, message):
        return message

    def handle_set(self, key, value, *args):
        # 处理 SET 命令,支持 EX、PX、NX、XX 等选项
        ex = None
        px = None
        nx = False
        xx = False
        i = 0
        while i < len(args):
            if args[i] == 'EX':
                ex = int(args[i+1])
                i += 2
            elif args[i] == 'PX':
                px = int(args[i+1])
                i += 2
            elif args[i] == 'NX':
                nx = True
                i += 1
            elif args[i] == 'XX':
                xx = True
                i += 1
            else:
                raise Exception(f'ERR syntax error')
        if nx and xx:
            raise Exception(f'ERR syntax error')
        if nx and key in self.data:
            return None
        if xx and key not in self.data:
            return None
        self.data[key] = value
        if ex is not None:
            self.expires[key] = time.time() + ex
        elif px is not None:
            self.expires[key] = time.time() + px / 1000
        self.aof_buffer.append(('SET', key, value, *args))
        return 'OK'

    def handle_get(self, key):
        # 处理 GET 命令,返回键对应的值
        print(f'Handling get: {key}')
        return self.data.get(key)

    def handle_incr(self, key):
        try:
            self.data[key] = int(self.data[key]) + 1
        except (KeyError, ValueError):
            self.data[key] = 1
        return self.data[key]

    def handle_decr(self, key):
        try:
            self.data[key] = int(self.data[key]) - 1
        except (KeyError, ValueError):
            self.data[key] = -1
        return self.data[key]

    def handle_subscribe(self, *channels):
        for channel in channels:
            if channel not in self.pubsub_channels:
                self.pubsub_channels[channel] = set()
            self.pubsub_channels[channel].add(self.transport)
        return [len(channels), *channels]

    def handle_unsubscribe(self, *channels):
        if not channels:
            channels = list(self.pubsub_channels.keys())
        for channel in channels:
            if channel in self.pubsub_channels:
                self.pubsub_channels[channel].discard(self.transport)
                if not self.pubsub_channels[channel]:
                    del self.pubsub_channels[channel]
        return [len(channels), *channels]

    def handle_publish(self, channel, message):
        receivers = self.pubsub_channels.get(channel, set())
        for transport in receivers:
            transport.write(self.encode_message(['message', channel, message]))
        return len(receivers)

    def handle_psubscribe(self, *patterns):
        for pattern in patterns:
            if pattern not in self.pubsub_patterns:
                self.pubsub_patterns[pattern] = set()
            self.pubsub_patterns[pattern].add(self.transport)
        return [len(patterns), *patterns]

    def handle_punsubscribe(self, *patterns):
        if not patterns:
            patterns = list(self.pubsub_patterns.keys())
        for pattern in patterns:
            if pattern in self.pubsub_patterns:
                self.pubsub_patterns[pattern].discard(self.transport)
                if not self.pubsub_patterns[pattern]:
                    del self.pubsub_patterns[pattern]
        return [len(patterns), *patterns]

    def handle_multi(self):
        # 处理 MULTI 命令,开始一个事务
        self.transaction = []
        return 'OK'

    def handle_exec(self):
        # 处理 EXEC 命令,执行事务中的命令
        if not hasattr(self, 'transaction'):
            raise Exception('ERR EXEC without MULTI')
        result = []
        for command, *args in self.transaction:
            try:
                result.append(self.handle_command(command, *args))
            except Exception as e:
                result.append(e)
        self.transaction = None
        return result

    def handle_discard(self):
        # 处理 DISCARD 命令,取消事务
        if not hasattr(self, 'transaction'):
            raise Exception('ERR DISCARD without MULTI')
        self.transaction = None
        return 'OK'

    def handle_watch(self, *keys):
        # 处理 WATCH 命令,监视一个或多个键
        for key in keys:
            if key not in self.watching:
                self.watching[key] = set()
            self.watching[key].add(self.transport)
        return 'OK'

    def handle_unwatch(self):
        # 处理 UNWATCH 命令,取消所有监视
        self.watching = {}
        return 'OK'

    def handle_save(self):
        # 处理 SAVE 命令,创建一个 RDB 文件
        self.save_rdb()
        return 'OK'

    def handle_bgsave(self):
        # 处理 BGSAVE 命令,在后台创建一个 RDB 文件
        asyncio.create_task(self.bgsave())
        return 'Background saving started'

    def handle_lastsave(self):
        # 处理 LASTSAVE 命令,返回最后一次成功创建 RDB 文件的 UNIX 时间戳
        return int(os.path.getmtime(self.rdb_file))

    def handle_bgrewriteaof(self):
        # 处理 BGREWRITEAOF 命令,在后台重写 AOF 文件
        asyncio.create_task(self.bgrewriteaof())
        return 'Background append only file rewriting started'

    def handle_slaveof(self, host, port):
        # 处理 SLAVEOF 命令,使当前服务器成为另一个服务器的从服务器
        if host == 'NO' and port == 'ONE':
            self.replication_id = None
            self.replication_offset = 0
            return 'OK'
        self.replication_id = None
        self.replication_offset = 0
        asyncio.create_task(self.slaveof(host, int(port)))
        return 'OK'

    def handle_role(self):
        # 处理 ROLE 命令,返回当前服务器的角色
        if self.replication_id is None:
            return ['master', self.replication_offset, []]
        else:
            return ['slave', self.master_host, self.master_port, self.replication_offset, self.replication_id]

    async def slaveof(self, host, port):
        # 异步方法,将当前服务器设置为另一个服务器的从服务器
        reader, writer = await asyncio.open_connection(host, port)
        writer.write(b'SYNC\r\n')
        await writer.drain()
        data = await reader.readuntil(b'\r\n')
        self.replication_id = data[:-2].decode()
        while True:
            data = await reader.read(1024)
            if not data:
                break
            self.load_rdb(data)
        writer.close()
        await writer.wait_closed()
        self.master_host = host
        self.master_port = port

    def save_rdb(self):
        # 创建一个 RDB 文件
        with open(self.rdb_file, 'wb') as f:
            pickle.dump(self.data, f)

    async def bgsave(self):
        # 在后台创建一个 RDB 文件
        with open(f'{self.rdb_file}.temp', 'wb') as f:
            pickle.dump(self.data, f)
        os.rename(f'{self.rdb_file}.temp', self.rdb_file)

    def load_rdb(self, data):
        # 从 RDB 文件中加载数据
        self.data = pickle.loads(data)

    def load_data(self):
        # 从 RDB 或 AOF 文件中加载数据
        if os.path.exists(self.aof_file):
            self.load_aof()
        elif os.path.exists(self.rdb_file):
            with open(self.rdb_file, 'rb') as f:
                self.data = pickle.load(f)

    def load_aof(self):
        # 从 AOF 文件中加载数据
        with open(self.aof_file, 'r') as f:
            for line in f:
                command, *args = json.loads(line)
                self.handle_command(command, *args)

    async def bgrewriteaof(self):
        # 在后台重写 AOF 文件
        with open(f'{self.aof_file}.temp', 'w') as f:
            for key, value in self.data.items():
                f.write(json.dumps(('SET', key, value)) + '\n')
        os.rename(f'{self.aof_file}.temp', self.aof_file)
        self.aof_buffer = []

    def flush_aof(self):
        # 将 AOF 缓冲区中的命令写入 AOF 文件
        with open(self.aof_file, 'a') as f:
            for command in self.aof_buffer:
                f.write(json.dumps(command) + '\n')
        self.aof_buffer = []


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    server = RedisServer()
    print('!server: {server}')
    server.run()