import asyncio
import logging
import os
import pickle
import time


logger = logging.getLogger(__name__)

logger.debug(f'init')

class ProtocolHandler:
    def __init__(self, server):
        self.server = server
        self.buffer = ''

    def data_received(self, data):
        logger.debug(f'Received data: {data}')
        self.buffer += data.decode()
        if '\n' in self.buffer:
            lines = self.buffer.split('\n')
            self.buffer = lines.pop()
            for line in lines:
                command, *args = line.split()
                response = self.server.handle_command(command, *args)
                self.server.transport.write(self.encode_response(response))

    def encode_response(self, response):
        logger.debug(f'Sending response: {response}')
        if response is None:
            return '$-1\r\n'.encode()
        elif isinstance(response, str):
            return f'+{response}\r\n'.encode()
        elif isinstance(response, int):
            return f':{response}\r\n'.encode()
        elif isinstance(response, bytes):
            return f'${len(response)}\r\n{response.decode()}\r\n'.encode()
        elif isinstance(response, list):
            return f'*{len(response)}\r\n'.encode() + b''.join(self.encode_response(item) for item in response)
        elif isinstance(response, Exception):
            return f'-{response}\r\n'.encode()
        else:
            raise ValueError(f'Invalid response type: {type(response)}')


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
        self.transport = None
        self.load_data()

    async def handle_client(self, reader, writer):
        self.transport = writer
        protocol = ProtocolHandler(self)
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

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        coro = asyncio.start_server(self.handle_client, self.host, self.port)
        server = loop.run_until_complete(coro)
        logger.info(f'Serving on {server.sockets[0].getsockname()}')
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.close()
            loop.run_until_complete(server.wait_closed())
            loop.close()

    def handle_command(self, command, *args):
        print(f'Handling command: {command} {args}')
        logger.debug(f'Executing command: {command} {args}')
        if not hasattr(self, f'handle_{command.lower()}'):
            logger.error(f'Unknown command: {command}')
            raise Exception(f'ERR unknown command `{command}`')
        result = getattr(self, f'handle_{command.lower()}')(*args)
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
        self.aof_buffer.append(f'SET {key} {value} {" ".join(args)}\n')
        self.publish('__keyspace@0__:' + key, 'set')
        return 'OK'

    def handle_get(self, key):
        # 处理 GET 命令,返回键对应的值
        print(f'Handling get: {key}')
        return self.data.get(key)

    # 事务相关命令
    def handle_multi(self):
        self.transaction = []
        return 'OK'

    def handle_exec(self):
        if not hasattr(self, 'transaction'):
            raise Exception('ERR EXEC without MULTI')
        result = []
        for command, *args in self.transaction:
            try:
                result.append(self.handle_command(command, *args))
            except Exception as e:
                result.append(e)
        self.transaction = []
        return result

    def handle_discard(self):
        if not hasattr(self, 'transaction'):
            raise Exception('ERR DISCARD without MULTI')
        self.transaction = []
        return 'OK'

    # 发布订阅相关命令
    def handle_subscribe(self, *channels):
        for channel in channels:
            if channel not in self.pubsub_channels:
                self.pubsub_channels[channel] = set()
            self.pubsub_channels[channel].add(self.transport)
        return ['subscribe', len(channels), list(channels)]

    def handle_unsubscribe(self, *channels):
        if not channels:
            channels = list(self.pubsub_channels.keys())
        for channel in channels:
            if channel in self.pubsub_channels:
                self.pubsub_channels[channel].discard(self.transport)
                if not self.pubsub_channels[channel]:
                    del self.pubsub_channels[channel]
        return ['unsubscribe', len(channels), list(channels)]

    def handle_psubscribe(self, *patterns):
        for pattern in patterns:
            if pattern not in self.pubsub_patterns:
                self.pubsub_patterns[pattern] = set()
            self.pubsub_patterns[pattern].add(self.transport)
        return ['psubscribe', len(patterns), list(patterns)]

    def handle_punsubscribe(self, *patterns):
        if not patterns:
            patterns = list(self.pubsub_patterns.keys())
        for pattern in patterns:
            if pattern in self.pubsub_patterns:
                self.pubsub_patterns[pattern].discard(self.transport)
                if not self.pubsub_patterns[pattern]:
                    del self.pubsub_patterns[pattern]
        return ['punsubscribe', len(patterns), list(patterns)]

    def handle_publish(self, channel, message):
        self.publish(channel, message)
        return len(self.pubsub_channels.get(channel, set()))

    def publish(self, channel, message):
        for transport in self.pubsub_channels.get(channel, set()):
            transport.write(self.encode_response(['message', channel, message]))
        for pattern, transports in self.pubsub_patterns.items():
            if self.match_pattern(pattern, channel):
                for transport in transports:
                    transport.write(self.encode_response(['pmessage', pattern, channel, message]))

    def match_pattern(self, pattern, channel):
        # 简单的通配符匹配实现,支持 * 和 ? 通配符
        import fnmatch
        return fnmatch.fnmatch(channel, pattern)

    # 主从复制相关命令
    def handle_slaveof(self, host, port):
        if host == 'no' and port == 'one':
            self.slaveof = None
        else:
            self.slaveof = (host, int(port))
        return 'OK'

    def handle_sync(self):
        if not self.slaveof:
            raise Exception('ERR not a slave')
        self.transport.write(self.encode_response(['fullresync', self.replication_id, self.replication_offset]))
        self.send_snapshot()

    def handle_psync(self, replication_id, replication_offset):
        if not self.slaveof:
            raise Exception('ERR not a slave')
        if replication_id == self.replication_id:
            self.transport.write(self.encode_response(['continue', self.replication_offset]))
            self.send_backlog(int(replication_offset))
        else:
            self.transport.write(self.encode_response(['fullresync', self.replication_id, self.replication_offset]))
            self.send_snapshot()

    def send_snapshot(self):
        # 发送 RDB 快照
        self.transport.write(self.encode_response(['rdb', self.rdb_file]))
        with open(self.rdb_file, 'rb') as f:
            while True:
                data = f.read(1024)
                if not data:
                    break
                self.transport.write(data)

    def send_backlog(self, offset):
        # 发送 AOF 增量数据
        with open(self.aof_file, 'r') as f:
            f.seek(offset)
            while True:
                line = f.readline()
                if not line:
                    break
                self.transport.write(line.encode())

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
                command, *args = line.split()
                self.handle_command(command, *args)

    async def bgrewriteaof(self):
        # 在后台重写 AOF 文件
        with open(f'{self.aof_file}.temp', 'w') as f:
            for key, value in self.data.items():
                f.write(f'SET {key} {value}\n')
        os.rename(f'{self.aof_file}.temp', self.aof_file)
        self.aof_buffer = []

    def flush_aof(self):
        # 将 AOF 缓冲区中的命令写入 AOF 文件
        with open(self.aof_file, 'a') as f:
            f.writelines(self.aof_buffer)
        self.aof_buffer = []


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    server = RedisServer()
    print(f'Server: {server}')
    server.run()