#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" 
acestreamengine python3 client.
acestream website: http://acestream.org
"""

from concurrent.futures import Future, TimeoutError as FutureTimeoutError
from collections import namedtuple, defaultdict
from subprocess import Popen, PIPE, TimeoutExpired, DEVNULL
from urllib.parse import unquote
from enum import Enum
from functools import partial
import threading
import telnetlib
import json
import hashlib
import logging
import select
import inspect
import argparse


PUB_DEV_KEY  = 'kjYX790gTytRaXV04IvC-xZH3A18sj5b1Tf3I-J5XVS1xsj-j0797KwxxLpBl26HPvWMm'
PRIV_DEV_KEY = 'n51LvQoTlJzNGaFxseRK-uvnvX-sD4Vm5Axwmc4UcoD-jruxmKsuJaH0eVgE'


def build_logger(level, format, file=None):
    loghandler = logging.FileHandler(file) if file else logging.StreamHandler()
    loghandler.setFormatter(logging.Formatter(format))
    logger = logging.getLogger('acestream')
    logger.setLevel(level)
    logger.addHandler(loghandler)

    return logger


class Gender(Enum):
    MALE     = 1
    FEMALE   = 2


class Age(Enum):
    AGE_00_12 = 1  # <13
    AGE_13_17 = 2  # 13-17
    AGE_18_24 = 3  # 18-24
    AGE_25_34 = 4  # 25-34
    AGE_35_44 = 5  # 35-44
    AGE_45_54 = 6  # 45-54
    AGE_55_64 = 7  # 55-64
    AGE_65_GT = 8  # >64

    @staticmethod
    def from_age(age):
        return Age(Age.AGE_00_12 if 00 <= age < 13 else \
                   Age.AGE_13_17 if 13 <= age < 17 else \
                   Age.AGE_18_24 if 18 <= age < 24 else \
                   Age.AGE_25_34 if 25 <= age < 34 else \
                   Age.AGE_35_44 if 35 <= age < 44 else \
                   Age.AGE_45_54 if 45 <= age < 54 else \
                   Age.AGE_55_64 if 55 <= age < 64 else \
                   Age.AGE_65_GT)


class State(Enum):
    IDLE            = 0
    PREBUFFERING    = 1
    DOWNLOADING     = 2
    BUFFERING       = 3
    COMPLETED       = 4
    CHECKING        = 5
    ERROR           = 6


class AceClientException(Exception):
    """
    AceClient base exception
    """
    pass


class AceClient():
    """ 
    Acestreamengine client
    Implements a high-level and low-level api for acestreamengine ver. 3.0.
    NB: AceClient is not thread-safe
    
    engine api description: http://wiki.acestream.org/wiki/index.php/Engine_API/en
    """

    class Result(namedtuple('Result', ['args', 'kwargs'])):
        def __str__(self):
            return ' '.join([str(arg) for arg in self.args] + 
                            ['{}={}'.format(key, str(val)) for key, val in self.kwargs.items()])

    API_VERSION = '6'
    ACE_VERSION = '3.0.3'
    EOL = '\r\n'

    ACESTREAM_ENGINE_PATH = 'acestreamengine'
    ACESTREAM_ENGINE_ARGS = ['--client-console', '--bind-all']

    LOGGER_DEFAULT_FORMAT = '%(levelname)-8s [%(asctime)s] %(message)s'
    LOGGER_DEFAULT_LOGLVL = logging.INFO

    def __init__(self, host=None, port=62062, timeout=5, 
                       gender=None, age=None,
                       logger=None,                     
                       engine_path=ACESTREAM_ENGINE_PATH, 
                       engine_args=ACESTREAM_ENGINE_ARGS, 
                       product_key=PRIV_DEV_KEY):
        self.host = host
        self.port = port
        self.logger = logger or build_logger(format=self.LOGGER_DEFAULT_FORMAT, 
                                             level=self.LOGGER_DEFAULT_LOGLVL)

        self.engine_path = engine_path
        self.engine_args = engine_args
        self.product_key = product_key

        self.done = False
        self.state = State.IDLE

        self.gender = gender
        self.age = age

        self.read_thread = None
        self.engine_proc = None
        self.telnet = None

        self.current_transport_data = None
        self.current_transport_type = None
        self.current_http_url = None

        self.cmd_handlers = defaultdict(list)
        
        self.init_default_handlers()

        if host is not None:
            self.open(host, port, timeout)

    def open(self, host, port=62062, timeout=5):
        self.host = host
        self.port = port
        self.timeout = timeout

        self.close()

        try:
            self.telnet = telnetlib.Telnet(host, port, timeout)
        except Exception as e:
            raise AceClientException("engine connection error: {}".format(e))
        else:
            self.logger.info("successfuly connected to engine")

        self.read_thread = threading.Thread(target=self.run)
        self.read_thread.start()

    def close(self, timeout=5):
        if self.telnet:
            self.done = True

            self.stop()
            self.shutdown()

            self.telnet.close()
            self.telnet = None

            self.logger.info("connection to {}:{} closed".format(self.host, self.port))

        if self.read_thread:
            self.read_thread.join(timeout)

            if self.read_thread.is_alive():
                raise AceClientException("dispatcher thread termination failed")
            self.read_thread = None

            self.logger.info("dispatcher thread terminated")

    def wait(self):
        self.read_thread.join()

    def run(self):
        self.logger.info("starting dispatcher thread")
        self.done = False
        self.dispatch()

    def dispatch(self):
        while not self.done:
            try:
                cmd, *params_str = self.recv()
                for handler in self.cmd_handlers[cmd]:
                    handler(*params_str)
                self.logger.debug("command handled: %s", ' '.join([cmd] + params_str))
            except (EOFError, BrokenPipeError, KeyboardInterrupt):
                self.done = True
            except KeyError as e:
                self.logger.debug("command unhandled: %s", ' '.join([cmd] + params_str))
            except Exception as e:
                self.logger.warning("command handling error: %s: %s", ' '.join([cmd] + params_str), e)

        self.logger.info("stopping dispatcher thread")

    def stop_client(self):
        self.close()
        self.stop_engine()
        self.logger.info("client stopped")

    def start_engine(self, engine_path=None, engine_args=None, timeout=10):
        self.stop_engine()

        self.logger.info("starting engine")

        executable = [engine_path or self.engine_path] + (engine_args or self.engine_args)

        try:
            self.engine_proc = Popen(executable, stdout=PIPE, stderr=PIPE, universal_newlines=True)
        except OSError as e:
            raise AceClientException("engine start error: {}".format(e))

        try:
            self.read_proc_output_until(self.engine_proc, 'acestream.APIServer|run: ready to receive remote commands', timeout*1000)
        except (KeyboardInterrupt, AceClientException) as e:
            self.stop_engine()
            raise AceClientException("engine initialization error: {err}".format(err=e))
        
        self.logger.info("engine started")

    def stop_engine(self, timeout=10):
        self.close()

        if self.engine_proc is not None:
            try:
                self.engine_proc.terminate()
                self.engine_proc.wait(timeout)
            except TimeoutExpired as e:
                self.logger.warning("engine process termination failed. let's kill it!!!")

                try:
                    self.engine_proc.kill()
                    self.engine_proc.wait(timeout)
                except TimeoutExpired as e:
                    self.logger.error("tough guy. don't know what else to do. maybe kill it later??? or it die by itself")
                    raise AceClientException("engine process killing failed: {err}".format(err=e))

            self.engine_proc = None
            self.logger.info("engine stopped")

    def read_proc_output_until(self, proc, match, timeout=None):
        selector = select.poll()
        selector.register(proc.stdout, select.POLLIN)
        selector.register(proc.stderr, select.POLLIN)

        self.logger.debug("starting engine output polling")
        for fd, event in self.select_generator(selector, timeout):
            if event == select.POLLHUP:
                raise AceClientException("engine output stream closed, possibly engine stoped")

            if event == select.POLLERR:
                raise AceClientException("engine output selector error")

            if fd == proc.stdout.fileno():
                for line in proc.stdout:
                    self.logger.debug("engine: %s", line.rstrip())
                    if match in line: return

            elif fd == proc.stderr.fileno():
                for line in proc.stderr:
                    self.logger.error("engine error: %s", line.rstrip())

            else:
                raise AceClientException("engine output selector got unexpected descriptor: {}".format(fd))

        raise AceClientException("engine init timeout")

    def select_generator(self, selector, timeout):
        while True:
            events = selector.poll(timeout)
            if not events:
                raise StopIteration()
            else:
                for fd, ev in events:
                    yield fd, ev

    def write(self, string):
        string = string + self.EOL
        if not self.telnet:
            raise AceClientException("aceclient is not connected to engine")

        try:
            self.telnet.write(string.encode('ascii'))
        except (AttributeError, BrokenPipeError): # in case Telnet is trying to write to the closed socket
            self.telnet = None
            raise AceClientException("aceclient connection closed. try to connect before")

        self.logger.debug("wrote: %s", string.rstrip())

    def read_until(self, expected):
        if not self.telnet:
            raise AceClientException("aceclient is not connected to engine")
        
        try:
            resp = self.telnet.read_until(expected.encode('ascii')).decode('utf8')
        except (AttributeError, BrokenPipeError): # in case Telnet is trying to read from the closed socket
            self.telnet = None
            raise AceClientException("aceclient connection closed. try to connect before")
        
        self.logger.debug("read: %s", resp.rstrip())
        return resp

    def send(self, cmd, *args, **kwargs):
        self.write(' '.join([cmd] + [str(arg) for arg in args] +
                            ['='.join([key, str(val)]) for (key, val) in kwargs.items()]))

    def recv(self):
        return unquote(self.read_until(self.EOL)).strip().split(' ', 1)

    def parse_args(self, string):
        args, kwargs = [], {}
        
        for arg in string.split():
            kwargs.update([arg.split('=')]) if '=' in arg else args.append(arg)
        
        return args, kwargs

    def parse_json(self, string):
        return json.loads(string)

    def send_userdata(self, gender=None, age=None):
        gender = gender or self.gender
        age = age or self.age

        if gender is None or age is None:
            raise AceClientException("userdata is not set")

        data = [
            {"gender":  gender.value},
            {"age":     age.value}
        ]

        self.send('USERDATA', data)

    def add_cmd_handler(self, cmd, handler):
        self.cmd_handlers[cmd].append(handler)

    def del_cmd_handler(self, cmd, handler):
        self.cmd_handlers[cmd].remove(handler)

    def init_default_handlers(self):
        self.add_cmd_handler('SHUTDOWN', self.close)

        def log_handler(lvl, msg):
            self.logger.log(logging.getLevelName(lvl), msg)
        
        self.add_cmd_handler('EVENT', partial(log_handler, 'INFO'))

        def state_handler(state):
            self.state = State(int(state))
            self.logger.info("state has been switched to %s", state)

        self.add_cmd_handler('STATE', state_handler)

    def hello_async(self):
        future = Future()

        def handler(params_str):
            args = self.parse_args(params_str)
            future.set_result(self.Result(*args))

        self.add_cmd_handler('HELLOTS', handler)
        self.send('HELLOBG', version=self.API_VERSION, ace=self.ACE_VERSION)

        return future

    def hello(self, timeout=5):
        return self.hello_async().result(timeout)

    def ready_async(self, request_key, product_key=None):
        future = Future()

        def handler(params_str):
            args = self.parse_args(params_str)
            future.set_result(self.Result(*args))

        self.add_cmd_handler('AUTH', handler)
        self.send('READY', key=self.keygen(request_key, product_key or self.product_key))

        return future

    def ready(self, request_key, product_key=None, timeout=5):
        return self.ready_async(request_key, product_key).result(timeout)

    def keygen(self, request_key, product_key):
        sha1 = hashlib.sha1()
        sha1.update(str(request_key + product_key).encode('ascii'))
        signature = sha1.hexdigest()

        x = product_key.split("-")[0];
        response_key = x + "-" + signature

        return response_key

    def load_async(self, transport_data, transport_type='TORRENT', request_id=0, developer_id=0, affiliate_id=0, zone_id=0):
        future = Future()

        def handler(params_str):
            request_id, response = params_str.split(' ', 1)
            future.set_result(self.Result([request_id], self.parse_json(response)))

        self.add_cmd_handler('LOADRESP', handler)
        self.send('LOADASYNC', request_id, transport_type, transport_data, developer_id, affiliate_id, zone_id)

        return future

    def load(self, transport_data, transport_type='TORRENT', request_id=0, developer_id=0, affiliate_id=0, zone_id=0, timeout=15):
        return self.load_async(transport_data, transport_type, request_id, developer_id, affiliate_id, zone_id).result(timeout)

    def start_async(self, transport_data, transport_type='TORRENT', developer_id=0, affiliate_id=0, zone_id=0, stream_id=0):
        future = Future()

        def handler(params_str):
            args = self.parse_args(params_str)
            future.set_result(self.Result(*args))

        self.add_cmd_handler('START', handler)
        self.send('START', transport_type, transport_data, developer_id, affiliate_id, zone_id, stream_id)

        return future

    def start(self, transport_data, transport_type='TORRENT', developer_id=0, affiliate_id=0, zone_id=0, stream_id=0, timeout=45):
        return self.start_async(transport_data, transport_type, developer_id, affiliate_id, zone_id, stream_id).result(timeout)

    def stop(self):
        self.current_transport_data = None
        self.current_transport_type = None
        self.current_http_url = None

        self.send('STOP')

    def shutdown(self):
        self.stop()
        self.send('SHUTDOWN')

    def setoptions(self, *options):
        self.write(' '.join(['SETOPTIONS'] + ['{}={}'.format(*option.split('=', 1)) for option in options]))

    def is_playing(self):
        return self.state == State.COMPLETED

    def handshake(self):
        _, kwargs = self.hello()
        self.ready(kwargs['key'])

    def play(self, transport_data, transport_type='TORRENT'):
        if not self.is_playing() or (self.current_transport_data != transport_data) or (self.current_transport_type != transport_type):
            self.load(transport_data, transport_type)
            _, kwargs = self.start(transport_data, transport_type)

            self.current_transport_data = transport_data
            self.current_transport_type = transport_type
            self.current_http_url = kwargs['url']

        return self.current_http_url


class CliException(Exception):
    """
    Cli base exception
    """
    pass

class Cli(object):
    """ 
    AceClient command line interface (interactive shell)    
    """

    def __init__(self, aceclient):
        self.done = False
        self.client = aceclient

        self.client.add_cmd_handler('SHUTDOWN', lambda: print("SHUTDOWN"))
        self.client.add_cmd_handler('NOTREADY', lambda: print('NOTREADY'))
        self.client.add_cmd_handler('EVENT', lambda ev: print("EVENT: {}".format(ev)))
        self.client.add_cmd_handler('STATE', lambda st: print("STATE: {}".format(st)))

    def cmd_loop(self):
        while not self.done:
            try:
                cmd = input('> ').split()
                if cmd: 
                    self.dispatch_cmd(cmd[0], *cmd[1:])
            except CliException as e:
                print(e)
                self.help()
            except FutureTimeoutError as e:
                print("engine response timeout error")            
            except AceClientException as e:
                print("AceClient error: {err}".format(err=e))            

    def dispatch_cmd(self, cmd, *args):
        attr = getattr(self, cmd, None)
        if hasattr(attr, '_is_handler'):
            if not self.check_signature(attr, args):
                raise CliException("command '{}' format error".format(cmd))
            else:
                attr(*args)
        else:
            raise CliException("command '{}' not found".format(cmd))

    def check_signature(self, func, args):
        spec = inspect.getargspec(func)
        if len(spec.args) - len(spec.defaults or []) <= len(args) + 1 <= len(spec.args) or spec.varargs:
            return True
        else:
            return False

    def handler(func):
        func._is_handler = True
        return func

    @handler
    def help(self):
        print("help                              - print help message                                     \n"
              "connect IP [PORT]                 - connect to a remote acestreamengine                    \n"
              "hello                             - send HELLOTS command                                   \n"
              "ready KEY [PROD_KEY]              - send READY command using KEY as response key           \n"
              "load DATA [TYPE]                  - send LOADASYNC command, TYPE: TORRENT|INFOHASH|RAW     \n"
              "start DATA [TYPE]                 - send START command, TYPE: TORRENT|INFOHASH|RAW         \n"
              "close                             - close remote connection                                \n"
              "stop                              - send STOP command                                      \n"
              "estart                            - start acestreamengine                                  \n"
              "estop                             - stop acestreamengine                                   \n"
              "play ACE_URL                      - play ACE_URL                                           \n"
              "handshake                         - hello and ready shortcut                               \n"
              "log LOGLEVEL                      - set loglevel, LOGLEVEL NOTSET|DEBUG|INFO|WARNING|ERROR \n"
              "userdata AGE GENDER               - send userdata, GENDER: MALE|FEMALE                     \n"
              "setoptions key=val [key=val ...]  - send raw command                                       \n"
              "raw [CMD]                         - send raw command                                       \n"
              "exit                              - close remote connection/stop acestreamengine and exit  \n")

    @handler
    def connect(self, host, port=62062):
        self.client.open(host, port, 5)
        print("successfuly connected to {}:{}".format(host, port))

    @handler
    def hello(self):
        print(self.client.hello())

    @handler
    def ready(self, key, product_key=PRIV_DEV_KEY):
        print("auth: {}".format(self.client.ready(key, product_key)))

    @handler
    def load(self, transport_data, transport_type='TORRENT'):
        print(self.client.load(transport_data, transport_type))

    @handler
    def start(self, transport_data, transport_type='TORRENT'):
        print(self.client.start(transport_data, transport_type))

    @handler
    def close(self):
        self.client.close()

    @handler
    def stop(self):
        self.client.stop()

    @handler
    def estart(self):
        self.client.start_engine()
        print("engine successfuly started")

    @handler
    def estop(self):
        self.client.stop_engine()
        print("engine successfuly stopped")

    @handler
    def log(self, lvl):
        try:
            self.client.logger.setLevel(lvl.upper())
        except ValueError:
            raise CliException("log command error: loglevel is incorrect")
        else:
            print("loglevel has been set to '{}'".format(lvl))

    @handler
    def handshake(self):
        self.client.handshake()
        print("connection has been successfuly initialized")

    @handler
    def play(self, url):
        print(self.client.play(url))

    @handler
    def userdata(self, age, gender):
        try:
            age, gender = Age.from_age(int(age)), Gender[gender.upper()]
        except (KeyError, ValueError):
            raise CliException("userdata is incorrect")
        else:
            self.client.send_userdata(gender, age)

    @handler
    def raw(self, *args):
        self.client.write(' '.join(args))

    @handler
    def exit(self):
        self.done = True



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="acestreamengine client.")
    subparsers = parser.add_subparsers(help="client modes", dest="mode")

    parser_shell = subparsers.add_parser("shell", help="open an interactive client shell")

    parser_local = subparsers.add_parser("local", help="start a local acestream engine")
    parser_local.add_argument("-p", "--path", help="acestreamengine path (default: %(default)s)", default="acestreamengine", metavar="PATH")
    parser_local.add_argument("-a", "--args", help="acestreamengine args (default: %(default)s)", default="--client-console --bind-all", metavar="ARGS")
    parser_local.add_argument("-d", "--data", required=True, help="transport data to open to", metavar="URL")
    parser_local.add_argument("-t", "--type", help="transport type (default: %(default)s)", metavar="TYPE", choices=['TORRENT','INFOHASH','RAW'], default='TORRENT')
    parser_local.add_argument("-y", "--player", help="player to play video stream with (default: %(default)s)", metavar="PLAYER", default="vlc ${URL}")
    parser_local.add_argument("-l", "--logfile", help="log file (default: stdout)", metavar="LOGFILE")
    parser_local.add_argument("-o", "--loglevel", help="loglevel (default: %(default)s)", choices=['NOTSET','DEBUG','INFO','WARNING','ERROR'], metavar="LOGLEVEL", default="INFO")
    parser_local.add_argument("-f", "--logformat", help="logformat (default: %(default)s)", metavar="LOGFORMAT", default="%(levelname)-8s [%(asctime)s] %(message)s")
    parser_local.add_argument("-e", "--age", help="user age", type=int, metavar="AGE")
    parser_local.add_argument("-g", "--gender", help="user gender", choices=['male','female'], metavar="GENDER")


    parser_remote = subparsers.add_parser("remote", help="connect to a remote acestream engine")
    parser_remote.add_argument("-a", "--host", required=True, help="host to connect to (default: %(default)s)", default="localhost", metavar="HOST")
    parser_remote.add_argument("-p", "--port", type=int, help="port to connect to (default: %(default)s)", default=62062, metavar="PORT")
    parser_remote.add_argument("-d", "--data", required=True, help="transport data to open to", metavar="URL")
    parser_remote.add_argument("-t", "--type", help="transport type (default: %(default)s)", metavar="TYPE", choices=['TORRENT','INFOHASH','RAW'], default='TORRENT')
    parser_remote.add_argument("-y", "--player", help="player to play video stream with (default: %(default)s)", metavar="PLAYER", default="vlc ${URL}")
    parser_remote.add_argument("-l", "--logfile", help="log file (default: stdout)", metavar="LOGFILE")
    parser_remote.add_argument("-o", "--loglevel", help="loglevel (default: %(default)s)", choices=['NOTSET','DEBUG','INFO','WARNING','ERROR'], metavar="LOGLEVEL", default="INFO")
    parser_remote.add_argument("-f", "--logformat", help="logformat (default: %(default)s)", metavar="LOGFORMAT", default="%(levelname)-8s [%(asctime)s] %(message)s")
    parser_remote.add_argument("-e", "--age", help="user age", type=int, metavar="AGE")
    parser_remote.add_argument("-g", "--gender", help="user gender", choices=['male','female'], metavar="GENDER")

    args = parser.parse_args()

    try:
        if args.mode == 'shell':
            client = AceClient()
            cli = Cli(client)
            cli.cmd_loop()

        elif args.mode in ('local', 'remote'):
            logger = build_logger(logging.getLevelName(args.loglevel.upper()), args.logformat, args.logfile)
            client = AceClient(logger=logger, gender=args.gender, age=args.age)

            if args.mode == 'local':
                client.start_engine(args.path, args.args.split())
                client.open('localhost')
            else:
                client.open(args.host, args.port)

            client.handshake()
            http_url = client.play(args.data, args.type)
            
            Popen(args.player.replace('${URL}', http_url), shell=True, stdout=DEVNULL, stderr=DEVNULL)            
            client.wait()
        else:
            parser.print_help()

    except KeyboardInterrupt:
        print("got sighup. exiting")
    except Exception as e:
        print("got an exception: {err}. exiting".format(err=e))
    finally:
        client.stop_client()
        print("client stoped")