import asyncio
import abc
import threading
import dataclasses
import datetime as dt
import aiohttp

import schemas

@dataclasses.dataclass
class ServerConfig:
  url: str

@dataclasses.dataclass
class AppInfo:
  name: str
  version: str

@dataclasses.dataclass
class Ping:
  name: str
  every: dt.timedelta

class MainThread(threading.Thread):
  def __init__(self, daemon=True):
    super().__init__(target=self._start, daemon=daemon)

  def _start(self):
    self._loop = asyncio.new_event_loop()
    asyncio.set_event_loop(self._loop)
    self._loop.call_soon(self._loop._main)
    self._loop.run_forever()

  @abc.abstractmethod
  def _main(self) -> None:
    pass

  def stop(self):
    self._loop.call_soon_threadsafe(self._loop.stop)
  
class ClientMainThread(MainThread):
  def __init__(
    self,
    daemon=False,
    collect_info_timeout=dt.timedelta(minutes=1)
    ):
    super().__init__(daemon=daemon)
    self.collect_info_timeout = collect_info_timeout 

  def _main(self) -> None:
    pass
    self.collect_info = _CollectInfoTask(self.collect_info_timeout).run()

    
class _CollectInfoTask():
  def __init__(self, timeout):
    self.timeout = timeout
  
  def run(self):
    collector = asyncio.InfoCollector()
    while True:
      # handle errors
      asyncio.to_thread(collector.collect()
      asyncio.sleep(self.timeout)

@dataclasses.dataclass
class DiskInfo:
  total: int 
  used: int
  free: int

@dataclasses.dataclass
class SystemInfo:
  disk: typing.Optional[DiskInfo]
  
class InfoCollector:
  def collect(self) -> SystemInfo:
    try:
      disk_info = self.collect_disk_info()
    except Exception as e:
      disk_info = None
    return SystemInfo(disk=disk_info)

  def collect_disk_info(self) -> DiskInfo:
    total, used, free = shutil.disk_usage("/")
    return DiskInfo(total, used, free)


class Ping:
  def __init__(self, name: str, valid_for: dt.timedelta):
    self._client = PingClient(name, valid_for)

  def ping(self):
    now = dt.datetime.now()
    asyncio.run_coroutine_threadsafe(self._client._set_ping(now))
  

# TODO: add thread assertions
class PingClient:
  def __init__(self, name, valid_for):
    self._name = name
    self._valid_for = valid_for
    self.last_sent_ping = dt.datetime.min
    self.pending_send = None

  async def _set_ping(self, now):
    self.pending_send = max(now, self.pending_send)
    await self._send_ping()

  async def _send_ping(self):
    pending_send = self.pending_send
    if pending_send <= self.last_sent_ping:
      return
    async with aiohttp.ClientSession() as session:
      body = { }
      if await PingClient._send_impl(session, self._create_url.for_ping(), body):
        self.last_sent_ping = max(self.last_send_ping, pending_send)

  @staticmethod
  async def _send_impl(session, url, body):
    async with session.post(url, json=body) as response:
      for i in range(2):
        response_content = await response.json()
        if response_content == {'status': 'ok'}:
          return True
        asyncio.sleep(1)
      return False




          



class Client():
  def __init__(self, config: ServerConfig, server_id: str, app_info: AppInfo, regular_ping: Ping | None = None):
    self.config = config
    self.app_info = app_info
    self.server_id = server_id
    self.run_id = uuid.uuid4()
    self.app_info 
    self._thread = threading.Thread(target=self._thread_main)
    self._regular_ping = regular_ping or Ping(name='regular', every=dt.timedelta(minutes=1))
    self._finished = threading.Event()
    self._finished.clear()

  def _thread_main(self):
    while not self._finished.wait(timeout=1):
      self.ping(self._regular_ping)

  def start_ping(self):
    self._thread.start()

  def stop(self):
    self._finished.set()
    self._thread.join()

  # Can be called from any thread
  def ping(self, ping: Ping):
    body = {
      "run": {
        "server": {
          "id": self.server_id,
        },
        "app": {
          "name": self.app_info.name,
          "version": self.app_info.version,
        },
        "run": {
          "id": self.run_id,
        },
      },
      "ping": {
        "name": ping.name,
        "every": ping.every,
      },
    }
    body = schemas.PingSchema().dump(body)
    assert body
    print(body)
    requests.post(
      f'{self.config.url}/api/ping',
      body=body,
    )