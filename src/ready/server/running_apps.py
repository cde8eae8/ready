import asyncio
import inspect
import threading
import traceback
import uuid
import enum
import typing
import datetime as dt
import dataclasses

from ready.common import schemas

PingKey = typing.NewType("PingKey", str)
RunId = typing.NewType("RunId", uuid.UUID)
InstanceId = typing.NewType("InstanceId", uuid.UUID)

class PingState(enum.Enum):
  RUNNING = 'running'
  PROBABLY_FAILED = 'probably_failed'
  FAILED = 'failed'

@dataclasses.dataclass
class Ping:
  name: PingKey
  valid_for: dt.timedelta
  created_at: dt.datetime
  last_ping: dt.datetime
  state: PingState

@dataclasses.dataclass
class Server:
  id: uuid.UUID

@dataclasses.dataclass
class App:
  app_name: str
  app_version: str

@dataclasses.dataclass
class PingReport:
  run_id: RunId
  ping_id: PingKey
  ping_sent_at: dt.datetime
  valid_for: dt.timedelta

Event = PingReport

@dataclasses.dataclass
class Instance:
  id: InstanceId
  server: Server
  app: App
  active_run: typing.Optional["Run"]
  recently_runs: dict[RunId, "FailedRun"]
  new_events: list[Event]

@dataclasses.dataclass
class FailedRun:
  run_id: RunId
  failed_at: dt.datetime

@dataclasses.dataclass
class Run:
  instance: InstanceId
  run_id: RunId
  pings: dict[PingKey, Ping]

@dataclasses.dataclass
class NewRun:
  run: Run

@dataclasses.dataclass
class RunFailed:
  run: Run

@dataclasses.dataclass
class NewPing:
  run: Run
  ping: Ping

@dataclasses.dataclass
class PingStateChange:
  run: Run
  ping: Ping
  state: PingState

Notification = (
    NewRun
  | RunFailed
  | NewPing
  | PingStateChange
)

@dataclasses.dataclass
class ReportEvent:
  id = 'ping_event'
  app: App
  server: Server
  report: PingReport

class AsyncioThread:
  def __init__(self):
    self._started = threading.Event()
    self.thread = threading.Thread(target=self._main)
    self.loop = asyncio.new_event_loop()

  def start(self):
    self.thread.start()
    self._started.wait()

  def stop(self):
    self.loop.stop()

  def _main(self):
    asyncio.set_event_loop(self.loop)
    self._started.set()
    self.loop.run_forever()

class PostTask:
  def __init__(self):
    self.loop = asyncio.get_event_loop()
    self.thread_id = threading.current_thread().ident

  def __call__(self, task):
    if threading.current_thread().ident == self.thread_id:
      asyncio.create_task(task)
    else:
      asyncio.run_coroutine_threadsafe(
        task, self.loop).add_done_callback(self._on_finished)

  def _on_finished(self, f):
    try:
      f.result()
    except Exception:
      print('exception in posted task')
      traceback.print_exc()

class CheckThread:
  def __init__(self):
    self._thread_id = threading.current_thread().ident
    self._thread_name = threading.current_thread().name

  def check(self):
    assert self._thread_id == threading.current_thread().ident, \
      'Function is called on {} thread instead of {}'.format(threading.current_thread().name, self._thread_name)

def check_thread(f):
  if inspect.iscoroutinefunction(f):
    async def _impl(self, *args, **kwargs):
      self._check_thread.check()
      return await f(self, *args, **kwargs)
  else:
    def _impl(self, *args, **kwargs):
      self._check_thread.check()
      return f(self, *args, **kwargs)
  return _impl

class Checker:
  _instances: dict[InstanceId, Instance]

  def __init__(self):
    self._instances = {}
    self._sleep_time = dt.timedelta(seconds=1)
    self._observers = []

    self._check_thread = CheckThread()
    self._post_task = PostTask()

  @check_thread
  def add_observer(self, o):
    self._observers.append(o)

  async def start(self):
    self._post_task(self._loop())
    
  def new_report(self, app: App, server: Server, report: PingReport):
    self._post_task(self._ping_event(ReportEvent(app, server, report)))

  @check_thread
  async def _loop(self):
    while True:
      self._process_events()
      await asyncio.sleep(self._sleep_time.total_seconds())

  @check_thread
  async def _ping_event(self, e: ReportEvent):
    target_instance = None
    for instance in self._instances.values():
      if instance.app == e.app and instance.server == e.server:
        target_instance = instance
        break
    if target_instance is None:
      new_instance = Instance(InstanceId(uuid.uuid4()), e.server, e.app, None, {}, [])
      self._instances[new_instance.id] = new_instance
      target_instance = new_instance
    print(self._instances)
    target_instance.new_events.append(e.report)
    
  @check_thread
  def _process_events(self):
    now = dt.datetime.now(dt.UTC)
    collected_notifications = {}
    print(self._instances)
    for instance in self._instances.values():
      instance_notifications = self._process_instance(now, instance)
      collected_notifications[instance.id] = instance_notifications
    if collected_notifications:
      self._send_notifications(collected_notifications)

  @check_thread
  def _process_instance(self, now, instance: Instance):
    notifications : list[Notification] = []
    for event in instance.new_events:
      if not instance.active_run or event.run_id != instance.active_run.run_id:
        if event.run_id in instance.recently_runs:
          print('skip event')
          continue
        if instance.active_run:
          print('run failed')
          notifications.append(RunFailed(instance.active_run))
          instance.recently_runs[instance.active_run.run_id] = FailedRun(instance.active_run.run_id, now)
        instance.active_run = Run(instance.id, event.run_id, {})
        notifications.append(NewRun(instance.active_run))
      assert event.run_id == instance.active_run.run_id
      if event.ping_id not in instance.active_run.pings:
        print('create ping')
        ping = Ping(event.ping_id, event.valid_for, created_at=now, last_ping=event.ping_sent_at, state=PingState.RUNNING)
        instance.active_run.pings[ping.name] = ping
        notifications.append(NewPing(instance.active_run, ping))
      ping = instance.active_run.pings[event.ping_id]
      print(ping.last_ping, event.ping_sent_at)
      if ping.last_ping <= event.ping_sent_at:
        print('update some time')
        ping.last_ping = event.ping_sent_at
        ping.valid_for = event.valid_for
    assert instance.active_run
    for ping in instance.active_run.pings.values():
      state = PingState.RUNNING
      delta = now - ping.last_ping
      print('delta=', delta)
      print('valid_for=', ping.valid_for)
      if delta > 2 * ping.valid_for:
        state = PingState.FAILED
      elif delta > ping.valid_for:
        state = PingState.PROBABLY_FAILED
      else: 
        state = PingState.RUNNING
      if state != ping.state:
        ping.state = state
        notifications.append(PingStateChange(instance.active_run, ping, ping.state))
    return notifications

  @check_thread
  def _send_notifications(self, events):
    for o in self._observers:
      o.on_events(events)

class Printer:
  def on_events(self, ev):
    for _, i in ev.items():
      for e in i: 
        print(e)

class RunningAppsService:
  def __init__(self):
    self._apps = {}
    self._check_thread = AsyncioThread()
    self._check_thread.start()
    self._checker = Checker()
    self._printer = Printer()
    asyncio.run_coroutine_threadsafe(self._checker.start(), self._check_thread.loop).result()
    asyncio.run_coroutine_threadsafe(self._checker.add_observer(self._printer), self._check_thread.loop).result()
    
  def report(self, report):
    app = App(report["run"]["app"]["name"], report["run"]["app"]["version"])
    server = Server(report["run"]["server"]["id"])
    event = PingReport(
      report["run"]["id"],
      report["ping"]["name"],
      report["ping"]["time"],
      report["ping"]["valid_for"],
    )
    self._add_event(server, app, event)

  def stop(self):
    self._checker_thread.stop()

  def _add_event(self, server: Server, app: App, event: Event):
    self._checker.new_report(server, app, event)

  
# TODO: add authorization/password protection
if __name__ == "__main__":
  s = RunningAppsService()
  report = {
    "run": {
      "app": {
        "name": "test",
        "version": "1.0",
      },
      "server": {
        "id": "server id",
      },
      "id": uuid.uuid4(),
    },
    "ping": {
      "name": "test ping",
      "time": dt.datetime.now(dt.UTC),
      "valid_for": dt.timedelta(seconds=10),
    }
  }
  schemas.PingSchema().validate(report)
  s.report(schemas.PingSchema().load(report))