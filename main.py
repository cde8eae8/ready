import threading
import traceback
import uuid
import schemas
import enum
import typing
import datetime as dt
import dataclasses
import queue

PingKey = typing.NewType("PingKey", str)
RunId = typing.NewType("RunId", uuid.UUID)
InstanceId = typing.NewType("InstanceId", uuid.UUID)

@dataclasses.dataclass
class Ping:
  name: PingKey
  valid_for: dt.timedelta
  created_at: dt.datetime
  last_ping: dt.datetime

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
  time: dt.datetime
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
  instance: Instance
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
class PingFailed:
  run: Run
  ping: Ping

Notification = (
    NewRun
  | RunFailed
  | NewPing
  | PingFailed
)

@dataclasses.dataclass
class ReportEvent:
  id = 'ping_event'
  app: App
  server: Server
  report: PingReport

class CheckThread(threading.Thread):
  def __init__(self):
    super().__init__(target=self._main)
    self._ev_loop = EventLoop()

  def stop(self):
    self._ev_loop.stop()

  def _main(self):
    self._ev_loop.start()
    

class Checker:
  _instances: dict[InstanceId, Instance]

  def __init__(self):
    self._ev_loop = EventLoop()
    self._ev_loop.add_handler(ReportEvent.id, self._ping_event)
    self._timer = RepeatingTimer(dt.timedelta(minutes=1), self._process_events)

  def start(self):
    self._timer.start()
    self._ev_loop.start()
    
  def new_report(self, app: App, server: Server, report: PingReport):
    self._ev_loop.add_event(ReportEvent(app, server, report))

  def _ping_event(self, e: ReportEvent):
    target_instance = None
    for instance in self._instances.values():
      if instance.app == e.app and instance.server == e.server:
        target_instance = instance
        break
    if target_instance is None:
      new_instance = Instance(InstanceId(uuid.UUID()), e.server, e.app, None, {}, [])
      self._instances[new_instance.id] = new_instance
      target_instance = new_instance
    target_instance.new_events.append(e.report)
    

  def _process_events(self):
    now = dt.datetime.now(dt.UTC)
    collected_notifications = {}
    for instance in self._instances.values():
      instance_notifications = self._process_instance(now, instance)
      collected_notifications[instance.id] = instance_notifications
    self._send_notifications(collected_notifications)

  def _process_instance(self, now, instance: Instance):
    notifications : list[Notification] = []
    for event in instance.new_events:
      if not instance.active_run or event.run_id != instance.active_run.run_id:
        if event.run_id in instance.recently_runs:
          continue
        if instance.active_run:
          notifications.append(RunFailed(instance.active_run))
          instance.recently_runs[instance.active_run.run_id] = FailedRun(instance.active_run.run_id, now)
        instance.active_run = Run(instance, event.run_id, {})
        notifications.append(NewRun(instance.active_run))
      assert event.run_id == instance.active_run.run_id
      if event.ping_id not in instance.active_run.pings:
        ping = Ping(event.ping_id, event.valid_for, now, now)
        instance.active_run.pings[ping.name] = ping
        notifications.append(NewPing(instance.active_run, ping))
      ping = instance.active_run.pings[event.ping_id]
      if ping.last_ping > event.time:
        ping.last_ping = event.time
        ping.valid_for = event.valid_for
    assert instance.active_run
    for ping in instance.active_run.pings.values():
      if ping.last_ping + ping.valid_for < now:
        notifications.append(PingFailed(instance.active_run, ping))
    return notifications


class RunningAppsService:
  def __init__(self):
    self._apps = {}
    self._check_thread = CheckThread()
    self._check_thread.start()
    
  def report(self, report):
    app = App(report["run"]["app"]["app_name"], report["run"]["app"]["app_version"])
    server = Server(report["run"]["server"]["id"])
    event = PingReport(
      report["run"]["id"],
      report["ping"]["name"],
      report["ping"]["time"],
      report["ping"]["valid_for"]
    )
    self._add_event(server, app, event)

  def _add_event(self, server: Server, app: App, event: Event):
    pass
