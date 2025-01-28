from freezegun import freeze_time
import pytest

from . import running_apps
import datetime as dt
import asyncio
import uuid
from collections import defaultdict

class EventCollector:
  def __init__(self):
    self.reset_events()

  def on_events(self, ev):
    for k, e in ev.items():
      self.events[k].extend(e)

  def reset_events(self):
    self.events = defaultdict(list)

class EventWaiter:
  def __init__(self, service, predicate):
    self.service = service
    self._event = asyncio.Event()
    self._predicate = predicate

  def on_events_changed(self):
    if self._predicate(self.service.events):
      self._event.set()
      self._predicate = lambda: None

  async def wait(self):
    await self._event.wait()

class Service:
  @classmethod
  async def create(cls):
    self = cls()
    self.service = running_apps.Checker()
    await self.service.start()
    self.service.add_observer(self)
    self.events = defaultdict(list)
    self._observer = []
    return self

  def add_observer(self, o):
    self._observer.append(o)

  def send_ping(self, app, server, report: running_apps.PingReport):
    self.service.new_report(app, server, report)

  def wait_events(self, size):
    loop = asyncio.new_event_loop()
    loop.run_forever()

  def on_events(self, ev):
    for k, e in ev.items():
      self.events[k].append(e)
    for o in self._observer:
      o.on_events_changed()

@pytest.mark.asyncio
async def test_reports():
  app = running_apps.App("app", "1.1")
  server = running_apps.Server(uuid.uuid4())
  run_id = uuid.uuid4()

  with freeze_time("2024-10-01") as time:
    events_collector = EventCollector()
    service = running_apps.Checker()
    service.add_observer(events_collector)

    now = dt.datetime.now(dt.UTC)
    await service._ping_event(running_apps.ReportEvent(app, server, running_apps.PingReport(
      run_id, 'ping1', ping_sent_at=now - dt.timedelta(seconds=2), valid_for=dt.timedelta(seconds=5)
    )))

    service._process_events()

    assert len(service._instances) == 1
    instance = next(i for i in service._instances.values() if i.app == app and i.server == server)
    ping = running_apps.Ping('ping1', dt.timedelta(seconds=5), created_at=now, last_ping=now - dt.timedelta(seconds=2), state=running_apps.PingState.RUNNING)
    run = running_apps.Run(instance.id, run_id, {
            'ping1': ping,
          })
    assert dict(events_collector.events) == { instance.id: [
        running_apps.NewRun(
          run,
        ),
        running_apps.NewPing(
          run,
          ping,
        )
      ]
    }

    events_collector.reset_events()
    time.tick(dt.timedelta(seconds=5))

    service._process_events()
    ping.state = running_apps.PingState.PROBABLY_FAILED

    assert dict(events_collector.events) == { instance.id: [
        running_apps.PingStateChange(
          run,
          ping,
          running_apps.PingState.PROBABLY_FAILED,
        ),
      ]
    }

    events_collector.reset_events()

    time.tick(dt.timedelta(seconds=5))

    service._process_events()
    ping.state = running_apps.PingState.FAILED

    assert dict(events_collector.events) == { instance.id: [
        running_apps.PingStateChange(
          run,
          ping,
          running_apps.PingState.FAILED,
        ),
      ]
    }

    events_collector.reset_events()
    time.tick(dt.timedelta(seconds=5))

    now = dt.datetime.now(dt.UTC)
    ping.last_ping = now - dt.timedelta(seconds=3)
    ping.state = running_apps.PingState.RUNNING
    await service._ping_event(running_apps.ReportEvent(app, server, running_apps.PingReport(
      run_id, 'ping1', now - dt.timedelta(seconds=3), dt.timedelta(seconds=5)
    )))
    service._process_events()
    assert dict(events_collector.events) == { instance.id: [
        running_apps.PingStateChange(
          run,
          ping,
          running_apps.PingState.RUNNING,
        ),
      ]
    }