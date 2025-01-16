import dataclasses
import functools
import threading
import typing
import datetime as dt
import heapq
from .event_loop import EventLoop, TaskEvent

@dataclasses.dataclass(frozen=True)
@functools.total_ordering
class TimerTask:
  call_time: dt.datetime
  callback: typing.Callable[[], None]

  def is_due(self):
    return self.call_time >= dt.datetime.now(dt.UTC)

  def __lt__(self, o):
    return self.call_time < o.call_time

  def __eq__(self, o):
    return self.call_time == o.call_time

Sentinel = TimerTask(dt.datetime.max.replace(tzinfo=dt.UTC), lambda: None)

class TimerThread:
  @classmethod
  def get_instance(cls) -> "TimerThread":
    if not hasattr(cls, 'instance'):
      cls._instance = TimerThread()
    return cls._instance

  def __init__(self):
    self._thread = threading.Thread(target=self._main)
    self._queue : list[TimerTask] = [Sentinel]
    self._m = threading.Lock()
    self._cv = threading.Condition(self._m)
    self._thread.setName("Timer")
    self._thread.setDaemon(True)
    self._thread.start()

  def _main(self):
    while True:
      with self._m:
        now = dt.datetime.now(dt.UTC)
        current_earliest_task = self._queue[0]
        self._cv.wait_for(lambda: self._queue[0].call_time < current_earliest_task.call_time, 
                          timeout=(min(dt.timedelta(days=1), current_earliest_task.call_time - now)).total_seconds())
        now = dt.datetime.now(dt.UTC)
        while self._queue[0] != Sentinel and self._queue[0].call_time < now:
          heapq.heappop(self._queue).callback()

  def call_after(self, time: dt.timedelta, callback: typing.Callable[[], None]):
    with self._m:
      heapq.heappush(self._queue, TimerTask(dt.datetime.now(dt.UTC) + time, callback))
      self._cv.notify()
    
class Timer:
  def __init__(self, time: dt.timedelta, callback: typing.Callable[[], None]):
    self._time = time
    self._callback = callback
    self._ev_loop = EventLoop.for_current_thread()

  def start(self):
    TimerThread.get_instance().call_after(self._time, self._on_timeout)
    
  def _on_timeout(self):
    self._ev_loop.add_event(TaskEvent(self._callback))

class RepeatingTimer:
  def __init__(self, time: dt.timedelta, callback: typing.Callable[[], None]):
    self._time = time
    self._callback = callback
    self._is_running = False

  def start(self):
    self._is_running = True
    self._timer = Timer(self._time, self._on_timeout)
    self._timer.start()

  def _on_timeout(self):
    try:
      self._callback()
    finally: 
      self.start()
