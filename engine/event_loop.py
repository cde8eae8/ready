import threading
import typing
import queue
import traceback

class TaskEvent:
  id = 'task'
  code: typing.Callable[[], None]

  def __init__(self, code):
    self.code = code

class EventLoop:
  _instance = threading.local()

  @staticmethod
  def for_current_thread() -> "EventLoop":
    return EventLoop._instance.value

  def __init__(self):
    EventLoop._instance.value = self
    self._queue = queue.Queue()
    self._handlers = {}
    self._finished = threading.Event()
    self.add_handler(TaskEvent.id, self._run_task)

  def start(self):
    self._loop()

  def stop(self):
    print('STOP')
    self._finished.set()

  def add_event(self, event):
    self._queue.put(event)

  def add_handler(self, id, handler):
    # TODO: make thread safe
    self._handlers[id] = handler

  def _loop(self):
    while True:
      e = None
      try:
        e = self._queue.get(timeout=1)
      except queue.Empty:
        continue
      if self._finished.is_set():
        return
      if not e:
        continue
      self._process(e)
      if self._finished.is_set():
        return

  def _process(self, e):
    try:
      if h := self._handlers.get(e.id):
        h(e)
      else:
        print('dropped event', e.id)
    except Exception:
      traceback.print_exc()

  def _run_task(self, task: TaskEvent):
    task.code()
