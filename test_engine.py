import datetime as dt

from engine.event_loop import EventLoop
from engine.timer import RepeatingTimer, Timer


def main():
    loop = EventLoop()
    timer = RepeatingTimer(dt.timedelta(seconds=10), lambda: print('10', dt.datetime.now().time()))
    timer2 = RepeatingTimer(dt.timedelta(seconds=7), lambda: print(' 7', dt.datetime.now().time()))
    timer3 = Timer(dt.timedelta(seconds=30), lambda: (print('exit'), loop.stop()))
    timer.start()
    timer2.start()
    timer3.start()
    loop.start()

    
if __name__ == "__main__":
    main()