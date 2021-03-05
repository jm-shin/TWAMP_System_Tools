# -*- coding: euc-kr -*-
'''
Created on 2021. 2. 26.
'''
import os
import time

from details import convert_stat_details

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class Target:
    watchDir = os.getcwd()

    def __init__(self):
        self.observer = Observer()

    def run(self):
        event_handler = Handler()
        self.observer.schedule(event_handler, self.watchDir, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(1)
        except:
            self.observer.stop()
            print("Error")

        self.observer.join()


class Handler(FileSystemEventHandler):
    # FileSystemEventHandler 클래스를 상속받음.
    # 아래 핸들러들을 오버라이드 함
    def on_created(self, event):
        print(event)
        convert_stat_details(event.src_path)


if __name__ == "__main__":
    w = Target()
    w.run()
