# -*- coding: euc-kr -*-
'''
Created on 2021. 2. 26.
'''
import os
import time

from details import convert_stat_details
from summary import summary_add_local

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class Watcher:
    watchDir = os.getcwd()

    def __init__(self):
        self.observer = Observer()
        self.current_directory_alarm()

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

    def current_directory_alarm(self):
        print("===================================================================")
        print("                  [ TWAMP_System_Tools ]")
        print("현재 감지 디렉토리:", end=" ")
        os.chdir(self.watchDir)
        print("{cwd}".format(cwd=os.getcwd()))
        print("===================================================================")


class Handler(FileSystemEventHandler):
    # FileSystemEventHandler 클래스를 상속받고, 아래 핸들러들을 오버라이드 함

    # 파일, 폴더 생성시 실행
    def on_created(self, event):
        print("[+] Files are Created in the Target Folder!")
        print("  L___", event.event_type, event.src_path)
        if 'DETAILS.txt' in event.src_path:
            print('L___ *.details.txt File detected!')
            convert_stat_details(event.src_path)
        if 'SUMMARY.txt' in event.src_path:
            print('L___ *.summary.txt File detected!')
            summary_add_local(event.src_path)


if __name__ == '__main__':
    w = Watcher()
    w.run()
