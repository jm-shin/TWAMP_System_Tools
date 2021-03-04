# -*- coding: euc-kr -*-
'''
Created on 2021. 2. 26.
'''
import time, os
import pandas as pd
from pandas import DataFrame
import json

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Event Hander Class
class MyHandler(FileSystemEventHandler):
    # Member �ʱ�ȭ
    target_file_types = ['txt', 'csv']
    cp_dir_src = "../../Desktop"
    cp_dir_dest = "../../Desktop"

    def __init__(self, a, b, c):
        target_file_types, cp_dir_src, cp_dir_dest = a, b, c
        print("-------Event Handler Start!!----------")

    def on_created(self, event):
        print("[+] Files are Created in the Target Folder !!!")
        print("  L___", event.event_type, event.src_path)

        self.do_action(event)

    def get_fileinfo_from_fullpath(self,full_path):
        file_path, file_ext = os.path.splitext(full_path)
        file_name = os.path.basename(full_path)
        return file_path,file_name,file_ext

    def make_stat_file(self, file_path, cp_dir_dest, file_ext):
        # read csv
        df = pd.read_csv(file_path+file_ext, encoding='utf-8-sig', low_memory=False)

        # select columns
        df2 = df[['reg_dtti_st', 'cust_id', 'area_nm', 'nw_cd', 'l3_rtt_avg', 'l3_rtt_min', 'l3_rtt_max',
                  'l3_loss_count', 'l3_loss_rate', 'l2_rtt_avg', 'l2_rtt_min', 'l2_rtt_max', 'l2_loss_count',
                  'l2_loss_rate', 'hgw_delay_down_avg', 'hgw_delay_down_min', 'hgw_delay_down_max',
                  'hgw_delay_down_stdev', 'hgw_loss_up_count', 'hgw_loss_up_rate', 'hgw_loss_down_count',
                  'hgw_loss_down_rate', 'hgw_jitter_up_avg', 'hgw_jitter_up_min', 'hgw_jitter_up_max', 'hgw_jitter_up_stdev',
                  'hgw_jitter_down_avg', 'hgw_jitter_down_min', 'hgw_jitter_down_max', 'hgw_jitter_down_stdev', 'hgw_reorder_up_count',
                  'hgw_reorder_up_rate', 'hgw_reorder_down_count', 'hgw_reorder_down_rate', 'hgw_duplication_up_count',
                  'hgw_duplication_up_rate', 'hgw_duplication_down_count', 'hgw_duplication_down_rate', 'hgw_rtt_avg',
                  'hgw_rtt_min', 'hgw_rtt_max', 'hgw_rtt_stdev', 'targ_tp_cd']]

        # main data extract from csv file
        main_filter = df2[df2['targ_tp_cd'] == 'MAIN']

        # [l3,l2]
        # local grouping to main data
        l3l2_local_grouping = DataFrame(main_filter).groupby([main_filter['area_nm'], main_filter['nw_cd']]).describe()

        # select l3,l2 columns
        l3l2_result = l3l2_local_grouping[['l3_rtt_avg', 'l3_rtt_min', 'l3_rtt_max', 'l3_loss_count', 'l3_loss_rate',
                                           'l2_rtt_avg', 'l2_rtt_min', 'l2_rtt_max', 'l2_loss_count', 'l2_loss_rate']]

        # [Ping]
        # condition filter
        ping_condition = main_filter[
            (main_filter['hgw_delay_down_avg'].isnull()) & ((main_filter['hgw_loss_down_count'] > 0)
                                                            | (main_filter['hgw_rtt_avg'].notnull()))]

        ping_local_grouping = ping_condition.groupby([ping_condition['area_nm'], ping_condition['nw_cd']]).describe()

        # select columns
        ping_result = ping_local_grouping[
            ['hgw_rtt_avg', 'hgw_rtt_min', 'hgw_rtt_max', 'hgw_loss_down_count', 'hgw_loss_down_rate']]

        # [TWAMP]
        # condition filter
        twamp_condition = main_filter[
            (main_filter['hgw_delay_down_avg'].notnull()) & (main_filter['hgw_loss_down_count'].notnull())
            & (main_filter['hgw_jitter_down_avg'].notnull()) & (main_filter['hgw_reorder_down_count'].notnull())
            & (main_filter['hgw_rtt_avg'].notnull())]

        twamp_local_grouping = twamp_condition.groupby(
            [twamp_condition['area_nm'], twamp_condition['nw_cd']]).describe()

        # select columns
        twamp_result = twamp_local_grouping[
            ['hgw_rtt_avg', 'hgw_rtt_min', 'hgw_rtt_max', 'hgw_loss_down_count', 'hgw_loss_down_rate',
             'hgw_delay_down_avg', 'hgw_delay_down_min', 'hgw_delay_down_max', 'hgw_jitter_down_avg',
             'hgw_jitter_down_min', 'hgw_jitter_down_max', 'hgw_reorder_down_count', 'hgw_reorder_down_rate',
             'hgw_duplication_down_count', 'hgw_duplication_down_rate']]

        # merge
        merged_data = pd.merge(l3l2_result, pd.merge(ping_result, twamp_result, on=['area_nm', 'nw_cd'], how='outer',
                                                     suffixes=('_PING', '_TWAMP')), on=['area_nm', 'nw_cd'],how='outer')
        print(merged_data)

        # to csv_file
        file_ext = '.csv'
        merged_data.to_csv(file_path+file_ext, float_format='%.3f', encoding='utf-8-sig')

    # �̺�Ʈ �߻� ��, ���� ������ �۾��� ����
    def do_action(self, event):
        file_path, file_name, file_ext = self.get_fileinfo_from_fullpath(event.src_path)
        print("  L___path:%s, name:%s, ext:%s\n" % (file_path, file_name, file_ext))

        # Config ���Ͽ��� ������, ��� ext���� Ȯ���Ͽ�,
        # �̺�Ʈ�� ������ ext�� ������ �Ǿ� ������, ������ �����Ѵ�.
        for ext in target_file_types:
            if ext == file_ext[1:]:
                print(' [+] type:%s, Infected !! Copying info.' % ext)
                self.make_stat_file(file_path, cp_dir_dest, file_ext)
        observer.stop()

if __name__ == "__main__":
    with open("config.json") as f:
        config = json.load(f)

    # ��� ���� �Է�
    target_dir = config['target_dir']
    temp_target_file_types = config['temp_target_file_types']
    cp_dir_src = config['cp_dir_src']
    cp_dir_dest = config['cp_dir_dest']

    # Ÿ�� �������� �ټ��� ���� ���� --> '|'�� �����ڷ� �Ѵ�.
    target_file_types = temp_target_file_types.split("|")

    print("Monitoring Start - target_dir : %s" % target_dir)
    print("target_file_type : %s" % target_file_types[0])

    # �̺�Ʈ �ڵ鷯�� �����ϰ�, �ʱ�ȭ
    event_handler = MyHandler(target_file_types, cp_dir_src, cp_dir_dest)

    # ����͸� Ŭ���� �����ϰ�, �̺�Ʈ �ڵ鷯�� �ٿ��� ���� ����
    observer = Observer()
    observer.schedule(event_handler, path=target_dir, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()