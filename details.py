# -*- coding: euc-kr -*-
"""
Created on 2021. 3. 4.
Author: jm.shin@pointi.com
"""
import pandas as pd
import time


def convert_stat_details(src_path):
    # csv ������ ��ο��� �о���� DataFrame(df)�� �־��ݴϴ�. encoding ������ ����� �ѱ��� �ȱ���.
    time.sleep(2)
    df = pd.read_csv(src_path, encoding='utf-8-sig', low_memory=False)

    # df���� Ư�� �÷��� �����մϴ�.
    df_select = df[['reg_dtti_st', 'cust_id', 'area_nm', 'nw_cd', 'l3_rtt_avg', 'l3_rtt_min', 'l3_rtt_max',
                    'l3_loss_count', 'l3_loss_rate', 'l2_rtt_avg', 'l2_rtt_min', 'l2_rtt_max', 'l2_loss_count',
                    'l2_loss_rate', 'hgw_delay_down_avg', 'hgw_delay_down_min', 'hgw_delay_down_max',
                    'hgw_delay_down_stdev', 'hgw_loss_up_count', 'hgw_loss_up_rate', 'hgw_loss_down_count',
                    'hgw_loss_down_rate', 'hgw_jitter_up_avg', 'hgw_jitter_up_min', 'hgw_jitter_up_max',
                    'hgw_jitter_up_stdev',
                    'hgw_jitter_down_avg', 'hgw_jitter_down_min', 'hgw_jitter_down_max', 'hgw_jitter_down_stdev',
                    'hgw_reorder_up_count',
                    'hgw_reorder_up_rate', 'hgw_reorder_down_count', 'hgw_reorder_down_rate',
                    'hgw_duplication_up_count',
                    'hgw_duplication_up_rate', 'hgw_duplication_down_count', 'hgw_duplication_down_rate', 'hgw_rtt_avg',
                    'hgw_rtt_min', 'hgw_rtt_max', 'hgw_rtt_stdev', 'targ_tp_cd']]

    # ������ df_selecte�� ���� ���͸� (targ_tp_cd = MAIN)
    main_data = df_select[df_select['targ_tp_cd'] == 'MAIN']

    # [l3 �����]
    # l3�� �����մϴ�. ����: l3_loss_rate�� 100�� ���� ���� and l3_rtt_avg�� null�̸� ����
    l3_condition = main_data[(main_data['l3_loss_rate'] != 100) & (main_data['l3_rtt_avg'].notnull())]
    # l3 Ư�� �÷��� �����ؼ� ����,ȸ�� �������� �׷�ȭ �� ���
    l3 = l3_condition[['area_nm', 'nw_cd', 'l3_rtt_avg', 'l3_rtt_min',
                       'l3_rtt_max', 'l3_loss_count', 'l3_loss_rate']].groupby(['area_nm', 'nw_cd']).describe()

    # [l2 �����]
    # l3�� ����, �׷�ȭ ����
    l2_condition = main_data[(main_data['l2_loss_rate'] != 100) & (main_data['l2_rtt_avg'].notnull())]
    l2 = l2_condition[['area_nm', 'nw_cd', 'l2_rtt_avg', 'l2_rtt_min',
                       'l2_rtt_max', 'l2_loss_count', 'l2_loss_rate']].groupby(['area_nm', 'nw_cd']).describe()

    # [Ping, TWAMP ����� �κ�]
    # Ping, TWAMP�� ���� ������ hgw_loss_down_rate, hgw_reorder_down_rate = 100�� �� ����
    remove_rate = main_data[(main_data['hgw_loss_down_rate'] != 100) &
                            (main_data['hgw_reorder_down_rate'] != 100) & (main_data['hgw_rtt_avg'].notnull())]

    # ���߿��� �ʿ��� �����͸� �����ؼ� ping�� twamp�� ����� ���ø��� ����.
    common_format = remove_rate[['area_nm', 'nw_cd', 'hgw_delay_down_avg', 'hgw_delay_down_min', 'hgw_delay_down_max',
                                 'hgw_delay_down_stdev', 'hgw_loss_up_count', 'hgw_loss_up_rate', 'hgw_loss_down_count',
                                 'hgw_loss_down_rate', 'hgw_jitter_up_avg', 'hgw_jitter_up_min', 'hgw_jitter_up_max',
                                 'hgw_jitter_up_stdev',
                                 'hgw_jitter_down_avg', 'hgw_jitter_down_min', 'hgw_jitter_down_max',
                                 'hgw_jitter_down_stdev',
                                 'hgw_reorder_up_count',
                                 'hgw_reorder_up_rate', 'hgw_reorder_down_count', 'hgw_reorder_down_rate',
                                 'hgw_duplication_up_count',
                                 'hgw_duplication_up_rate', 'hgw_duplication_down_count', 'hgw_duplication_down_rate',
                                 'hgw_rtt_avg',
                                 'hgw_rtt_min', 'hgw_rtt_max', 'hgw_rtt_stdev', 'targ_tp_cd']]

    # [Ping]
    # ping ����: hgw_delay_down_avg�� isnull()
    ping_condition = common_format[common_format['hgw_delay_down_avg'].isnull()]

    # ping Ư�� �÷����� �׷�ȭ �� ���
    ping = ping_condition[['area_nm', 'nw_cd', 'hgw_rtt_avg', 'hgw_rtt_min', 'hgw_rtt_max',
                           'hgw_loss_down_count', 'hgw_loss_down_rate']].groupby(['area_nm', 'nw_cd']).describe()

    # TWAMP
    # TWAMP�� ����: hgw_delay_down_avg�� notnull()
    twamp_condition = common_format[common_format['hgw_delay_down_avg'].notnull()]

    # TWAMP Ư�� �÷����� �׷�ȭ �� ���
    twamp = twamp_condition[['area_nm', 'nw_cd', 'hgw_rtt_avg', 'hgw_rtt_min', 'hgw_rtt_max',
                             'hgw_loss_down_count', 'hgw_loss_down_rate', 'hgw_delay_down_avg',
                             'hgw_delay_down_min', 'hgw_delay_down_max', 'hgw_jitter_down_avg',
                             'hgw_jitter_down_min', 'hgw_jitter_down_max', 'hgw_reorder_down_count',
                             'hgw_reorder_down_rate', 'hgw_duplication_down_count',
                             'hgw_duplication_down_rate']].groupby(['area_nm', 'nw_cd']).describe()

    # ��� ���(l3,l2,ping,twamp) ����.
    # l3�� l2 ����
    merge_l3_l2 = pd.merge(l3, l2, on=['area_nm', 'nw_cd'], how='outer')

    # ping�� twamp ����
    merge_ping_twamp = pd.merge(ping, twamp, on=['area_nm', 'nw_cd'], how='outer')

    # (l3,l2) + (ping, twamp)
    final_data = pd.merge(merge_l3_l2, merge_ping_twamp, on=['area_nm', 'nw_cd'], how='outer')

    # ���������͸� csv ���Ϸ� ��������
    result_dir = src_path.replace('.txt', '.csv')
    final_data.to_csv(result_dir, float_format='%.3f', encoding='utf-8-sig')

if __name__ == '__main__':
    path = ''
    filename = ''
    convert_stat_details(path, filename)
