# -*- coding: euc-kr -*-
"""
Created on 2021. 3. 4.
Author: jm.shin@pointi.com
"""
import pandas as pd
import time


def convert_stat_details(src_path):
    # csv 파일을 경로에서 읽어오고 DataFrame(df)에 넣어줍니다. encoding 설정을 해줘야 한글이 안깨짐.
    time.sleep(2)
    df = pd.read_csv(src_path, encoding='utf-8-sig', low_memory=False)

    # df에서 특정 컬럼을 선택합니다.
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

    # 가져온 df_selecte를 조건 필터링 (targ_tp_cd = MAIN)
    main_data = df_select[df_select['targ_tp_cd'] == 'MAIN']

    # [l3 만들기]
    # l3를 생성합니다. 조건: l3_loss_rate가 100인 행은 제거 and l3_rtt_avg가 null이면 제거
    l3_condition = main_data[(main_data['l3_loss_rate'] != 100) & (main_data['l3_rtt_avg'].notnull())]
    # l3 특정 컬럼만 추출해서 지역,회선 기준으로 그룹화 및 통계
    l3 = l3_condition[['area_nm', 'nw_cd', 'l3_rtt_avg', 'l3_rtt_min',
                       'l3_rtt_max', 'l3_loss_count', 'l3_loss_rate']].groupby(['area_nm', 'nw_cd']).describe()

    # [l2 만들기]
    # l3와 조건, 그룹화 동일
    l2_condition = main_data[(main_data['l2_loss_rate'] != 100) & (main_data['l2_rtt_avg'].notnull())]
    l2 = l2_condition[['area_nm', 'nw_cd', 'l2_rtt_avg', 'l2_rtt_min',
                       'l2_rtt_max', 'l2_loss_count', 'l2_loss_rate']].groupby(['area_nm', 'nw_cd']).describe()

    # [Ping, TWAMP 공통된 부분]
    # Ping, TWAMP의 공통 조건인 hgw_loss_down_rate, hgw_reorder_down_rate = 100인 행 제거
    remove_rate = main_data[(main_data['hgw_loss_down_rate'] != 100) &
                            (main_data['hgw_reorder_down_rate'] != 100) & (main_data['hgw_rtt_avg'].notnull())]

    # 그중에서 필요한 데이터만 추출해서 ping과 twamp의 공통된 템플릿을 만듬.
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
    # ping 조건: hgw_delay_down_avg가 isnull()
    ping_condition = common_format[common_format['hgw_delay_down_avg'].isnull()]

    # ping 특정 컬럼으로 그룹화 및 통계
    ping = ping_condition[['area_nm', 'nw_cd', 'hgw_rtt_avg', 'hgw_rtt_min', 'hgw_rtt_max',
                           'hgw_loss_down_count', 'hgw_loss_down_rate']].groupby(['area_nm', 'nw_cd']).describe()

    # TWAMP
    # TWAMP의 조건: hgw_delay_down_avg가 notnull()
    twamp_condition = common_format[common_format['hgw_delay_down_avg'].notnull()]

    # TWAMP 특정 컬럼으로 그룹화 및 통계
    twamp = twamp_condition[['area_nm', 'nw_cd', 'hgw_rtt_avg', 'hgw_rtt_min', 'hgw_rtt_max',
                             'hgw_loss_down_count', 'hgw_loss_down_rate', 'hgw_delay_down_avg',
                             'hgw_delay_down_min', 'hgw_delay_down_max', 'hgw_jitter_down_avg',
                             'hgw_jitter_down_min', 'hgw_jitter_down_max', 'hgw_reorder_down_count',
                             'hgw_reorder_down_rate', 'hgw_duplication_down_count',
                             'hgw_duplication_down_rate']].groupby(['area_nm', 'nw_cd']).describe()

    # 모든 통계(l3,l2,ping,twamp) 병합.
    # l3와 l2 병합
    merge_l3_l2 = pd.merge(l3, l2, on=['area_nm', 'nw_cd'], how='outer')

    # ping과 twamp 병합
    merge_ping_twamp = pd.merge(ping, twamp, on=['area_nm', 'nw_cd'], how='outer')

    # (l3,l2) + (ping, twamp)
    final_data = pd.merge(merge_l3_l2, merge_ping_twamp, on=['area_nm', 'nw_cd'], how='outer')

    # 최종데이터를 csv 파일로 내보내기
    result_dir = src_path.replace('.txt', '.csv')
    final_data.to_csv(result_dir, float_format='%.3f', encoding='utf-8-sig')

if __name__ == '__main__':
    path = ''
    filename = ''
    convert_stat_details(path, filename)
