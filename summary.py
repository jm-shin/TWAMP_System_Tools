# -*- coding: euc-kr -*-
import pandas as pd
from pandas import DataFrame

# 메인 함수
if __name__ == "__main__":
    # csv file 읽어오기 (summary, details file), 인코딩을 utf-8-sig로 설정해야 한글이 깨지지않는다. 그리고 메모리옵션을 꺼준다.
    # df = DataFrame 줄임말. 또한, 불러올 경로 및 파일은 수동으로 지정해줘야함.
    summary_df = pd.read_csv('C:/Users/82103/Desktop/result/20210219_SUMMARY.txt', encoding='utf-8-sig', low_memory=False)
    details_df = pd.read_csv('C:/Users/82103/Desktop/result/20210219_DETAILS.txt', encoding='utf-8-sig', low_memory=False)

    # 읽어온 details file에서 필요한 columns만 가져온다.
    data_details = details_df[['area_nm', 'nw_cd', 'hgw_mac_addr']].copy()

    # 병합을 위해 details의 hgw_mac_addr 컬럼명을 hgw_mac으로 변경
    data_details.rename(columns={'hgw_mac_addr':'hgw_mac'}, inplace=True)

    # 중복된 맥주소 제거
    unique_mac_address = DataFrame(data_details).drop_duplicates(['hgw_mac'], keep='last')

    # summary와 details 병합
    merged_data = pd.merge(summary_df, unique_mac_address, how='left', on='hgw_mac')
    print(merged_data)

    # column 출력 순서를 맞춰주기 위해서 작성
    sort_data = merged_data[['area_nm', 'nw_cd', 'nw_dm_dtti', 'cust_id', 'dm_type', 'targ_tp_cd', 'nw_problem', 'device_problem',
                     'sop_msg', 'hgw_delay_up_avg', 'hgw_delay_up_min', 'hgw_delay_up_max', 'hgw_delay_up_stdev', 'hgw_delay_down_avg',
                     'hgw_delay_down_min', 'hgw_delay_down_max', 'hgw_delay_down_stdev', 'hgw_jitter_up_avg', 'hgw_jitter_up_min',
                     'hgw_jitter_up_max', 'hgw_jitter_up_stdev', 'hgw_jitter_down_avg', 'hgw_jitter_down_min', 'hgw_jitter_down_max',
                     'hgw_jitter_down_stdev', 'hgw_loss_up_count', 'hgw_loss_up_percent', 'hgw_reorder_up_count', 'hgw_reorder_up_percent',
                     'hgw_dup_up_count', 'hgw_dup_up_percent', 'hgw_dup_down_count', 'hgw_dup_down_percent', 'hgw_rt_rtt_avg','hgw_rt_rtt_min',
                     'hgw_rt_rtt_max', 'hgw_rt_rtt_stdev', 'hgw_rt_loss_count', 'hgw_rt_loss_percent', 'l3_rt_rtt_avg', 'l3_rt_rtt_min',
                    'l3_rt_rtt_max', 'l3_rt_rtt_stdev', 'l3_rt_loss_count', 'l3_rt_loss_percent', 'l2_rt_rtt_avg', 'l2_rt_rtt_min', 'l2_rt_rtt_max',
                     'l2_rt_rtt_stdev', 'l2_rt_loss_count', 'l2_rt_loss_percent', 'hgw_mac', 'l3_ip', 'l3_type', 'l2_ip', 'l2_type','duration','quality']]

    # sort된 data를 csv로 지정된 경로에 출력
    DataFrame(sort_data).to_csv('C:/Users/82103/Desktop/result/summary_result.csv', encoding='utf-8-sig')