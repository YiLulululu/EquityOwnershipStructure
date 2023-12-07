import sys
sys.path.append('../')

import pandas as pd
import os
import numpy as np
import matplotlib.pyplot as plt

import string
import itertools

import processHist
import utils

ctry = 'GB'

pj_base = '/nas/longleaf/home/yiluy/projects/BvDAnalysis/'
fd_base = '' # replace with your dir
bvd_fd = fd_base + 'bvd/'
sub_fd = fd_base + 'bvd_sub/'
header_fd = fd_base + 'bvd_header/'
bvd_proc_fd = fd_base + 'bvd_qualified/'
ind_fd = fd_base + 'bvd_full_header/'
output_fd = pj_base + 'output/'
hist_fd = fd_base + 'bvd_hist_2020_06/'
fin_hist_fd = hist_fd +  'Financials/'
des_hist_fd = hist_fd + 'Descriptive/'
own_hist_fd = hist_fd + 'Ownership/'
zephyr_fd = fd_base + 'Zephyr/'
additional_fd = pj_base + 'additional_data/'

class Zephyr():
    def __init__(self):
        df_deal = pd.read_csv(zephyr_fd + 'deal_combined_uk_subtype.csv')
        df_deal['Completed date'] = pd.to_datetime(df_deal['Completed date'])
        df_deal.drop(columns=['Unnamed: 0'], inplace=True)
        df_deal = df_deal[df_deal['Deal status']=='Completed'] # deal sub-type will create empty rows, need to fix this later
        df_deal['year'] = df_deal['Completed date'].dt.year
        df_deal['deal_subtype_mg'] = df_deal['deal_subtype_mg'].str.lower()
        df_deal['Deal type'] = df_deal['Deal type'].str.lower()

        df_deal.set_index('Deal Number', drop=False, inplace=True)
        deal_set = set(df_deal.index)

        # merge with dates
        temp_list = []
        for fname in ['deal_dates_1997_2005.xlsx', 'deal_dates_2006_2013.xlsx', 'deal_dates_2014_2021.xlsx']:
            temp = pd.read_excel(zephyr_fd + fname, sheet_name='Results')
            temp_list.append(temp)
        temp = pd.concat(temp_list, axis=0)
        temp.drop(columns=['Unnamed: 0', 'Target BvD ID number', 'Acquiror BvD ID number'], inplace=True)
        temp.set_index('Deal Number', inplace=True)
        df_deal = df_deal.merge(temp, how='left', left_index=True, right_index=True, validate='1:1')

        # merge with deal stake
        temp_list = []
        for fname in ['deal_stake_1997_2009.xlsx', 'deal_stake_2010_2021.xlsx']:
            temp = pd.read_excel(zephyr_fd + fname, sheet_name='Results')
            temp_list.append(temp)
        temp = pd.concat(temp_list, axis=0)
        temp.drop(columns=['Unnamed: 0', 'Target name', 'Acquiror name'], inplace=True)
        temp.drop_duplicates(subset=['Deal Number'], inplace=True)
        temp.set_index('Deal Number', inplace=True)
        df_deal = df_deal.merge(temp, how='left', left_index=True, right_index=True, validate='1:1')

        # merge with deal industries
        temp_list = []
        for fname in ['deal_industry_1997_2004.xlsx', 'deal_industry_2005_2009.xlsx', 'deal_industry_2010_2016.xlsx', 'deal_industry_2017_2021.xlsx']:
            temp = pd.read_excel(zephyr_fd + fname, sheet_name='Results')
            temp_list.append(temp)
        temp = pd.concat(temp_list, axis=0)
        temp.drop(columns=['Unnamed: 0', 'Target BvD ID number', 'Acquiror BvD ID number'], inplace=True)
        temp.drop_duplicates(subset=['Deal Number'], inplace=True)
        temp.set_index('Deal Number', inplace=True)
        df_deal = df_deal.merge(temp, how='left', left_index=True, right_index=True, validate='1:1')

        # fill in missing target id
        rto_fill = pd.read_csv(additional_fd + 'rto_target_nan_ready.csv')
        ipo_fill = pd.read_csv(additional_fd + 'ipo_target_nan_ready.csv')

        rto_fill.rename(columns={'Target BvD ID number':'fill_rto_target_id'}, inplace=True)
        ipo_fill.rename(columns={'Target BvD ID number':'fill_ipo_target_id'}, inplace=True)

        rto_fill.set_index('Deal Number', inplace=True)
        ipo_fill.set_index('Deal Number', inplace=True)

        df_deal = df_deal.merge(rto_fill[['fill_rto_target_id']], how='left', left_index=True, right_index=True, validate='1:1')
        df_deal = df_deal.merge(ipo_fill[['fill_ipo_target_id']], how='left', left_index=True, right_index=True, validate='1:1')

        df_deal.loc[df_deal['Target BvD ID number'].isna(), 'Target BvD ID number'] = df_deal.loc[df_deal['Target BvD ID number'].isna(), 'fill_rto_target_id']
        df_deal.loc[df_deal['Target BvD ID number'].isna(), 'Target BvD ID number'] = df_deal.loc[df_deal['Target BvD ID number'].isna(), 'fill_ipo_target_id']

        df_deal.drop(columns=['fill_rto_target_id', 'fill_ipo_target_id'], inplace=True)
        df_deal.to_csv(zephyr_fd + 'deal_combined_uk_subtype_filled.csv', index=False)

        self.df_deal = df_deal 
        self.deal_set = set(df_deal[df_deal['Target BvD ID number'].notna()]['Target BvD ID number'].unique())
        self.deal_acq_set = set(df_deal[df_deal['Acquiror BvD ID number'].notna()]['Acquiror BvD ID number'].unique())

    def extractDeals(self):
        df_deal = self.df_deal

        # extract reverse takeover deals
        deal_rt = df_deal[df_deal['deal_subtype_mg'].notna()]
        deal_rt = deal_rt[deal_rt['deal_subtype_mg'].str.contains('reverse')]
        self.rt_acq_set = set(deal_rt['Acquiror BvD ID number'].unique())
        self.rt_target_set = set(deal_rt['Target BvD ID number'].unique())
        df_temp = df_deal[~df_deal.index.isin(set(deal_rt.index))]

        # extract differnt deal types
        df_acq = df_temp[df_temp['Deal type'].str.contains('acquisition')]
        df_temp = df_temp[~df_temp['Deal type'].str.contains('acquisition')]

        temp_idx = (df_temp['Deal type'].str.contains('initial public')) | (df_temp['Deal type'].str.contains('ipo'))
        df_ipo = df_temp[temp_idx]
        df_temp = df_temp[~temp_idx]

        df_ms = df_temp[df_temp['Deal type'].str.contains('minority')]
        df_temp = df_temp[~df_temp['Deal type'].str.contains('minority')]

        df_ci = df_temp[df_temp['Deal type'].str.contains('capital increase')]
        df_temp = df_temp[~df_temp['Deal type'].str.contains('capital increase')]

        df_mg = df_temp[df_temp['Deal type'].str.contains('merger')]
        df_temp = df_temp[~df_temp['Deal type'].str.contains('merger')]

        temp_idx = (df_temp['Deal type'].str.contains('institutional buy-out')) | (df_temp['Deal type'].str.contains('institutional buyout')) | (df_temp['Deal type'].str.contains('ibo'))
        df_ibo = df_temp[temp_idx]
        df_temp = df_temp[~temp_idx]

        temp_idx = (df_temp['Deal type'].str.contains('management buy-out')) | (df_temp['Deal type'].str.contains('management buyout')) | (df_temp['Deal type'].str.contains('mbo'))
        df_mbo = df_temp[temp_idx]
        df_temp = df_temp[~temp_idx]

        temp_idx = (df_temp['Deal type'].str.contains('management buy-in')) | (df_temp['Deal type'].str.contains('management buyin')) | (df_temp['Deal type'].str.contains('mbi'))
        df_mbi = df_temp[temp_idx]
        df_temp = df_temp[~temp_idx]

        df_jv = df_temp[df_temp['Deal type'].str.contains('joint venture')]
        df_temp = df_temp[~df_temp['Deal type'].str.contains('joint venture')]

        temp_idx = (df_temp['Deal type'].str.contains('share buyback')) | (df_temp['Deal type'].str.contains('share buy-back'))
        df_sb = df_temp[temp_idx]
        df_temp = df_temp[~temp_idx]

        # assign back to the object
        self.deal_dic = {
                        'rto': deal_rt,
                        'rto_acq': deal_rt.copy(),
                        'acq': df_acq,
                        'acq_acq': df_acq.copy(),
                        'ipo': df_ipo,
                        'ms': df_ms,
                        'ci': df_ci,
                        'mg': df_mg,
                        'mg_acq': df_mg.copy(),
                        'ibo': df_ibo,
                        'mbo': df_mbo,
                        'mbi': df_mbi,
                        'jv': df_jv,
                        'sb': df_sb
                        }

        self.merge_keys = ['rto', 'rto_acq','acq', 'acq_acq', 'ipo', 'mg', 'mg_acq', 'ibo', 'mbo', 'mbi', 'jv']
        self.bvdid_dic = {}
        self.merge_dic = {}

    def removeDuplicates(self):
        # eliminate multiple entries per (bvdid, year), more details are in notes
        for key in self.merge_keys:
            if key in ['rto_acq', 'acq_acq', 'mg_acq']:
                self.deal_dic[key] = self.deal_dic[key][self.deal_dic[key]['Acquiror BvD ID number'].notna()]
                self.deal_dic[key] = self.deal_dic[key][~self.deal_dic[key]['Acquiror BvD ID number'].isin(['foreign', 'unknown', 'US', 'NZ'])]
                continue
            self.deal_dic[key] = self.deal_dic[key][self.deal_dic[key]['Target BvD ID number'].notna()]
            self.deal_dic[key] = self.deal_dic[key][~self.deal_dic[key]['Target BvD ID number'].isin(['foreign', 'unknown', 'US', 'NZ'])]
            if key == 'ipo':
                self.deal_dic[key] = self.deal_dic[key][self.deal_dic[key].index!=1941591293] 
                self.deal_dic[key] = self.deal_dic[key].loc[self.deal_dic[key].groupby(['Target BvD ID number', 'year'])['Deal Number'].idxmax()] 
            elif key == 'rto':
                self.deal_dic['rto'] = self.deal_dic['rto'].loc[self.deal_dic['rto'].groupby(['Target BvD ID number', 'year'])['Deal Number'].idxmax()]
                self.deal_dic['rto_acq'] = self.deal_dic['rto_acq'].loc[self.deal_dic['rto_acq'].groupby(['Acquiror BvD ID number', 'year'])['Deal Number'].idxmax()]
                self.bvdid_dic['rto_acq'] = set(self.deal_dic['rto_acq']['Acquiror BvD ID number'].unique())
            elif key == 'acq': 
                # process acq deals
                temp = self.deal_dic[key]
                temp_idx = temp['Final stake (%)'].str.contains('majority', na=False)
                temp.loc[temp_idx, 'Final stake (%)'] = '51'
                temp_idx = temp['Acquired stake (%)'].str.contains('majority', na=False)
                temp.loc[temp_idx, 'Acquired stake (%)'] = '51'

                temp['final_stake_2'] = temp['Deal type'].str.split('to').str[1].str.replace('%', '')
                temp['final_stake_2']=temp.final_stake_2.str.extract(r"(\d+\.\d+)")
                temp['final_stake_2'] = temp['final_stake_2'].astype(float)

                temp['Final stake (%)'] = temp['Final stake (%)'].str.split('\n').str[0]
                temp['Acquired stake (%)'] = temp['Acquired stake (%)'].str.split('\n').str[0]

                temp['Final stake (%)'].replace('Unknown %', np.nan, inplace=True)
                temp['Final stake (%)'].replace('Unknown minority', np.nan, inplace=True)
                temp['Acquired stake (%)'].replace('Unknown %', np.nan, inplace=True)
                temp['Acquired stake (%)'].replace('Unknown minority', np.nan, inplace=True)
                temp['Acquired stake (%)'].replace('Unknown remaining %', np.nan, inplace=True)     

                temp['Final stake (%)'] = temp['Final stake (%)'].astype(float)
                temp['Acquired stake (%)'] = temp['Acquired stake (%)'].astype(float)

                temp['final_stake_3'] = temp[['Acquired stake (%)', 'Final stake (%)', 'final_stake_2']].max(axis=1)
                temp['final_stake_3'] = temp['final_stake_3'].astype(float)

                temp = temp[temp.final_stake_3.notna()]
                temp['final_stake'] = temp['final_stake_3']
                temp.drop(columns=['final_stake_2', 'final_stake_3'], inplace=True)

                temp1 = temp.loc[temp.groupby(['Target BvD ID number', 'year'])['final_stake'].idxmax()]
                # temp1 = temp1[temp1.final_stake>=50]
                temp2 = temp.loc[temp.groupby(['Acquiror BvD ID number', 'year'])['final_stake'].idxmax()]
                # temp2 = temp2[temp2.final_stake>=50]
                self.deal_dic['acq'] = temp1.copy()
                self.deal_dic['acq_acq'] = temp2.copy()
                self.bvdid_dic['acq_acq'] = set(self.deal_dic['acq_acq']['Acquiror BvD ID number'].unique())
            elif key == 'mg':
                self.deal_dic['mg'] = self.deal_dic['mg'].loc[self.deal_dic['mg'].groupby(['Target BvD ID number', 'year'])['Deal Number'].idxmax()] 
                self.deal_dic['mg_acq'] = self.deal_dic['mg_acq'].loc[self.deal_dic['mg_acq'].groupby(['Acquiror BvD ID number', 'year'])['Deal Number'].idxmax()] 
                self.bvdid_dic['mg_acq'] = set(self.deal_dic['mg_acq']['Acquiror BvD ID number'].unique())
            else:
                self.deal_dic[key] = self.deal_dic[key].loc[self.deal_dic[key].groupby(['Target BvD ID number', 'year'])['Deal Number'].idxmax()] 
            self.bvdid_dic[key] = set(self.deal_dic[key]['Target BvD ID number'].unique())

    def checkUnique(self):
        for key in self.merge_keys:
            if key in ['rto_acq', 'acq_acq', 'mg_acq']:
                continue
            print(key, len(self.deal_dic[key]), len(self.deal_dic[key].groupby(['Target BvD ID number', 'year']).nth(0)))
            self.deal_dic[key]['deal_count'] = self.deal_dic[key].groupby(['Target BvD ID number', 'year'])['Deal Number'].transform('count')
            temp = self.deal_dic[key]
            temp = temp[temp.deal_count>=2]
            if len(temp) > 0:
                temp.to_csv('test_'+key+'_count.csv', index=False)

    def refineColumns(self, key):
        # process ipo deals
        if key == 'ipo':
            temp = self.deal_dic[key].copy()
            temp.sort_values(by='Completed date', inplace=True)

            # rename columns
            temp = temp[['Target BvD ID number', 'Completed date', 'year', 'Deal type', 'deal_subtype_mg', 'Deal Stock exchange']]
            temp.rename(columns={'Target BvD ID number':'bvdid',
                                    'Completed date':'zephyr_ipo_dt',
                                    'Deal type':'zephyr_ipo_deal_type',
                                    'deal_subtype_mg':'zephyr_ipo_deal_subtype',
                                    'Deal Stock exchange':'zephyr_ipo_exch'}, inplace=True)
            # flag
            temp['zephyr_ipo'] = 1
            # refine exchange
            lse_idx = temp['zephyr_ipo_exch']=='London Stock Exchange'
            na_idx = temp['zephyr_ipo_exch'].isna()
            deal_ofex_idx = temp['zephyr_ipo_deal_type'].str.contains('ofex')
            deal_aim_idx = temp['zephyr_ipo_deal_type'].str.contains('aim')
            # assign
            temp.loc[(lse_idx | na_idx) & deal_ofex_idx, 'zephyr_ipo_exch'] = 'ofex'
            temp.loc[(lse_idx | na_idx) & deal_aim_idx, 'zephyr_ipo_exch'] = 'London AIM Stock Exchange'
            temp.loc[temp.zephyr_ipo_exch.isna(), 'zephyr_ipo_exch'] = 'ofex'
        # process rto deals
        elif key == 'rto':
            temp = self.deal_dic[key].copy()
            # rename columns
            temp = temp[['Target BvD ID number', 'Acquiror BvD ID number', 'Completed date', 'year', 'Deal type', 'deal_subtype_mg', 'Deal Stock exchange']]
            temp.rename(columns={'Target BvD ID number':'bvdid',
                                    'Acquiror BvD ID number':'zephyr_rto_acquiror_bvdid',
                                    'Completed date':'zephyr_rto_dt',
                                    'Deal type':'zephyr_rto_deal_type',
                                    'deal_subtype_mg':'zephyr_rto_deal_subtype',
                                    'Deal Stock exchange':'zephyr_rto_exch'}, inplace=True)
            # flag
            temp['zephyr_rto'] = 1   
        # process rto_acq deals
        elif key == 'rto_acq':
            temp = self.deal_dic[key].copy()
            # rename columns
            temp = temp[['Target BvD ID number', 'Acquiror BvD ID number', 'Completed date', 'year', 'Deal type', 'deal_subtype_mg', 'Deal Stock exchange']]
            temp.rename(columns={'Acquiror BvD ID number':'bvdid',
                                    'Target BvD ID number':'zephyr_rto_acq_target_bvdid',
                                    'Completed date':'zephyr_rto_acq_dt',
                                    'Deal type':'zephyr_rto_acq_deal_type',
                                    'deal_subtype_mg':'zephyr_rto_acq_deal_subtype',
                                    'Deal Stock exchange':'zephyr_rto_acq_exch'}, inplace=True)
            # flag
            temp['zephyr_rto_acq'] = 1
        # process acquisition deals
        elif key == 'acq':                 
            temp = self.deal_dic[key].copy()
            # rename columns
            temp = temp[['Target BvD ID number', 'Acquiror BvD ID number', 'Completed date', 'year', 'Deal type', 'deal_subtype_mg', 'Deal Stock exchange', 'final_stake']]
            temp.rename(columns={'Target BvD ID number':'bvdid',
                                    'Acquiror BvD ID number':'zephyr_acq_acquiror_bvdid',
                                    'Completed date':'zephyr_acq_dt',
                                    'Deal type':'zephyr_acq_deal_type',
                                    'deal_subtype_mg':'zephyr_acq_deal_subtype',
                                    'Deal Stock exchange':'zephyr_acq_exch',
                                    'final_stake':'zephyr_acq_final_stake'}, inplace=True)
            # flag
            temp['zephyr_acq'] = 1
        #process acquisition deals (merged with acq)
        elif key == 'acq_acq':
            temp = self.deal_dic[key].copy()
            # rename columns
            temp = temp[['Target BvD ID number', 'Acquiror BvD ID number', 'Completed date', 'year', 'Deal type', 'deal_subtype_mg', 'Deal Stock exchange', 'final_stake']]
            temp.rename(columns={'Acquiror BvD ID number':'bvdid',
                                    'Target BvD ID number':'zephyr_acq_target_bvdid',
                                    'Completed date':'zephyr_acq_acq_dt',
                                    'Deal type':'zephyr_acq_acq_deal_type',
                                    'deal_subtype_mg':'zephyr_acq_acq_deal_subtype',
                                    'Deal Stock exchange':'zephyr_acq_acq_exch',
                                    'final_stake':'zephyr_acq_acq_final_stake'}, inplace=True)
            # flag
            temp['zephyr_acq_acq'] = 1
        # process merger deals
        elif key == 'mg':
            temp = self.deal_dic[key].copy()
            # rename columns
            temp = temp[['Target BvD ID number', 'Acquiror BvD ID number', 'Completed date', 'year', 'Deal type', 'deal_subtype_mg', 'Deal Stock exchange']]
            temp.rename(columns={'Target BvD ID number':'bvdid',
                                    'Acquiror BvD ID number':'zephyr_mg_acquiror_bvdid',
                                    'Completed date':'zephyr_mg_dt',
                                    'Deal type':'zephyr_mg_deal_type',
                                    'deal_subtype_mg':'zephyr_mg_deal_subtype',
                                    'Deal Stock exchange':'zephyr_mg_exch'}, inplace=True)
            # flag
            temp['zephyr_mg'] = 1
        # process merger deals (merged with acq)
        elif key == 'mg_acq':
            temp = self.deal_dic[key].copy()
            # rename columns
            temp = temp[['Target BvD ID number', 'Acquiror BvD ID number', 'Completed date', 'year', 'Deal type', 'deal_subtype_mg', 'Deal Stock exchange']]
            temp.rename(columns={'Acquiror BvD ID number':'bvdid',
                                    'Target BvD ID number':'zephyr_mg_target_bvdid',
                                    'Completed date':'zephyr_mg_acq_dt',
                                    'Deal type':'zephyr_mg_acq_deal_type',
                                    'deal_subtype_mg':'zephyr_mg_acq_deal_subtype',
                                    'Deal Stock exchange':'zephyr_mg_acq_exch'}, inplace=True)
            # flag
            temp['zephyr_mg_acq'] = 1
        # process mbo deals
        elif key == 'mbo':
            temp = self.deal_dic[key].copy()
            # rename columns
            temp = temp[['Target BvD ID number', 'Acquiror BvD ID number', 'Completed date', 'year', 'Deal type', 'deal_subtype_mg', 'Deal Stock exchange']]
            temp.rename(columns={'Target BvD ID number':'bvdid',
                                    'Acquiror BvD ID number':'zephyr_mbo_acquiror_bvdid',
                                    'Completed date':'zephyr_mbo_dt',
                                    'Deal type':'zephyr_mbo_deal_type',
                                    'deal_subtype_mg':'zephyr_mbo_deal_subtype',
                                    'Deal Stock exchange':'zephyr_mbo_exch'}, inplace=True)
            # flag
            temp['zephyr_mbo'] = 1
        # process mbi deals
        elif key == 'mbi':
            temp = self.deal_dic[key].copy()
            # rename columns
            temp = temp[['Target BvD ID number', 'Acquiror BvD ID number', 'Completed date', 'year', 'Deal type', 'deal_subtype_mg', 'Deal Stock exchange']]
            temp.rename(columns={'Target BvD ID number':'bvdid',
                                    'Acquiror BvD ID number':'zephyr_mbi_acquiror_bvdid',
                                    'Completed date':'zephyr_mbi_dt',
                                    'Deal type':'zephyr_mbi_deal_type',
                                    'deal_subtype_mg':'zephyr_mbi_deal_subtype',
                                    'Deal Stock exchange':'zephyr_mbi_exch'}, inplace=True)
            # flag
            temp['zephyr_mbi'] = 1
        # process ibo deals
        elif key == 'ibo':
            temp = self.deal_dic[key].copy()
            # rename columns
            temp = temp[['Target BvD ID number', 'Acquiror BvD ID number', 'Completed date', 'year', 'Deal type', 'deal_subtype_mg', 'Deal Stock exchange']]
            temp.rename(columns={'Target BvD ID number':'bvdid',
                                    'Acquiror BvD ID number':'zephyr_ibo_acquiror_bvdid',
                                    'Completed date':'zephyr_ibo_dt',
                                    'Deal type':'zephyr_ibo_deal_type',
                                    'deal_subtype_mg':'zephyr_ibo_deal_subtype',
                                    'Deal Stock exchange':'zephyr_ibo_exch'}, inplace=True)
            # flag
            temp['zephyr_ibo'] = 1
        # process jv deals
        elif key == 'jv':
            temp = self.deal_dic[key].copy()
            # rename columns
            temp = temp[['Target BvD ID number', 'Acquiror BvD ID number', 'Completed date', 'year', 'Deal type', 'deal_subtype_mg', 'Deal Stock exchange']]
            temp.rename(columns={'Target BvD ID number':'bvdid',
                                    'Acquiror BvD ID number':'zephyr_jv_acquiror_bvdid',
                                    'Completed date':'zephyr_jv_dt',
                                    'Deal type':'zephyr_jv_deal_type',
                                    'deal_subtype_mg':'zephyr_jv_deal_subtype',
                                    'Deal Stock exchange':'zephyr_jv_exch'}, inplace=True)
            # flag
            temp['zephyr_jv'] = 1
        return temp

    def readHandCheckData(self):
        self.hand_check = pd.read_csv(additional_fd + 'hand_check_age1_filled.csv')
        self.hand_check = self.hand_check[['bvdid', 'ipo_date_year', 'searched_dateinc', 'reason_dateinc', 'reason2_dateinc', 'reanson3_dateinc', 'PE or VC', 'zephyr_exch']]
        self.hand_check_acq = self.hand_check.copy()
        self.hand_check.rename(columns={'ipo_date_year':'year',
                                        'searched_dateinc':'hc_dateinc',
                                        'reason_dateinc':'hc_reason1',
                                        'reason2_dateinc':'hc_reason2',
                                        'reanson3_dateinc':'hc_reason3',
                                        'PE or VC':'hc_pe_vc',
                                        'zephyr_exch':'hc_exch',
                                        }, inplace=True)

        # drop zephyr_exch column in hand_check_acq
        self.hand_check_acq.drop(columns=['zephyr_exch', 'searched_dateinc'], inplace=True)
        # extract rows in hand_check_acq that the column reason3_dateinc contains 'GB'
        self.hand_check_acq = self.hand_check_acq[self.hand_check_acq['reanson3_dateinc'].str.contains('GB', na=False)]
        self.hand_check_acq.rename(columns={'bvdid':'hc_acq_bvdid',
                                            'ipo_date_year':'year',
                                            'reason_dateinc':'hc_acq_reason1',
                                            'reason2_dateinc':'hc_acq_reason2',
                                            'reanson3_dateinc':'bvdid',
                                            'PE or VC':'hc_acq_pe_vc',
                                            }, inplace=True)

    def mergePanel(self, df_panel):
        for key in self.merge_keys:
            self.merge_dic[key] = self.refineColumns(key)
            df_panel = df_panel.merge(self.merge_dic[key], how='left', left_on=['bvdid', 'year'], right_on=['bvdid', 'year'], validate='m:1')
        # merge hand_check
        df_panel = df_panel.merge(self.hand_check, how='left', left_on=['bvdid', 'year'], right_on=['bvdid', 'year'], validate='m:1')
        # merge hand_check_acq
        df_panel = df_panel.merge(self.hand_check_acq, how='left', left_on=['bvdid', 'year'], right_on=['bvdid', 'year'], validate='m:1')
        return df_panel

    def extractSIC(self):
        zp_tag_sic = self.df_deal[self.df_deal['Target BvD ID number'].notna() & self.df_deal['Target primary US SIC code'].notna()][['Target BvD ID number', 'Target primary US SIC code']]
        zp_tag_sic.rename(columns={'Target BvD ID number':'bvdid', 'Target primary US SIC code':'ussicpcod'}, inplace=True)
        zp_acq_sic = self.df_deal[zp.df_deal['Acquiror BvD ID number'].notna() & self.df_deal['Acquiror primary US SIC code'].notna()][['Acquiror BvD ID number', 'Acquiror primary US SIC code']]
        zp_acq_sic.rename(columns={'Acquiror BvD ID number':'bvdid', 'Acquiror primary US SIC code':'ussicpcod'}, inplace=True)
        zp_tag_sic.ussicpcod = zp_tag_sic.ussicpcod.str[:4]
        zp_acq_sic.ussicpcod = zp_acq_sic.ussicpcod.str[:4]
        tag_set = set(zp_tag_sic.bvdid.unique())
        zp_sic = pd.concat([zp_tag_sic, zp_acq_sic[~zp_acq_sic.bvdid.isin(tag_set)]], axis=0)
        zp_sic = zp_sic[zp_sic.bvdid.str.contains('GB')]
        self.zp_sic = zp_sic

if __name__ == "__main__":
    zp = Zephyr()
    zp.extractDeals()
    zp.removeDuplicates()
    zp.checkUnique()    
    zp.extractSIC()