import pandas as pd
import os
import numpy as np
import dask.dataframe as dd
import re
from collections import defaultdict
import bvdColumns
import processZephyr

ctry = 'GB'

fd_base = '' #Replace with your dir
bvd_fd = fd_base + 'bvd/'
sub_fd = fd_base + 'bvd_sub/'
header_fd = fd_base + 'bvd_header/'
bvd_proc_fd = fd_base + 'bvd_qualified/'
ind_fd = fd_base + 'bvd_full_header/'
output_fd = 'output/'

hist_fd = fd_base + 'bvd_hist_2020_06/'
fin_hist_fd = hist_fd +  'Financials/'
des_hist_fd = hist_fd + 'Descriptive/'
own_hist_fd = hist_fd + 'Ownership/'
add_fd = 'additional_data/'

def readWRDSHeader(ctry=ctry):
    '''Read wrds header'''
    header_cols = ['bvdid', 'historic_status_str', 'historic_statusdate', 'historic_statusdate_year',
               'dateinc', 'dateinc_year', 'listed', 'delisted_date', 'delisted_date_year', 'delisted_comment',
               'mainexch', 'ipo_date', 'ipo_date_year', 'sd_ticker', 'sd_isin', 'akaname', 'name_native', '_40025']
    df_header = pd.read_csv(header_fd + ctry + '_.csv', usecols=header_cols)

    # handle outliers
    df_header.loc[df_header.bvdid.isin(['GB03573049', 'GB03690065', 'GB07476617', 'GBIM002391V', 'GBJE104742']), 'special_delisted'] = 1
    df_header.loc[df_header.bvdid=='GB03847921', 'ipo_date']='1962-01-01'
    df_header.loc[df_header.bvdid=='GB03847921', 'ipo_date_year']= 1962

    df_header.loc[df_header.bvdid=='GB04191106', 'ipo_date']='1900-01-01'
    df_header.loc[df_header.bvdid=='GB04191106', 'ipo_date_year']= 1900

    df_header.loc[df_header.bvdid=='GB04250459', 'ipo_date']='2007-01-01'
    df_header.loc[df_header.bvdid=='GB04250459', 'ipo_date_year']= 2007

    df_header.loc[df_header.bvdid=='GB04498002', 'ipo_date']='2000-03-01'
    df_header.loc[df_header.bvdid=='GB04498002', 'ipo_date_year']= 2000

    df_header.loc[df_header.bvdid=='GB05209284', 'ipo_date']='2007-12-07'
    df_header.loc[df_header.bvdid=='GB05209284', 'ipo_date_year']= 2007

    df_header.loc[df_header.bvdid=='GB05375141', 'ipo_date']='2005-00-01'
    df_header.loc[df_header.bvdid=='GB05375141', 'ipo_date_year']= 2005

    df_header.loc[df_header.bvdid=='GB07826629', 'ipo_date']='2013-03-01'
    df_header.loc[df_header.bvdid=='GB07826629', 'ipo_date_year']= 2013

    df_header.loc[df_header.bvdid=='GBGG50463', 'ipo_date']='2009-09-01'
    df_header.loc[df_header.bvdid=='GBGG50463', 'ipo_date_year']= 2009

    df_header.loc[df_header.bvdid=='GBJE103911', 'ipo_date']='2009-10-23'
    df_header.loc[df_header.bvdid=='GBJE103911', 'ipo_date_year']= 2009

    df_header.loc[df_header.bvdid=='GBJE109471', 'ipo_date']='2012-04-01'
    df_header.loc[df_header.bvdid=='GBJE109471', 'ipo_date_year']= 2012
    return df_header


def readHistFin(ctry=ctry):
    '''Read historical financials'''
    df = dd.read_csv(fin_hist_fd+"Industry-Global_financials_and_ratios-USD.txt", sep='\t', lineterminator='\n', encoding="ISO-8859-1",
                    dtype={'Accounting practice': 'str',
                        'Audit status': 'str',
                        'Source (for publicly quoted companies)': 'str',
                        'Operating revenue original range value': 'str',
                        'Costs of employees': 'str',
                        'Information date': 'str',
                        'Added value': 'str'})

    df = df[df['ï»¿BvD ID number'].str[:2]==ctry]
    df = df.compute()
    df.to_csv(fin_hist_fd + ctry + '/' + ctry + '_fin_hist.csv', index=False)


def readHistDesc(ctry=ctry):
    '''Read historical business descriptions'''
    # read trade description
    df = pd.read_csv(des_hist_fd+"Trade_description.txt", sep='\t', lineterminator='\n', encoding="ISO-8859-1", dtype=str, on_bad_lines='skip')
    df = df[df['ï»¿BvD ID number'].str[:2]==ctry]
    df.to_csv(des_hist_fd + ctry + '/' + ctry + '_trade_desc.csv', index=False)

    # read overview
    df = pd.read_csv(des_hist_fd+"Overviews.txt", sep='\t', lineterminator='\n', encoding="ISO-8859-1", dtype=str, on_bad_lines='skip')
    df = df[df['ï»¿BvD ID number'].str[:2]==ctry]
    df.to_csv(des_hist_fd + ctry + '/' + ctry + '_overviews.csv', index=False)

    # read bankers-current
    df = pd.read_csv(des_hist_fd+"Bankers-current.txt", sep='\t', lineterminator='\n', encoding="ISO-8859-1", dtype=str, on_bad_lines='skip')
    df = df[df['ï»¿BvD ID number'].str[:2]==ctry]
    df.to_csv(des_hist_fd + ctry + '/' + ctry + '_bankers.csv', index=False)

    # read other advisors-current
    df = pd.read_csv(des_hist_fd+"Other_advisors-current.txt", sep='\t', lineterminator='\n', encoding="ISO-8859-1", dtype=str, on_bad_lines='skip')
    df = df[df['ï»¿BvD ID number'].str[:2]==ctry]
    df.to_csv(des_hist_fd + ctry + '/' + ctry + '_advisors.csv', index=False)


def getHistDesc(ctry=ctry):
    column_match = {'ï»¿BvD ID number': 'bvdid'}
    # trade description
    df_trdes = pd.read_csv(des_hist_fd + ctry + '/' + ctry + '_trade_desc.csv')
    df_trdes.rename(columns=column_match, inplace=True)
    df_trdes = df_trdes[['bvdid', 'Description and history']]
    df_trdes = df_trdes[df_trdes['Description and history'].notna()]

    # overview
    df_ov = pd.read_csv(des_hist_fd + ctry + '/' + ctry + '_overviews.csv')
    df_ov.rename(columns=column_match, inplace=True)
    df_ov = df_ov[['bvdid', 'Main domestic country', 'Main foreign countries or regions', 'Main production sites', 'Main distribution sites']]

    # bankers-current
    df_bk = pd.read_csv(des_hist_fd + ctry + '/' + ctry + '_bankers.csv')
    df_bk.rename(columns=column_match, inplace=True)
    # We checked that 'BNK Original advisor function (in English)' only contains 'banker' so we remove it
    df_bk = df_bk[['bvdid', 'BNK Full name']]

    # other advisors-current
    df_adv = pd.read_csv(des_hist_fd + ctry + '/' + ctry + '_advisors.csv')
    df_adv.rename(columns=column_match, inplace=True)
    df_adv = df_adv[['bvdid', 'ADV Full name', 'ADV Original advisor function (in English)']]
    
    return df_trdes, df_ov, df_bk, df_adv


def readHistHeader(ctry=ctry):
    '''Read historical header'''
    # read hist header
    bvd_hist_set = []
    with open(des_hist_fd + "Legal_info.txt", 'r') as f:
        for line in f:
            if line[:2] == ctry:
                bvd_hist_set.append(line.split('\t')[0])
    bvd_hist_set = set(bvd_hist_set)

    # read wrds header
    df_header = readWRDSHeader(ctry)
    wrds_set = set(df_header.bvdid.unique())

    # get the remaining set from the hist data
    diff_set = bvd_hist_set - wrds_set

    # get entries that are not already in header
    sup_header = []
    with open(des_hist_fd + "Legal_info.txt", 'r') as f:
        for line in f:
            if line[:2] == 'GB':
                temp = line.split('\t')
                if temp[0] in diff_set:
                    sup_header.append(temp)

    df_temp = pd.read_csv(des_hist_fd + "Legal_info.txt", sep='\t', lineterminator='\n', encoding="ISO-8859-1", dtype=str, nrows=2)
    df_sup_header = pd.DataFrame(index=list(range(len(list(diff_set)))), columns=df_temp.columns)
    for i in range(len(sup_header)):
        df_sup_header.loc[i] = sup_header[i]
    df_sup_header.to_csv(des_hist_fd + ctry + '/' + ctry + '_sup_header.csv', index=False)


def readHistInd(ctry=ctry):
    '''Extract the industry classification of a country from the historical data'''
    df = dd.read_csv(des_hist_fd + "Industry_classifications.txt", sep='\t', lineterminator='\n', dtype=str)
    df = df[df['BvD ID number'].str[:2]=='GB']
    df = df.compute()
    df.to_csv(des_hist_fd + ctry + '/' + ctry + '_hist_ind.csv', index=False)


def getMergedFinSet(ctry=ctry):
    '''Get the bvdid set for firms meeting filter1'''
    bvd_stats = pd.DataFrame(index=list(range(1999, 2021)), columns=['hist_total','hist_lf', 'hist_filter1', 'wrds', 'wrds_lf', 'wrds_filter1'])
    column_match = bvdColumns.getBVDColDict()
    df = pd.read_csv(fin_hist_fd + ctry + '/' + ctry + '_fin_hist.csv')
    df['closdate_year'] = df['Closing date'].astype(str).str[:4].astype(float)
    df.rename(columns=column_match, inplace=True)

    # record statistics
    for yr in range(1999, 2021):
        print('filling statistics', yr)    
        bvd_stats.loc[yr, 'hist_total'] = len(df[df.closdate_year==yr].bvdid.unique())
        bvd_stats.loc[yr, 'hist_lf'] = len(df[(df.closdate_year==yr) & (df['conscode']!='LF')].bvdid.unique())

    df = df[df['conscode']!='LF']
    
    # df['closdate'] = pd.to_datetime(df.closdate, format='%Y%m%d')
    df.reset_index(drop=True, inplace=True)
    filter_1_set = set(df.loc[((df['empl']>=10) | (df['toas']>=1000000) | (df['opre']>=250000)) & (df.closdate_year>=1999)]['bvdid'].unique())

    # record statistics
    for yr in range(1999, 2021):
        bvd_stats.loc[yr, 'hist_filter1'] = len(df[(df.closdate_year==yr) & (df.bvdid.isin(filter_1_set))].bvdid.unique())

    ## get the list of qualified companies in the wrds data
    info_cols = ['bvdid', 'category_of_company', 'conscode', 'ctryiso', 'closdate', 'closdate_year', 'orig_units', 'orig_currency', 'exchrate', 'accpractice']
    fin_cols = ['capi', 'toas', 'roa', 'cuas', 'stok', 'culi', 'debt', 'toas', 'ftdi', 'ic', 'turn', 'fias', 'onet', 'cash', 'cf', 'opre', 'fire', 'gros', 'empl']
    temp_set = set([])
    for yr in range(2008, 2021):
        print('reading wrds file', yr)
        # read financial report data
        df_temp = pd.read_csv(bvd_fd + str(yr) + '/' + ctry + '_' + str(yr) + '.csv', usecols=info_cols+fin_cols+['source', 'filing_type', 'accpractice', 'nr_months'])
        df_temp = df_temp[df_temp.conscode!='LF']
        df_temp = df_temp[(df_temp['empl']>=10) | (df_temp['toas']>=1000000) | (df_temp['opre']>=250000)]
        temp_set = temp_set | set(df_temp.bvdid.unique())

    for yr in range(2008, 2021):
        print('filling statistics', yr)
        df_temp = pd.read_csv(bvd_fd + str(yr) + '/' + ctry + '_' + str(yr) + '.csv', usecols=info_cols+fin_cols+['source', 'filing_type', 'accpractice', 'nr_months'])
        bvd_stats.loc[yr, 'wrds'] = len(df_temp[df_temp.closdate_year==yr].bvdid.unique())
        bvd_stats.loc[yr, 'wrds_lf'] = len(df_temp[(df_temp.closdate_year==yr) & (df_temp['conscode']!='LF')].bvdid.unique())
        df_temp = df_temp[df_temp.conscode!='LF']
        bvd_stats.loc[yr, 'wrds_filter1'] = len(df_temp[(df_temp.closdate_year==yr) & (df_temp.bvdid.isin(temp_set))].bvdid.unique())

    # get the union set
    filter_1_merge_set = filter_1_set | temp_set
    df = df[df.bvdid.isin(filter_1_merge_set)]
    df.to_csv(fin_hist_fd + ctry + '/' + ctry + '_fin_hist_filter1_rename.csv', index=False)
    filter_1_merge_set_df = pd.DataFrame(filter_1_merge_set, columns=["bvdid"])
    filter_1_merge_set_df.to_csv(fin_hist_fd + ctry + '/' + ctry + 'filter_1_merge_set.csv', index=False)

    bvd_stats.to_csv(output_fd + ctry + '_merge_wrds_hist_filter1.csv')


def combineFin(ctry=ctry):
    # read stats
    bvd_stats = pd.read_csv(output_fd + ctry + '_merge_wrds_hist_filter1.csv', index_col=0)

    # read the union filter 1 set
    filter_1_merge_set = pd.read_csv(fin_hist_fd + ctry + '/' + ctry + 'filter_1_merge_set.csv')
    filter_1_merge_set = set(filter_1_merge_set.bvdid)

    df = pd.read_csv(fin_hist_fd + ctry + '/' + ctry + '_fin_hist_filter1_rename.csv')
    df['closdate'] = pd.to_datetime(df.closdate, format='%Y%m%d')

    # now we have the filter list, we can merge with the wrds data
    # get the list of qualified companies in the wrds data
    df_wrds = []
    for yr in range(2008, 2021):
        print(yr)
        
        # read financial report data
        df_temp = pd.read_csv(bvd_fd + str(yr) + '/' + ctry + '_' + str(yr) + '.csv', usecols=list(df.columns))
        df_temp = df_temp[df_temp.conscode!='LF']
        df_temp = df_temp[df_temp.bvdid.isin(filter_1_merge_set)]
        df_wrds.append(df_temp)
        
    df_wrds = pd.concat(df_wrds, axis=0)
    df_wrds.reset_index(drop=True, inplace=True)
    df_wrds['closdate'] = pd.to_datetime(df_wrds.closdate)

    # label
    df['wrds'] = 0
    df_wrds['wrds'] = 1

    # remove redundancy
    df = df.groupby(['bvdid', 'conscode', 'filing_type', 'closdate']).nth(-1)
    df_wrds = df_wrds.groupby(['bvdid', 'conscode', 'filing_type', 'closdate']).nth(-1)

    # reset index
    df.reset_index(inplace=True)
    df_wrds.reset_index(inplace=True)

    # merge
    df = pd.concat([df_wrds, df], axis=0)
    df['closdate'] = pd.to_datetime(df.closdate)

    # remove duplicates
    df['dup_count'] = df.groupby(['bvdid', 'conscode', 'filing_type', 'closdate'])['bvdid'].transform('count')
    df = df[(df.dup_count<=1) | ((df.dup_count>1) & (df.wrds==1))]
    df.to_csv(fin_hist_fd + ctry + '/' + ctry + '_filter1_merge.csv', index=False)

    # save stats
    for yr in range(1999, 2021):
        bvd_stats.loc[yr, 'total_filter1'] = len(df[(df.closdate_year==yr)].bvdid.unique())
    bvd_stats.to_csv(output_fd + ctry + '_merge_wrds_hist_filter1.csv')


def getMergedHeader(ctry=ctry):
    # get merged header data
    df_sup_header = pd.read_csv(des_hist_fd + ctry + '/' + ctry + '_sup_header.csv', dtype=str)

    # rename
    header_col_match = {
        'ï»¿BvD ID number': 'bvdid', 
        'Also known as name': 'akaname', 
        'Status': 'historic_status_str',
        'Status date': 'historic_statusdate',
        'Date of incorporation': 'dateinc', 
        'Type of entity': '_40025', 
        # 'Category of the company', 
        'Listed/Delisted/Unlisted': 'listed', 
        'Delisted date': 'delisted_date',
        'Delisted comment': 'delisted_comment', 
        'Main exchange': 'mainexch', 
        'IPO date': 'ipo_date',
    }
    df_sup_header.rename(columns=header_col_match, inplace=True)

    # to datetime
    for col in ['historic_statusdate', 'dateinc', 'delisted_date', 'ipo_date']:
        col_year = col + '_year'
        df_sup_header[col_year] = np.nan
        temp_idx = (df_sup_header[col].notna()) & (df_sup_header[col]!='')
        df_sup_header.loc[temp_idx, col_year] = df_sup_header.loc[temp_idx, col].str[:4].astype(float)
        
        idx_4 = df_sup_header[col].str.len()==4
        idx_6 = df_sup_header[col].str.len()==6
        idx_old = df_sup_header[col_year] < 1678
        
        df_sup_header.loc[idx_4, col] = df_sup_header.loc[idx_4, col] + '0101'
        df_sup_header.loc[idx_6, col] = df_sup_header.loc[idx_6, col] + '01'
        df_sup_header.loc[idx_old, col] = '16780101'
        
        df_sup_header[col] = pd.to_datetime(df_sup_header[col], format='%Y%m%d')

    # read wrds header
    df_header = readWRDSHeader(ctry)

    hist_header_cols = ['bvdid', 'akaname', 'historic_status_str', 'historic_statusdate', 'historic_statusdate_year', 
              'dateinc', 'dateinc_year', '_40025', 'listed', 'delisted_date', 'delisted_date_year',
              'delisted_comment', 'mainexch', 'ipo_date', 'ipo_date_year']

    df_header = pd.concat([df_header, df_sup_header[hist_header_cols]], axis=0)

    return df_header


def readHistLinks(ctry=ctry):
    dtype_dict = {'Direct %': 'str',
                    'Total %': 'str',
                    'GUO 25': 'str',
                    'GUO 25 JO': 'str',
                    'GUO 25c': 'str',
                    'GUO 50': 'str',
                    'GUO 50c': 'str',
                    'Information date': 'str'}

    # read yearly active links
    for yr in range(2007, 2020):
        links = dd.read_csv(own_hist_fd + "Links_" + str(yr) + ".txt", sep='\t', lineterminator='\n', encoding="ISO-8859-1", dtype=dtype_dict)

        links = links[(links['Subsidiary BvD ID'].str[:2]==ctry) & (links['Active/archived']=='active')]
        links = links.compute()

        links.to_csv(own_hist_fd + ctry + '/' + ctry + '_links_' +  str(yr) + '.csv', index=False)

    # read both active and archived links from the last year
    yr += 1
    links = dd.read_csv(own_hist_fd + "Links_" + str(yr) + ".txt", sep='\t', lineterminator='\n', encoding="ISO-8859-1", dtype=dtype_dict)
    links = links[(links['Subsidiary BvD ID'].str[:2]==ctry)]
    links = links.compute()
    links[links['Active/archived']=='active'].to_csv(own_hist_fd + ctry + '/' + ctry + '_links_' +  str(yr) + '.csv', index=False)
    links[links['Active/archived']!='active'].to_csv(own_hist_fd + ctry + '/' + ctry + '_links_archived.csv', index=False)


def readHistEntities(ctry=ctry):
    '''read entities from the historical data'''
    ent = pd.read_csv(own_hist_fd + "Entities.txt", sep='\t', lineterminator='\n', encoding="ISO-8859-1", dtype=str)
    ent = ent[ent['BvD ID of the subsidiary or shareholder'].str[:2]==ctry]
    ent.to_csv(own_hist_fd + ctry + '/entities_' + ctry + '.csv', index=False)


def readHistEFPVY():
    '''extract EFPVY entities from the historical data'''
    ent = pd.read_csv(own_hist_fd + "Entities.txt", sep='\t', lineterminator='\n', encoding="ISO-8859-1", dtype=str)

    # extract I entities from the historical data
    ent[ent['Entity type\r']=='I\r'].to_csv(own_hist_fd + '/entities_i.csv', index=False)

    # extract EFPVY entities from the historical data
    ent = ent[ent['Entity type\r'].isin(['E\r', 'F\r', 'P\r', 'V\r', 'Y\r'])]
    ent.to_csv(own_hist_fd + '/entities_efpvy.csv', index=False)

    # extract EFY entities from the historical data for adding the P entities
    ent = ent[ent['Entity type\r'].isin(['E\r', 'F\r', 'Y\r'])]
    ent.to_csv(own_hist_fd + '/entities_efy.csv', index=False)


def modHistEPVY():
    ent_epvy = pd.read_csv(own_hist_fd + 'entities_efpvy.csv')
    ent_matched = pd.read_csv(output_fd + 'ent_matched_final.csv')
    pv_set = set(ent_matched['BvD ID of the subsidiary or shareholder'].unique())
    mod_idx = ent_epvy['BvD ID of the subsidiary or shareholder'].isin(pv_set)
    ent_epvy.loc[mod_idx, 'Entity type'] = 'P'
    ent_epvy.to_csv(own_hist_fd + '/entities_epvy_mod.csv', index=False)
    

def getEntitiesSets(ctry=ctry):
    '''get the set of entities in the historical data'''
    ent = pd.read_csv(own_hist_fd + ctry + '/' + ctry + '_entities_sic.csv')
    ind_set = set(ent[ent['ent_final'].isin(['C'])]['bvdid'].unique())

    en_inv = pd.read_csv(own_hist_fd + '/entities_epvy_matched.csv') #pd.read_csv(own_hist_fd + '/entities_epvy_mod.csv')
    inv_set = set(en_inv['BvD ID of the subsidiary or shareholder'].unique())
    return ind_set, inv_set, ent


def readNACE(ctry=ctry):
    # get industry
    ind_cols = ['bvdid', 'naceccod2']
    ind_l = pd.read_csv(ind_fd + ctry + '_l.csv', usecols=ind_cols, dtype={'naceccod2': str})
    ind_m = pd.read_csv(ind_fd + ctry + '_m.csv', usecols=ind_cols, dtype={'naceccod2': str})
    ind_s = pd.read_csv(ind_fd + ctry + '_s.csv', usecols=ind_cols, dtype={'naceccod2': str})
    df_ind = pd.concat([ind_l, ind_m, ind_s], axis=0)

    # df = dd.read_csv(des_hist_fd + "Industry_classifications.txt", sep='\t', lineterminator='\n', dtype=str)
    # df = df[df['BvD ID number'].str[:2]=='GB']
    # df = df.compute()
    df = pd.read_csv(des_hist_fd + ctry + '/' + ctry + '_hist_ind.csv')

    hist_set = set(df['BvD ID number'].unique())
    wrds_set = set(df_ind.bvdid.unique())

    df = df[df['BvD ID number'].isin(hist_set - wrds_set)]
    df = df[df['National industry classification used by the IP'].notna() | df['NACE Rev. 2, Core code (4 digits)'].notna() | df['NAICS, Core code (4 digits)'].notna()]
    df.to_csv(des_hist_fd + ctry + '/' + ctry + '_sup_industry.csv', index=False)


def readIndustries(ctry=ctry, ind_type='uksic'):
    '''read industry codes from the historical and the WRDS data'''
    # get industry
    if ind_type=='sic':
        wrds_type_name = 'ussicpcod'
        hist_type_name = 'US SIC, Primary code(s)'
    elif ind_type=='naics':
        wrds_type_name = 'naicspcod2017'
        hist_type_name = 'NAICS, Primary code(s)'
    elif ind_type=='uksic':
        wrds_type_name = 'natpcod'
        hist_type_name = 'Primary code(s) in this classification'

    ind_cols = ['bvdid', wrds_type_name]
    if ind_type=='uksic':
        ind_cols.append('natclass')
    
    # wrds data
    ind_l = pd.read_csv(ind_fd + ctry + '_l.csv', usecols=ind_cols, dtype={wrds_type_name: str})
    ind_m = pd.read_csv(ind_fd + ctry + '_m.csv', usecols=ind_cols, dtype={wrds_type_name: str})
    ind_s = pd.read_csv(ind_fd + ctry + '_s.csv', usecols=ind_cols, dtype={wrds_type_name: str})
    df_ind = pd.concat([ind_l, ind_m, ind_s], axis=0)

    if ind_type=='uksic':
        df_ind = df_ind[df_ind['natclass']=='UK SIC (2007)']
        df_ind = df_ind[df_ind[wrds_type_name].notna()]

    # historical data
    df = pd.read_csv(des_hist_fd + ctry + '/' + ctry + '_hist_ind.csv')

    if ind_type=='uksic':
        # fill missing uksic using nace
        # fill missing primary uksic using secondary uksic
        uksic_idx1 = df['National industry classification used by the IP']=='UK SIC (2007)'
        temp_idx = uksic_idx1 & (df['Primary code(s) in this classification'].isna()) & (df['Secondary code(s) in this classification'].notna())
        df.loc[temp_idx, 'Primary code(s) in this classification'] = df.loc[temp_idx, 'Secondary code(s) in this classification']
        # get all uksic
        uksic_all = list(df.loc[uksic_idx1, 'Primary code(s) in this classification'].dropna().unique())
        # get processed uksic list (remove and replace outliers)
        uksic_all = [str(int(i)) for i in list(uksic_all)]
        for i in ['791', '772', '80']:
            uksic_all.remove(i)
        for idx,s in enumerate(uksic_all):
            if s=='711':
                uksic_all[idx] = '99999'
                continue
            if len(s) < 5:
                uksic_all[idx] = '0' + s
        uksic_all = sorted(uksic_all)

        # fill missing uksic using nace (note that nace might not be in the same row as uksic)
        nace_set = set(df[df['NACE Rev. 2, Primary code(s)'].notna()]['BvD ID number'].unique())
        uksic_set = set(df[(df['National industry classification used by the IP']=='UK SIC (2007)') & (df['Primary code(s) in this classification'].notna())]['BvD ID number'].unique())
        temp_idx = (df['BvD ID number'].isin(nace_set - uksic_set)) & (df['NACE Rev. 2, Primary code(s)'].notna())
        df.loc[temp_idx, 'Primary code(s) in this classification'] = df.loc[temp_idx, 'NACE Rev. 2, Primary code(s)']
        df.loc[temp_idx, 'National industry classification used by the IP'] = 'UK SIC (2007)'
        df.loc[temp_idx, 'Primary code(s) in this classification'] = df.loc[temp_idx, 'Primary code(s) in this classification'].astype(int).astype(str)

        # creat a mapping from nace to uksic using first 4 digits
        nace2uksic = {}
        nace_all = set(df['NACE Rev. 2, Primary code(s)'].dropna().unique())
        nace_all = [str(int(i)) for i in list(nace_all)]
        for idx,s in enumerate(nace_all):
            if s=='6500':
                continue
            if len(s) < 4:
                s = '0' + s
            for j in uksic_all:
                if j[:4] == s:
                    nace2uksic[nace_all[idx]] = j
                    break
        df.loc[temp_idx, 'Primary code(s) in this classification'] = df.loc[temp_idx, 'Primary code(s) in this classification'].replace(nace2uksic)
        df['Primary code(s) in this classification'].replace('6500', np.nan, inplace=True)

    if ind_type=='uksic':
        df = df[df['National industry classification used by the IP']=='UK SIC (2007)']

    hist_set = set(df['BvD ID number'].unique())
    wrds_set = set(df_ind.bvdid.unique())

    if ind_type=='uksic':
        df_ind = df_ind[df_ind['bvdid'].isin(wrds_set - hist_set)]
    else:
        df = df[df['BvD ID number'].isin(hist_set - wrds_set)]
    df = df[df[hist_type_name].notna()]
    df.rename(columns={'BvD ID number':'bvdid', hist_type_name: wrds_type_name}, inplace=True)
    df = df[['bvdid', wrds_type_name]]
    df = df[df[wrds_type_name].notna()]
    df = pd.concat([df_ind, df], axis=0)

    if ind_type=='uksic':
        df.rename(columns={'natpcod':'uksic'}, inplace=True)

    # get unique bvdid rows
    df = df.groupby('bvdid').nth(0).reset_index()

    df.to_csv(des_hist_fd + ctry + '/' + ctry + '_' + ind_type + '.csv', index=False)


def findEntitiesGPLP():
    '''find entities with GP or LP in the name'''
    ent = pd.read_csv(own_hist_fd + "Entities.txt", sep='\t', lineterminator='\n', encoding="ISO-8859-1", dtype=str)
    ent['Name'] = ent['Name'].str.lower()
    ent["Name"] = ent["Name"].str.replace('.', '', regex=False)
    ent["Name"] = ent["Name"].str.replace(',', '', regex=False)
    ent = ent[(ent['Name'].str.contains(' gp ', na=False)) | (ent['Name'].str.contains(' lp ', na=False))]
    ent['c_count'] = ent.Name.str.len()
    ent = ent[ent['c_count']<200]
    ent.drop(columns='c_count', inplace=True)
    ent.to_csv(own_hist_fd + '/entities_gp_lp.csv', index=False)


def remove_brackets(x):
    '''remove brackets and content within'''
    return re.sub("[\(\[].*?[\)\]]", "", x)


def getNanSICfromPanel():
    '''get the entities with SIC code missing from the panel data'''
    df = pd.read_csv(fin_hist_fd + ctry + '/GB_filter1_merge_cons_yr_panel_ind.csv')
    df = df[df.ussicpcod.isna() & df.ent_type.notna()]
    df = df.groupby('bvdid').nth(0).reset_index()[['bvdid']]
    df.to_csv('additional_data/sic_na_ent_notna_panel.csv', index=False)


def pvNameMatch():
    '''match the names of the entities with P and V types'''
    # read the entities and get all matching candidates
    ent_1 = pd.read_csv(own_hist_fd + '/entities_efpvy.csv')
    ent_2 = pd.read_csv(own_hist_fd + ctry + '/' + ctry + '_entities_sic.csv')
    ent_3 = pd.read_csv(own_hist_fd + '/entities_gp_lp.csv')

    sic_na = pd.read_csv(add_fd + 'sic_na_ent_notna_panel.csv')
    sic_na_set = set(sic_na.bvdid.unique())

    # temp_idx = ent_2.ussicpcod.isin([7389, 8742, 7299, 1521, 8811])
    temp_idx = ent_2.ent_final.isin(['E', 'F', 'P', 'V', 'Y'])
    temp_idx = temp_idx | ent_2.bvdid.isin(sic_na_set)
    ent_2 = ent_2[temp_idx]

    ent_bvdid_set = set(ent_1['BvD ID of the subsidiary or shareholder'].unique())
    ent_bvdid_set_2 = set(ent_2.bvdid.unique())
    ent_bvdid_set_3 = set(ent_3['BvD ID of the subsidiary or shareholder'].unique())

    ent_2 = ent_2[ent_2.bvdid.isin(ent_bvdid_set_2 - ent_bvdid_set)]
    ent_2.rename(columns={'bvdid':'BvD ID of the subsidiary or shareholder','ent_type':'Entity type','name':'Name'}, inplace=True)

    ent_3 = ent_3[ent_3['BvD ID of the subsidiary or shareholder'].isin(ent_bvdid_set_3 - ent_bvdid_set - ent_bvdid_set_2)]
    ent = pd.concat([ent_1, ent_2, ent_3], axis=0)

    # remove those that their names are too long (probably errors)
    ent['c_count'] = ent.Name.str.len()
    ent = ent[ent.c_count<200]

    # process the names
    ent = ent[ent.Name.notna()]
    ent['name_mod'] = ent['Name'].str.lower()
    ent["name_mod"] = ent["name_mod"].apply(remove_brackets)
    ent["name_mod"] = ent["name_mod"].str.replace('.', '', regex=False)
    ent["name_mod"] = ent["name_mod"].str.replace(',', '', regex=False)
    ent['name_mod'] = ent['name_mod'].str.normalize('NFKD').str.encode('ascii', 'ignore').str.decode('utf-8')
    ent['name_mod'] = ent['name_mod'].str.strip().replace(r'\s+',' ', regex=True)
    ent['name_mod'] = ['-'+'-'.join([item for item in x.split(' ')])+'-' for x in ent["name_mod"]]

    temp_idx = ent['Entity type'].isin(['P', 'V']) | (ent['ent_final'].isin(['P', 'V']))
    ent_npv = ent[~temp_idx]
    ent_pv = ent[temp_idx]

    # read the company names that are P or V
    preqin_pe_ori = pd.read_csv(add_fd + 'preqin_pe_funds.csv')
    preqin_pe_ori = preqin_pe_ori[preqin_pe_ori.Fund_Type!='Fund of Funds']
    burgiss_ori = pd.read_excel(add_fd + 'burgiss.xlsx', sheet_name='Sheet1')
    stepstone_1_ori = pd.read_excel(add_fd + 'SSG_Performance_Data_10182020.xlsx', sheet_name='Oct-2020')
    stepstone_1_ori = stepstone_1_ori[stepstone_1_ori.Fund_Asset_Class=='Private Equity']
    stepstone_2_ori = pd.read_excel(add_fd + 'StepstoneDealDatae.xlsx', sheet_name='Updated SOI')
    stepstone_2_ori = stepstone_2_ori[stepstone_2_ori.Fund_Asset_Class=='Private Equity']

    # get all the fund names and GP names
    skip_list = ['@ventures', '83north', 'abenex', 'accel', 'accretive', 'amberia', 'ambienta', 'capiton', 'castlelake', 'chryscapital', 
                'erhvervsinvest', 'g2vp', 'mezzvest', 'mithril', 'nicoya', 'smeremediumcap', 'urbanamerica', 'viventures', 'chinavest']

    def process_gp_cols(df, gp_col, fund_col, skip_list):
        df['gp_word_count'] = df[gp_col].str.split().str.len()
        df['gp_name'] = df[gp_col].str.lower()
        temp_idx = (df.gp_word_count==1) & (~df.gp_name.isin(skip_list))
        df.loc[temp_idx, 'gp_name'] = df.loc[temp_idx, fund_col].str.lower()
        df['last_word'] = df['gp_name'].str.split().str[-1]

        temp_idx = (df.gp_name.str.split().str.len()>2) & (df.last_word=='management')
        df.loc[temp_idx, 'gp_name'] = df.loc[temp_idx, 'gp_name'].str.replace('management', '')
        
        df = df[df.gp_name.notna()]
        df = df[['gp_name']].groupby('gp_name').nth(0).reset_index()
        return df

    preqin_pe_fd = process_gp_cols(preqin_pe_ori, 'Fund_Name', 'Fund_Name', skip_list)
    preqin_pe_gp = process_gp_cols(preqin_pe_ori, 'Firm_Name', 'Fund_Name', skip_list)
    burgiss_fd = process_gp_cols(burgiss_ori, 'Name', 'Name', skip_list)
    burgiss_gp = process_gp_cols(burgiss_ori, 'Manager.Name', 'Name', skip_list)
    stepstone_1_fd = process_gp_cols(stepstone_1_ori, 'Fund_Name', 'Fund_Name', skip_list)
    stepstone_1_gp = process_gp_cols(stepstone_1_ori, 'GP_Name', 'Fund_Name', skip_list)
    stepstone_2_fd = process_gp_cols(stepstone_2_ori, 'Fund_Name', 'Fund_Name', skip_list)
    stepstone_2_gp = process_gp_cols(stepstone_2_ori, 'GP_Name', 'Fund_Name', skip_list)    

    # process the names
    def gp_name_process(df):
        df["name_mod"] = df['gp_name'].copy()
        df["name_mod"] = df["name_mod"].apply(remove_brackets)
        df["name_mod"] = df["name_mod"].str.replace('.', '', regex=False)
        df["name_mod"] = df["name_mod"].str.replace(',', '', regex=False)
        df["name_mod"] = df["name_mod"].str.normalize('NFKD').str.encode('ascii', 'ignore').str.decode('utf-8')
        df["name_mod"] = df["name_mod"].str.strip().replace(r'\s+',' ', regex=True)
        df["name_mod"] = ['-'+'-'.join([item for item in x.split(' ')])+'-' for x in df["name_mod"]]
        df = df.groupby('name_mod').nth(0).reset_index()
        return df

    ss_pq_bg = pd.concat([stepstone_1_fd, stepstone_2_fd, preqin_pe_fd, burgiss_fd], axis=0)
    ss_pq_bg = ss_pq_bg.groupby('gp_name').nth(0).reset_index()
    ss_pq_bg = gp_name_process(ss_pq_bg)

    ss_pq_bg_2 = pd.concat([stepstone_1_gp, stepstone_1_fd, 
                            stepstone_2_gp, stepstone_2_fd, 
                            preqin_pe_gp, preqin_pe_fd, 
                            burgiss_gp, burgiss_fd], axis=0)
    ss_pq_bg_2 = ss_pq_bg_2.groupby('gp_name').nth(0).reset_index()
    ss_pq_bg_2 = gp_name_process(ss_pq_bg_2)   

    # start to match the names
    # 2. whole word fund name match
    gpn_list = ss_pq_bg.name_mod.to_list()
    out_list = ['-rio-', '-ace-', '-xxx-', '-nev-', '-eac-', '-aei-', '-sep-', '-els-', '-klp-', '-ash-', '-bsv-', '-ph3-', 
            '-star-', '-fund-','-aria-', '-edda-', '-solar-', '-retro-', '-elite-','-alpha-', '-opera-', '-lefund-', '-aurora-',
            '-dynamo-', '-studio-', '-hickory-', '-platina-', '-fintech-', '-the-fund-', '-investec-', '-infinity-', '-evergreen-',
            '-next-capital-', '-human-capital-', '-working-capital-', '-group-investments-']
    pv_2 = ent_npv[ent_npv.name_mod.isin(gpn_list)]
    pv_2['name_len'] = pv_2['name_mod'].str.len()
    pv_2 = pv_2.sort_values('name_len')
    temp_idx = (pv_2.name_mod.str[:5]=='-fund') & (pv_2.name_len<=10)
    pv_2 = pv_2[~temp_idx]
    pv_2['name_count'] = pv_2.groupby('name_mod')['BvD ID of the subsidiary or shareholder'].transform('count')
    pv_2 = pv_2[~pv_2.name_mod.isin(out_list)]
    pv_2_set = set(pv_2['BvD ID of the subsidiary or shareholder'].unique())
    ent_npv_2 = ent_npv[~ent_npv['BvD ID of the subsidiary or shareholder'].isin(pv_2_set)].copy()

    # 3. start word fund name match
    gpn_list_2 = ss_pq_bg[~ss_pq_bg.name_mod.isin(out_list)].name_mod.to_list()
    gpn_list_2 = sorted(gpn_list_2, key=len)
    gpn_dic = defaultdict(list)
    for gpn in gpn_list_2:
        ln = len(gpn)
        gpn_dic[ln].append(gpn)

    temp_idx = ent_npv_2['name_mod']==''
    for idx,ln in enumerate(gpn_dic.keys()):
        if idx%10==0: 
            print(idx)
        t_idx = ent_npv_2['name_mod'].str[:ln].isin(gpn_dic[ln])
        temp_idx = temp_idx | t_idx

    pv_3 = ent_npv_2[temp_idx].copy()
    temp_idx = pv_3['name_mod']==''
    for idx,g in enumerate(gpn_list_2):
        if idx%5000==0: 
            print(idx)
        t_idx = pv_3['name_mod'].str[:len(g)]==g
        pv_3.loc[t_idx, 'matched_name'] = g

    pv_3['name_count'] = pv_3.groupby('matched_name')['BvD ID of the subsidiary or shareholder'].transform('count')
    pv_3.sort_values('name_count', inplace=True)
    out_list_2 = ['-select-', '-wise-', '-cross-', '-crescent-', '-evolve-', '-preston-', '-fairfield-', '-columbus-', '-jigsaw-', '-reach-', '-wing-',
                '-latitude-', '-highway-', '-caspian-', '-junction-', '-spinnaker-', '-hive-', '-venice-', '-capital-partners-',
                '-arcus-', '-crescendo-', '-artesian-',
                '-strategic-partners-','-other-']
    pv_3 = pv_3[~pv_3.matched_name.isin(out_list_2)]
    pv_3_set = set(pv_3['BvD ID of the subsidiary or shareholder'].unique())

    # 4. start word gp+fund name match
    ent_npv_3 = ent_npv_2[~ent_npv_2['BvD ID of the subsidiary or shareholder'].isin(pv_3_set)].copy()
    gpn_list_3 = ss_pq_bg_2[~ss_pq_bg_2.name_mod.isin(out_list+out_list_2)].name_mod.to_list()
    out_list_3 = ['-capital-invest-']
    pv_4 = ent_npv_3[ent_npv_3.name_mod.isin(gpn_list_3)]
    pv_4['name_len'] = pv_4['name_mod'].str.len()
    pv_4 = pv_4.sort_values('name_len')
    temp_idx = (pv_4.name_mod.str[:5]=='-fund') & (pv_4.name_len<=10)
    pv_4 = pv_4[~temp_idx]
    pv_4['name_count'] = pv_4.groupby('name_mod')['BvD ID of the subsidiary or shareholder'].transform('count')
    pv_4 = pv_4[~pv_4.name_mod.isin(out_list_3)]
    pv_4_set = set(pv_4['BvD ID of the subsidiary or shareholder'].unique())    

    # 5. start word gp+fund name match
    ent_npv_4 = ent_npv_3[~ent_npv_3['BvD ID of the subsidiary or shareholder'].isin(pv_4_set)].copy()
    gpn_list_4 = ss_pq_bg_2[~ss_pq_bg_2.name_mod.isin(out_list+out_list_2+out_list_3)].name_mod.to_list()
    gpn_list_4 = sorted(gpn_list_4, key=len)
    gpn_dic = defaultdict(list)
    for gpn in gpn_list_4:
        ln = len(gpn)
        gpn_dic[ln].append(gpn)

    temp_idx = ent_npv_4['name_mod']==''
    for idx,ln in enumerate(gpn_dic.keys()):
        if idx%10==0: 
            print(idx)
        t_idx = ent_npv_4['name_mod'].str[:ln].isin(gpn_dic[ln])
        temp_idx = temp_idx | t_idx    

    pv_5 = ent_npv_4[temp_idx].copy()
    temp_idx = pv_5['name_mod']==''
    for idx,g in enumerate(gpn_list_4):
        if idx%5000==0: 
            print(idx)
        t_idx = pv_5['name_mod'].str[:len(g)]==g
        pv_5.loc[t_idx, 'matched_name'] = g

    pv_5['name_count'] = pv_5.groupby('matched_name')['BvD ID of the subsidiary or shareholder'].transform('count')
    pv_5.sort_values('name_count', inplace=True)
    out_list_4 = ['-global-finance-', '-now-', '-share-capital-', '-equity-partners-', '-pine-tree-', '-community-investment-']
    pv_5 = pv_5[~pv_5.name_mod.isin(out_list_4)]
    pv_5_set = set(pv_5['BvD ID of the subsidiary or shareholder'].unique())

    # 6. add special cases
    ent_npv_5 = ent_npv_4[~ent_npv_4['BvD ID of the subsidiary or shareholder'].isin(pv_5_set)].copy()

    def find_special_word(df_temp):
        special_word_idx = df_temp.name_mod.str.contains('-gp-') | df_temp.name_mod.str.contains('-lp-') | df_temp.name_mod.str.contains('-capital-') | df_temp.name_mod.str.contains('-investments-') | df_temp.name_mod.str.contains('-fund-') | df_temp.name_mod.str.contains('-nominees-')
        temp_idx = (df_temp.name_mod.str.contains('-vct-')) & (df_temp.name_mod.str.contains('-plc-'))
        temp_idx = temp_idx | (df_temp.name_mod.str.contains('-venture-capital-'))
        temp_idx = temp_idx | (df_temp.name_mod.str.contains('-private-equity-'))
        temp_idx = temp_idx | (df_temp.name_mod.str.contains('-cbpe-'))
        temp_idx = temp_idx | (df_temp.name_mod.str.contains('-bridgepoint-adviser'))
        temp_idx = temp_idx | (df_temp.name_mod.str.contains('-bregal-'))
        temp_idx = temp_idx | (df_temp.name_mod.str.contains('-lms-capital'))
        temp_idx = temp_idx | (df_temp.name_mod=='-albion-venture-capital-trust-plc-')
        temp_idx = temp_idx | (df_temp.name_mod.str.contains('-blue-star-capital'))
        temp_idx = temp_idx | (df_temp.name_mod.str.contains('-special-opportunities'))
        temp_idx = temp_idx | (df_temp.name_mod.str.contains('-fortress-') & special_word_idx)
        temp_idx = temp_idx | (df_temp.name_mod.str.contains('-kkr-') & special_word_idx)
        temp_idx = temp_idx | (df_temp.name_mod.str.contains('-carlyle-') & special_word_idx)
        temp_idx = temp_idx | (df_temp.name_mod.str.contains('-cvc-') & special_word_idx)
        temp_idx = temp_idx | (df_temp.name_mod.str.contains('-blackstone-') & special_word_idx)
        temp_idx = temp_idx | (df_temp.name_mod.str.contains('-neuberger-berman-') & special_word_idx)
        temp_idx = temp_idx | (df_temp.name_mod.str.contains('-silver-lake-') & special_word_idx)
        temp_idx = temp_idx | (df_temp.name_mod.str.contains('-hellman-&-friedman-') & special_word_idx)
        temp_idx = temp_idx | (df_temp.name_mod.str.contains('-apax-') & special_word_idx)
        temp_idx = temp_idx | (df_temp.name_mod.str.contains('-permira-') & special_word_idx)
        temp_idx = temp_idx | (df_temp.name_mod.str.contains('-graphite-capital-') & special_word_idx)
        temp_idx = temp_idx | (df_temp.name_mod.str.contains('-duke-street-') & special_word_idx)
        temp_idx = temp_idx | (df_temp.name_mod.str.contains('-3i-') & special_word_idx)    
        return temp_idx
    
    pv_6_idx = find_special_word(ent_npv_5)
    pv_6 = ent_npv_5[pv_6_idx].copy()
    pv_6_set = set(pv_6['BvD ID of the subsidiary or shareholder'].unique())

    # 7. add special cases on top of pv_3
    pv_7_idx = find_special_word(ent_npv_3)
    pv_7 = ent_npv_3[pv_7_idx].copy()
    pv_7_set = set(pv_7['BvD ID of the subsidiary or shareholder'].unique())

    # update the entities
    pe_ori_set = set(ent_pv['BvD ID of the subsidiary or shareholder'].unique())
    pe_mat_2_set = pe_ori_set | pv_2_set
    pe_mat_3_set = pe_mat_2_set | pv_3_set
    pe_mat_4_set = pe_mat_3_set | pv_4_set
    pe_mat_5_set = pe_mat_4_set | pv_5_set
    pe_mat_6_set = pe_mat_5_set | pv_6_set
    pe_mat_7_set = pe_mat_3_set | pv_7_set
    temp_idx = ent.ent_final.isna()
    ent.loc[temp_idx, 'ent_final'] = ent.loc[temp_idx, 'Entity type']
    ent.loc[ent['BvD ID of the subsidiary or shareholder'].isin(pe_mat_6_set-pe_ori_set), 'ent_final'] = 'P'
    ent[ent.ent_final.isin(['E','P','V','Y'])].to_csv(own_hist_fd + '/entities_epvy_matched.csv', index=False)

    # for the set up to start word fund name match + special cases
    ent[ent['BvD ID of the subsidiary or shareholder'].isin(pe_mat_7_set)].to_csv(own_hist_fd + '/entities_pv_matched_fund.csv', index=False)


def splitHistSpecialLinks(ctry=ctry):
    '''get the links of foreign global ultimate parents and non-industrial domestic ultimate parents for each year'''
    ind_set, _, entity = getEntitiesSets(ctry)
    yr_list = list(range(2007,2021)) + ['archived']
    for yr in yr_list:
        print(yr)
        links = pd.read_csv(own_hist_fd + ctry + '/' + ctry + '_links_' + str(yr) + '.csv')
        links = links[(~links['Subsidiary BvD ID'].str.contains('-')) & (~links['Shareholder BvD ID'].str.contains('-'))]
        links = links[links['Type of relation']!='HQ']
        
        links['Information date'] = links['Information date'].astype(str).str[:4]
        # handle missing information date
        if str(yr) != 'archived':
            # fill missing information date
            temp_idx = links['Information date']=='nan'
            links.loc[temp_idx, 'Information date'] = str(yr)
        else:
            links = links[links['Information date'].notna()]
            links = links[links['Information date']!='nan']
        
        fg_list = ['GBJE', 'GBFC', 'GBGG', 'GBIM']
        temp_idx = (links['GUO 50'].notna() & (links['GUO 50'].str[:2]!='GB')) | (links['GUO 50c'].notna() & (links['GUO 50c'].str[:2]!='GB'))
        temp_idx_2 = (links['GUO 50'].str[:4].isin(fg_list)) | (links['GUO 50c'].str[:4].isin(fg_list))
        temp_idx = temp_idx | temp_idx_2
        df_fg_uo = links[temp_idx].copy()
        df_fg_uo = df_fg_uo.groupby(['Subsidiary BvD ID', 'Information date']).nth(0).reset_index()
        df_fg_uo.to_csv(own_hist_fd + ctry + '/fg_uo_' + ctry + '_' + str(yr) + '.csv', index=False)
        
        temp_idx = (links['GUO 25'].notna() & (links['GUO 25'].str[:2]!='GB')) | (links['GUO 25c'].notna() & (links['GUO 25c'].str[:2]!='GB'))
        temp_idx_2 = (links['GUO 25'].str[:4].isin(fg_list)) | (links['GUO 25c'].str[:4].isin(fg_list))
        temp_idx = temp_idx | temp_idx_2
        df_fg_uo = links[temp_idx].copy()
        df_fg_uo = df_fg_uo.groupby(['Subsidiary BvD ID', 'Information date']).nth(0).reset_index()
        df_fg_uo.to_csv(own_hist_fd + ctry + '/fg_uo_25_' + ctry + '_' + str(yr) + '.csv', index=False)
        
        # label subs with non-industrial domestic parents
        links_duo50_all = links[links['Type of relation']=='DUO 50']
        links_duo50_all = links_duo50_all[~links_duo50_all['Shareholder BvD ID'].isin(ind_set)]
        links_duo50_all = links_duo50_all.groupby(['Subsidiary BvD ID', 'Information date']).nth(0).reset_index()
        links_duo50_all.to_csv(own_hist_fd + ctry + '/' + ctry + '_links_duo50_notC_'+str(yr)+'.csv', index=False)

    #TODO improve the process
    fg_50 = fg_50.groupby(['Subsidiary BvD ID', 'Information date']).nth(0).reset_index()
    fg_25 = fg_25.groupby(['Subsidiary BvD ID', 'Information date']).nth(0).reset_index()
    duo_nind = duo_nind.groupby(['Subsidiary BvD ID', 'Information date']).nth(0).reset_index()

    fg_50.to_csv(own_hist_fd + ctry + '/fg_uo_' + ctry + '_total.csv', index=False)
    fg_25.to_csv(own_hist_fd + ctry + '/fg_uo_25_' + ctry + '_total.csv', index=False)
    duo_nind.to_csv(own_hist_fd + ctry + '/' + ctry + '_links_duo50_notC_total.csv', index=False)


def splitHistLinks(ctry=ctry, dir50=True, duo50=True, inv=True):
    '''extract the links for
        1. >50% direct ownership
        2. domestic parents
        3. PE/VC/Mutual Funds-backed companies    
    '''
    en_inv = pd.read_csv(own_hist_fd + '/entities_epvy_matched.csv')
    inv_set = set(en_inv['BvD ID of the subsidiary or shareholder'].unique())

    yr_list = list(range(2007,2021)) + ['archived']
    for yr in yr_list:
        print(yr)
        links = pd.read_csv(own_hist_fd + ctry + '/' + ctry + '_links_' + str(yr) + '.csv')
        links = links[(~links['Subsidiary BvD ID'].str.contains('-')) & (~links['Shareholder BvD ID'].str.contains('-'))]
        links = links[links['Type of relation']!='HQ']

        if dir50:
            links_dir50 = links[links['Direct % (only figures)']>50]
            links_dir50.to_csv(own_hist_fd + ctry + '/' + ctry + '_links_dir50_'+str(yr)+'.csv', index=False)

        if duo50:
            links_duo50 = links[links['Type of relation']=='DUO 50']
            links_duo50.to_csv(own_hist_fd + ctry + '/' + ctry + '_links_duo50_'+str(yr)+'.csv', index=False)

        if inv:
            # # get foreign uo labels and filter out
            links['sub_ctry'] = links['Subsidiary BvD ID'].str[:2]
            links['pr_ctry'] = links['Shareholder BvD ID'].str[:2]

            # combined those with other PE/VC/Mutual Funds-backed companies
            inv_pr_idx = (links.sub_ctry=='GB') & (links['Shareholder BvD ID'].isin(inv_set))
            links_inv = links[inv_pr_idx].copy()
            # links_inv = pd.concat([links_inv, add_pe], axis=0)
            links_inv.to_csv(own_hist_fd + ctry + '/' + ctry + '_links_inv_' + str(yr) + '.csv', index=False)


def mergeHistLinks(ctry=ctry, link_type='duo50'):
    duo50_cols = ['Subsidiary BvD ID', 'Shareholder BvD ID', 'Information date']
    yr_list = list(range(2007,2021)) + ['archived']

    # get all the pairs as index
    pair_set = set([])
    for yr in yr_list:
        links_duo50 = pd.read_csv(own_hist_fd + ctry + '/' + ctry + '_links_' + link_type + '_' + str(yr)+'.csv', usecols=duo50_cols)
        # handle missing information date
        if str(yr) != 'archived':
            links_duo50['Information date'] = links_duo50['Information date'].fillna(str(yr))
        else:
            links_duo50 = links_duo50[links_duo50['Information date'].notna()]
        links_duo50 = links_duo50.groupby(['Subsidiary BvD ID', 'Shareholder BvD ID']).nth(0)
        pair_yr_set = set(links_duo50.index)
        pair_set = pair_set | pair_yr_set
        print(yr, len(pair_set))

    # setup the dataframe
    duo50_all = pd.DataFrame(index=pair_set, columns=['first', 'last'])
    duo50_all.index.set_names(['Subsidiary BvD ID', 'Shareholder BvD ID'], inplace=True)

    for yr in range(2007, 2021):
        print(yr)
        links_duo50 = pd.read_csv(own_hist_fd + ctry + '/' + ctry + '_links_' + link_type + '_' +str(yr)+'.csv', usecols=duo50_cols, dtype=str)
        links_duo50['Information date'] = links_duo50['Information date'].fillna(str(yr))
        links_duo50['Information date'] = links_duo50['Information date'].str[:4].astype(int)
        links_duo50.sort_values(by=['Information date'], inplace=True)
        links_duo50_first = links_duo50.groupby(['Subsidiary BvD ID', 'Shareholder BvD ID']).nth(0)
        
        links_duo50_first.rename(columns={'Information date': 'first_yr'}, inplace=True)
        links_duo50_first['last_yr'] = yr
        
        duo50_all = duo50_all.merge(links_duo50_first, how='left', left_index=True, right_index=True)

        duo50_all.loc[duo50_all['first'].isna(), 'first'] = duo50_all.loc[duo50_all['first'].isna(), 'first_yr']
        duo50_all.loc[duo50_all['first'].notna(), 'first'] = duo50_all.loc[duo50_all['first'].notna(), ['first', 'first_yr']].min(axis=1)
        duo50_all.loc[duo50_all['last'].isna(), 'last'] = duo50_all.loc[duo50_all['last'].isna(), 'last_yr']
        duo50_all.loc[duo50_all['last'].notna(), 'last'] = duo50_all.loc[duo50_all['last'].notna(), ['last', 'last_yr']].max(axis=1)
        duo50_all.drop(columns=['first_yr','last_yr'], inplace=True)

    # for achived
    links_duo50 = pd.read_csv(own_hist_fd + ctry + '/' + ctry + '_links_' + link_type + '_archived.csv', usecols=duo50_cols, dtype=str)
    links_duo50 = links_duo50[links_duo50['Information date'].notna()]
    links_duo50['Information date'] = links_duo50['Information date'].str[:4].astype(int)
    links_duo50.sort_values(by=['Information date'], inplace=True)
    links_duo50_arc = links_duo50

    links_duo50_first = links_duo50_arc.groupby(['Subsidiary BvD ID', 'Shareholder BvD ID']).nth(0)
    links_duo50_last = links_duo50_arc.groupby(['Subsidiary BvD ID', 'Shareholder BvD ID']).nth(-1)
    links_duo50_first.rename(columns={'Information date': 'first_yr'}, inplace=True)
    links_duo50_last.rename(columns={'Information date': 'last_yr'}, inplace=True)
    duo50_all = duo50_all.merge(links_duo50_first, how='left', left_index=True, right_index=True)
    duo50_all = duo50_all.merge(links_duo50_last, how='left', left_index=True, right_index=True)
    duo50_all.loc[duo50_all['first'].isna(), 'first'] = duo50_all.loc[duo50_all['first'].isna(), 'first_yr']
    duo50_all.loc[duo50_all['first'].notna(), 'first'] = duo50_all.loc[duo50_all['first'].notna(), ['first', 'first_yr']].min(axis=1)
    duo50_all.loc[duo50_all['last'].isna(), 'last'] = duo50_all.loc[duo50_all['last'].isna(), 'last_yr']
    duo50_all.loc[duo50_all['last'].notna(), 'last'] = duo50_all.loc[duo50_all['last'].notna(), ['last', 'last_yr']].max(axis=1)
    duo50_all.drop(columns=['first_yr','last_yr'], inplace=True)
    
    # save the intermediate file
    duo50_all.to_csv(own_hist_fd +  ctry + '/' + ctry + '_links_' + link_type + '_final.csv')


def extractHistLinks(ctry=ctry, link_type='duo50'):
    duo50_all = pd.read_csv(own_hist_fd +  ctry + '/' + ctry + '_links_' + link_type + '_final.csv')

    if link_type == 'inv':
        duo50_all[list(range(1999,2021))] = 0
        for index, row in duo50_all.iterrows():
            duo50_all.loc[index, int(row['first']):int(row['last'])] = 1
        
        duo50_all = duo50_all.groupby('Subsidiary BvD ID')[list(range(1999,2021))].sum()
        duo50_all.ffill(axis = 1, inplace=True)
        duo50_all.to_csv(own_hist_fd +  ctry + '/' + ctry + '_links_' + link_type + '_extract.csv')
        return 0
    # else:
    #     # restrict shareholders to be GB companies
    #     duo50_all = duo50_all[duo50_all['Shareholder BvD ID'].str[:2]=='GB']
    
    # replace shareholders that are individuals (with their BvD ID starting with 'WW' or the third character being *) with the subsidiary BvD ID
    ent = pd.read_csv(own_hist_fd + '/entities_i.csv')
    i_set = set(ent['BvD ID of the subsidiary or shareholder'].unique())
    temp_idx = (duo50_all['Shareholder BvD ID'].isin(i_set)) | (duo50_all['Shareholder BvD ID'].str[:2]=='WW')
    duo50_all.loc[temp_idx, 'Shareholder BvD ID'] = duo50_all.loc[temp_idx, 'Subsidiary BvD ID']

    # drop duplicate (subsidiary, shareholder, first, and last) entries
    duo50_all.drop_duplicates(subset=['Subsidiary BvD ID', 'Shareholder BvD ID', 'first', 'last'], inplace=True)

    # refine links with duplicate (subsidiary, first, and last) entries
    if link_type == 'dir50': # note that in duo50, we don't have links with duplicate (subsidiary, first, and last) entries or even (subsidiary, last) entries
        # start to remove duplicate (subsidiary, first, and last) entries
        duo50_all['count'] = duo50_all.groupby(['Subsidiary BvD ID', 'first', 'last'])['Shareholder BvD ID'].transform('count')
        # seperate duo50_all into two parts: one with duplicate (subsidiary, first, and last) entries and one without
        duo50_all_dup = duo50_all[duo50_all['count']>1]
        duo50_all = duo50_all[duo50_all['count']==1]
        # remove duplicate (subsidiary, first, and last) entries by removing shareholders that are themselves subsidiaries
        duo50_all_dup = duo50_all_dup[~(duo50_all_dup['Shareholder BvD ID']==duo50_all_dup['Subsidiary BvD ID'])]
        # seperate duo50_all_dup into two parts: one with duplicate (subsidiary, first, and last) entries and one without
        duo50_all_dup['count'] = duo50_all_dup.groupby(['Subsidiary BvD ID', 'first', 'last'])['Shareholder BvD ID'].transform('count')
        duo50_all_dup_meg = duo50_all_dup[duo50_all_dup['count']==1]
        # merge duo50_all_dup_meg with duo50_all
        duo50_all = pd.concat([duo50_all, duo50_all_dup_meg], axis=0)

        # keep removing
        duo50_all_dup = duo50_all_dup[duo50_all_dup['count']>1]
        # remove duplicate (subsidiary, first, and last) entries by removing shareholders that are foreign companies
        duo50_all_dup['sub_fst_lst'] = list(zip(duo50_all_dup['Subsidiary BvD ID'], duo50_all_dup['first'], duo50_all_dup['last']))
        dup_gb_set = set(duo50_all_dup[duo50_all_dup['Shareholder BvD ID'].str[:2]==ctry]['sub_fst_lst'].unique())
        duo50_all_dup = duo50_all_dup[~( duo50_all_dup['sub_fst_lst'].isin(dup_gb_set) & (duo50_all_dup['Shareholder BvD ID'].str[:2]!=ctry) )]
        # seperate duo50_all_dup into two parts: one with duplicate (subsidiary, first, and last) entries and one without, then merge duo50_all_dup_meg with duo50_all
        duo50_all_dup['count'] = duo50_all_dup.groupby(['Subsidiary BvD ID', 'first', 'last'])['Shareholder BvD ID'].transform('count')
        duo50_all_dup_meg = duo50_all_dup[duo50_all_dup['count']==1]
        duo50_all = pd.concat([duo50_all, duo50_all_dup_meg], axis=0)

        # keep removing
        duo50_all_dup = duo50_all_dup[duo50_all_dup['count']>1]
        # read in the entity type file
        ent = pd.read_csv(own_hist_fd + ctry + '/' + ctry + '_entities_sic.csv')
        c_set = set(ent[ent['ent_final']=='C']['bvdid'].unique())

        # remove duplicate (subsidiary, first, and last) entries by removing shareholders that do not have entity type C
        dup_c_set = set(duo50_all_dup[duo50_all_dup['Shareholder BvD ID'].isin(c_set)]['sub_fst_lst'].unique())
        duo50_all_dup = duo50_all_dup[~( duo50_all_dup['sub_fst_lst'].isin(dup_c_set) & (~duo50_all_dup['Shareholder BvD ID'].isin(c_set)) )]
        # merge duo50_all_dup_meg with duo50_all
        duo50_all_dup['count'] = duo50_all_dup.groupby(['Subsidiary BvD ID', 'first', 'last'])['Shareholder BvD ID'].transform('count')
        duo50_all = pd.concat([duo50_all, duo50_all_dup], axis=0)
        print('Number of duplicates:', (duo50_all_dup[duo50_all_dup['count']>1]['Subsidiary BvD ID'].nunique()))
        duo50_all.drop(columns='sub_fst_lst', inplace=True)

    duo50_all = duo50_all[duo50_all['Shareholder BvD ID'].notna()]

    # extend the years to columns
    duo50_all[list(range(1999,2021))] = 0
    for yr in range(1999,2021):
        temp_idx = (duo50_all['first']<=yr) & (duo50_all['last']>=yr)
        duo50_all.loc[temp_idx, yr] = 1

    sub_set = set(duo50_all['Subsidiary BvD ID'].unique())
    duo_final = pd.DataFrame(index=sub_set)
    duo_final.index.set_names(['Subsidiary BvD ID'], inplace=True)

    # loop through each year, extract the data, and merge with duo_final
    for yr in range(1999,2021):
        print(yr)
        duo_yr = duo50_all[duo50_all[yr]==1][['Subsidiary BvD ID', 'Shareholder BvD ID', 'first', 'last']].copy()
        # sort the data by dp_flag, first, last, and self_flag
        duo_yr['dp_flag'] = 0
        duo_yr['self_flag'] = 0
        temp_idx = (duo_yr['Shareholder BvD ID'].str[:2]=='GB') & (~duo_yr['Shareholder BvD ID'].str[2:4].isin(['JE','FC','GG','IM'])) & (duo_yr['Shareholder BvD ID']!=duo_yr['Subsidiary BvD ID'])
        duo_yr.loc[temp_idx, 'dp_flag'] = 1
        duo_yr.loc[(duo_yr['Shareholder BvD ID']!=duo_yr['Subsidiary BvD ID']), 'self_flag'] = 1
        duo_yr.sort_values(by=['dp_flag', 'first', 'last', 'self_flag'], ascending=[False, False, False, False], inplace=True)
        duo_yr = duo_yr.groupby('Subsidiary BvD ID').nth(0).reset_index()
        duo_yr.drop(columns=['first', 'last'], inplace=True)
        duo_yr.rename(columns={'Shareholder BvD ID':str(yr)}, inplace=True)
        duo_yr.set_index('Subsidiary BvD ID', inplace=True)
        duo_final = duo_final.merge(duo_yr[str(yr)], how='left', left_index=True, right_index=True)   

    # fill forward the data from 1999 to 2007
    duo_final.loc[:, [str(i) for i in range(1999,2008)]] = duo_final.loc[:, [str(i) for i in range(1999,2008)]].ffill(axis=1)

    # save to files
    for yr in range(1999, 2021):
        df_temp = duo_final[[str(yr)]]
        df_temp = df_temp.dropna()
        df_temp.rename(columns={str(yr):'Shareholder BvD ID'}, inplace=True)
        print(yr, len(df_temp))
        df_temp.to_csv(own_hist_fd +  ctry + '/' + ctry + '_links_' + link_type + '_extract_' + str(yr) + '.csv')


def splitBackedLinks(ctry=ctry, fd=False, ori=False, nff=False):
    df_header = pd.read_csv(des_hist_fd + ctry + '/' + ctry + '_header_zp_cpst.csv')
    duo50_all = pd.read_csv(own_hist_fd +  ctry + '/' + ctry + '_links_inv_final.csv')
    # en_inv = pd.read_csv(own_hist_fd + '/entities_epvy_mod.csv')
    if fd:
        en_inv = pd.read_csv(own_hist_fd + '/entities_pv_matched_fund.csv')
    else:
        en_inv = pd.read_csv(own_hist_fd + '/entities_epvy_matched.csv')
    
    fd_label = '_fd' if fd else ''
    ori_label = '_ori' if ori else ''
    nff_label = '_nff' if nff else ''

    ent_col = 'Entity type' if ori else 'ent_final'
    inv_set = set(en_inv[en_inv[ent_col].isin(['P','V'])]['BvD ID of the subsidiary or shareholder'].unique())
    df_temp = duo50_all[duo50_all['Shareholder BvD ID'].isin(inv_set)].copy()
    df_temp[list(range(1999,2021))] = np.nan
    df_temp = df_temp.reset_index(drop=True)
    for index, row in df_temp.iterrows():
        df_temp.loc[index, int(row['first']):int(row['last'])] = 1

    df_temp = df_temp.groupby('Subsidiary BvD ID')[list(range(1999,2021))].sum()
    df_temp.replace(0.0, np.nan, inplace=True)
    df_temp_copy = df_temp.copy()

    if nff:
        df_temp_2 = df_temp
    else:
        df_temp_2 = df_temp.ffill(axis = 1)

    df_temp_2.to_csv(own_hist_fd +  ctry + '/' + ctry + '_links_PV1_extract' + fd_label + ori_label + nff_label + '.csv')

    total_set = set(df_temp.index)
    yr_list = list(range(2007,2021)) + ['archived']
    for yr in yr_list:
        # print(yr)
        # read links
        links = pd.read_csv(own_hist_fd + ctry + '/' + ctry + '_links_' + str(yr) + '.csv')
        links = links[(~links['Subsidiary BvD ID'].str.contains('-')) & (~links['Shareholder BvD ID'].str.contains('-'))]
        links = links[links['Type of relation']!='HQ']

        links['Information date'] = links['Information date'].astype(str).str[:4]
        # handle missing information date
        if str(yr) != 'archived':
            # fill missing information date
            temp_idx = links['Information date']=='nan'
            links.loc[temp_idx, 'Information date'] = str(yr)
        else:
            links = links[links['Information date'].notna()]
            links = links[links['Information date']!='nan']
        links['Information date'] = links['Information date'].astype(int)

        # get unique information date years
        info_years = list(links['Information date'].unique())
        for info_yr in info_years:
            if info_yr < 1999:
                continue
            print(yr, info_yr)
            pr_set = set(df_temp_copy[df_temp_copy[info_yr]>0].index)
            li_set = set(df_header.loc[(df_header['ipo_date_year']<=info_yr) & ( (df_header['delisted_date_year'].isna()) | (info_yr<=df_header['delisted_date_year']) ), 'bvdid'].unique())
            pr_set = pr_set - li_set
            add_set = set(links[(links['Information date']==info_yr) & (links['Shareholder BvD ID'].isin(pr_set)) & (~links['Subsidiary BvD ID'].isin(pr_set))]['Subsidiary BvD ID'].unique())
            total_set = total_set | add_set
            df_temp = df_temp.reindex(total_set)
            df_temp.loc[add_set, info_yr] = 1

    if not nff:
        df_temp.ffill(axis = 1, inplace=True)
    df_temp.to_csv(own_hist_fd +  ctry + '/' + ctry + '_links_PV_extract' + fd_label + ori_label + nff_label + '.csv')


def getLinkPath(df_sub_50, ind_set, num_iters=20, pfx='sh_', up_str='up_bvdid', up_nind_str='up_bvdid_nind'):
    '''A helper function to get the link path'''
    for i in range(1, 1+num_iters):
        # if i%5==0:
        #     print(i)
        temp_set = set(df_sub_50[pfx+str(i)].dropna().unique())
        if len(temp_set)==0:
            break
        temp_mg = df_sub_50.loc[df_sub_50.index.isin(temp_set), pfx+'1'].rename(pfx+str(i+1))
        df_sub_50 = df_sub_50.merge(temp_mg, how='left', left_on=pfx+str(i), right_index=True)

        pre_cols = [pfx+str(j) for j in range(1,i)]
        cycle_idx = df_sub_50[pre_cols].isin(df_sub_50[pfx+str(i+1)]).any(1)
        df_sub_50.loc[cycle_idx, pfx+str(i+1)] = np.nan

    max_i = i
    if df_sub_50[pfx+str(i)].notna().sum()==0:
        df_sub_50.drop(columns=pfx+str(i), inplace=True)
        max_i-=1

    if up_str!='':
        df_sub_50[up_str] = np.nan
    if up_nind_str!='':
        df_sub_50[up_nind_str] = np.nan
        
    for i in range(max_i, 0, -1):
        # if i%5==0:
        #     print(i)
            
        if up_nind_str!='':
            temp_idx = (df_sub_50[up_nind_str].isna()) & (df_sub_50[pfx+str(i)].notna())
            df_sub_50.loc[temp_idx, up_nind_str] = df_sub_50.loc[temp_idx, pfx+str(i)]
        if up_str!='':
            temp_idx = (df_sub_50[up_str].isna()) & (df_sub_50[pfx+str(i)].notna()) & (df_sub_50[pfx+str(i)].isin(ind_set))
            df_sub_50.loc[temp_idx, up_str] = df_sub_50.loc[temp_idx, pfx+str(i)]
    
    return df_sub_50


def propagateHistLinks(ctry=ctry):
    # get industrial set and the VC/PE backed set
    ind_set, inv_set, entity = getEntitiesSets(ctry)

    # get header
    df_header = getMergedHeader(ctry)

    for yr in range(2020, 1998, -1):
        print(yr)
        df_dir_temp = pd.read_csv(own_hist_fd +  ctry + '/' + ctry + '_links_dir50_extract_' + str(yr) + '.csv', dtype=str)
        df_duo_temp = pd.read_csv(own_hist_fd +  ctry + '/' + ctry + '_links_duo50_extract_' + str(yr) + '.csv', dtype=str)

        df_dir_temp['dir'] = 1
        df_duo_temp['dir'] = 0

        df = pd.concat([df_duo_temp, df_dir_temp], axis=0).reset_index(drop=True)

        # remove branches
        df = df[(~df['Subsidiary BvD ID'].str.contains('-')) & (~df['Shareholder BvD ID'].str.contains('-'))]

        # get the latest year
        df['year'] = yr

        # get foreign uo labels and filter out
        df['pr_ctry'] = df['Shareholder BvD ID'].str[:2]

        # filters
        fg_list = ['JE', 'FC', 'GG', 'IM']
        df = df[df.pr_ctry=='GB']
        df = df[~df['Shareholder BvD ID'].str[2:4].isin(fg_list)]
        df = df[df['Subsidiary BvD ID']!=df['Shareholder BvD ID']]

        # merge headers
        df = df.merge(df_header[['bvdid', 'historic_status_str', 'historic_statusdate', 'historic_statusdate_year']], how='left', left_on='Shareholder BvD ID', right_on='bvdid')
        df['historic_statusdate'] = pd.to_datetime(df.historic_statusdate)

        # filter out inactive parent companies
        ia_idx = (df.historic_status_str.str.contains('Dissolved')) | (df.historic_status_str.str.contains('Bankruptcy')) | (df.historic_status_str.str.contains('Inactive')) | (df.historic_status_str.str.contains('liquidation'))
        ia_t_idx = df.historic_statusdate <= pd.Timestamp(year=yr, month=1, day=1, hour=12)

        df = df[~( ia_idx & ia_t_idx )]

        df.drop(columns=['pr_ctry', 'bvdid','historic_status_str', 'historic_statusdate','historic_statusdate_year'], inplace=True)
        df.rename(columns={'Shareholder BvD ID':'sh_1'}, inplace=True)

        # propagate ultimate parents
        # dir50
        df_dir_50 = df[df['dir']==1].copy()
        df_dir_50.sort_values(by=['year'], inplace=True)

        # get unique subsidiaries using the last year entries
        df_dir_50 = df_dir_50.groupby('Subsidiary BvD ID').nth(-1)
        df_dir_50 = getLinkPath(df_dir_50, ind_set, num_iters=20)

        # duo50c
        df_duo_50 = df[df['dir']==0].copy()
        df_duo_50.sort_values(by=['year'], inplace=True)

        # get unique subsidiaries using the last year entries
        df_duo_50 = df_duo_50.groupby('Subsidiary BvD ID').nth(-1)
        df_duo_50 = getLinkPath(df_duo_50, ind_set, num_iters=10)

        # merge duo50 with dir50
        df_dir_50 = df_dir_50.merge(df_duo_50[['up_bvdid', 'up_bvdid_nind']], how='outer', left_index=True, right_index=True, suffixes=['_2', ''])
        df_dir_50['up_bvdid'] = df_dir_50['up_bvdid'].fillna(df_dir_50['up_bvdid_2'])
        df_dir_50['up_bvdid_nind'] = df_dir_50['up_bvdid_nind'].fillna(df_dir_50['up_bvdid_nind_2'])

        # propagate up_bvdid and up_bvdid_nind
        df_dir_50.drop(columns=['up_bvdid_2', 'up_bvdid_nind_2'], inplace=True)
        df_dir_50.rename(columns={'up_bvdid':'up_bvdid_1', 'up_bvdid_nind':'up_bvdid_nind_1'}, inplace=True)

        df_dir_50 = getLinkPath(df_dir_50, ind_set, num_iters=5, pfx='up_bvdid_', up_str='up_bvdid', up_nind_str='')
        df_dir_50 = getLinkPath(df_dir_50, ind_set, num_iters=5, pfx='up_bvdid_nind_', up_str='up_bvdid_nind', up_nind_str='')
        df_dir_50.drop(columns=[c for c in list(df_dir_50.columns) if re.match("(up_bvdid_[0-9]+)|(up_bvdid_nind_[0-9]+)",c)], inplace=True)

        df_dir_50.to_csv(own_hist_fd +  ctry + '/comb_' + ctry + '_' + str(yr) + '.csv')


def createListedStatus(df, status_name, listed_name, ipo_yr_name, delisted_yr_name, bydate=False, idx=None):
    '''Create listed status by year (or by date)'''
    if idx is not None:
        idx = idx & (df[listed_name]=='Unlisted')
    else:
        idx = df[listed_name]=='Unlisted'
    df.loc[idx, status_name] = 0

    # if by date, use closdate
    if bydate:
        df.loc[(df[ipo_yr_name] < df.closdate) & (df[listed_name]=='Listed'), status_name] = 1
        df.loc[(df[ipo_yr_name] < df.closdate) & (df.closdate < df[delisted_yr_name]) & (df[listed_name]=='Delisted'), status_name] = 1
        return df

    df.loc[(df[ipo_yr_name] <= df.closdate_year) & (df[listed_name]=='Listed'), status_name] = 1
    df.loc[(df[ipo_yr_name] <= df.closdate_year) & (df.closdate_year < df[delisted_yr_name]) & (df[listed_name]=='Delisted'), status_name] = 1
    return df


def mergeHeader(df, df_header, ctry=ctry):
    '''Merge header data to the financial data'''
    # df_header = getMergedHeader(ctry)
    df = df.merge(df_header, how='left', left_on='bvdid', right_on='bvdid', validate='m:1')
    li_set = set(df_header[df_header.listed=='Listed'].bvdid.unique())
    de_set = set(df_header[df_header.listed=='Delisted'].bvdid.unique())

    # convert date columns to datetime
    df['ipo_date'] = df.ipo_date.str.replace('-00', '-01')
    df['ipo_date'] = df.ipo_date.str.replace('-02-29', '-02-28')
    df['ipo_date'] = pd.to_datetime(df.ipo_date)
    df['closdate'] = pd.to_datetime(df.closdate)
    df['delisted_date'] = pd.to_datetime(df.delisted_date)

    # set listed status by date
    df = createListedStatus(df, 'listed_status_bydate', 'listed', 'ipo_date', 'delisted_date', bydate=True)

    # set listed status by year
    df = createListedStatus(df, 'listed_status', 'listed', 'ipo_date_year', 'delisted_date_year')
    return df


def mergeLinks(df, df_header, ctry=ctry, start_yr=2002):
    '''Merge links data to the financial data'''
    ind_set, inv_set, entity_copy = getEntitiesSets(ctry)

    # merge special links
    fg_50 = pd.read_csv(own_hist_fd + ctry + '/fg_uo_' + ctry + '_total.csv')
    fg_25 = pd.read_csv(own_hist_fd + ctry + '/fg_uo_25_' + ctry + '_total.csv')
    # duo_nind = pd.read_csv(own_hist_fd + ctry + '/' + ctry + '_links_duo50_notC_total.csv')

    fg_50 = fg_50[['Subsidiary BvD ID', 'Information date']]
    fg_50['flag_fg_50'] = 1
    fg_50.rename(columns={'Subsidiary BvD ID':'bvdid', 'Information date':'closdate_year'},inplace=True)

    fg_25 = fg_25[['Subsidiary BvD ID', 'Information date']]
    fg_25['flag_fg_25'] = 1
    fg_25.rename(columns={'Subsidiary BvD ID':'bvdid', 'Information date':'closdate_year'},inplace=True)

    # duo_nind = duo_nind[['Subsidiary BvD ID', 'Information date']]
    # duo_nind['flag_non_ind_pr'] = 1
    # duo_nind.rename(columns={'Subsidiary BvD ID':'bvdid', 'Information date':'closdate_year'},inplace=True)

    df = df.merge(fg_50, how='left', on=['bvdid', 'closdate_year'], validate='m:1')
    df = df.merge(fg_25, how='left', on=['bvdid', 'closdate_year'], validate='m:1')
    # df = df.merge(duo_nind, how='left', on=['bvdid', 'closdate_year'], validate='m:1')

    # merge up_bvdid
    df_list = []
    for yr in range(start_yr, 2021): 
        print(yr)
        # get the combined links
        df_comb = pd.read_csv(own_hist_fd +  ctry + '/comb_' + ctry + '_' + str(yr) + '.csv')
        # df_comb.drop(columns=['year', 'sub_ctry', 'pr_ctry', 'bvdid', 'historic_status_str', 'historic_statusdate', 'historic_statusdate_year', 'dateinc_year'], inplace=True)
        # df_comb.drop(columns=['year', 'sub_ctry', 'pr_ctry', 'bvdid', 'historic_status_str', 'historic_statusdate', 'historic_statusdate_year', 'dateinc_year', 'Shareholder BvD ID'], inplace=True)
        df_comb = df_comb[['Subsidiary BvD ID', 'up_bvdid', 'up_bvdid_nind']]
        df_comb['closdate_year'] = yr
        # df_comb.rename(columns={'Subsidiary BvD ID':'bvdid', 'Shareholder BvD ID':'dir_pr_bvdid'}, inplace=True)
        df_comb.rename(columns={'Subsidiary BvD ID':'bvdid'}, inplace=True)
        df_list.append(df_comb)

    df_comb = pd.concat(df_list, axis=0)
    df_comb = df_comb[df_comb.bvdid.isin(set(df.bvdid.unique()))]
    df_comb = df_comb.groupby(['bvdid', 'closdate_year']).nth(0).reset_index()

    # get the previous year up_bvdid and up_bvdid_nind
    df_comb['prev_up_bvdid'] = df_comb.sort_values('closdate_year').groupby('bvdid')['up_bvdid'].shift(periods=1)
    df_comb['prev_up_bvdid_nind'] = df_comb.sort_values('closdate_year').groupby('bvdid')['up_bvdid_nind'].shift(periods=1)

    # merge the combined links into the financial data
    df = df.merge(df_comb, how='left', on=['bvdid', 'closdate_year'], validate='m:1')
    # get up_bvdid listed status
    df = df.merge(df_header[['bvdid', 'listed', 'ipo_date_year', 'delisted_date_year', 'dateinc_year']], how='left', 
                            left_on='up_bvdid', right_on='bvdid', suffixes=[None, '_up'], validate='m:1')
    # set up_bvdid listed status by year
    df = createListedStatus(df, 'listed_status_up', 'listed_up', 'ipo_date_year_up', 'delisted_date_year_up')
    # get age of up_bvdid
    df['age_up'] = df.closdate_year - df.dateinc_year_up

    # merge links identifying PV-backed firms
    links_pv = pd.read_csv(own_hist_fd +  ctry + '/' + ctry + '_links_PV_extract_nff.csv')
    links_pv_fd = pd.read_csv(own_hist_fd +  ctry + '/' + ctry + '_links_PV_extract_fd_nff.csv')
    links_pv_ori = pd.read_csv(own_hist_fd +  ctry + '/' + ctry + '_links_PV_extract_ori_nff.csv')

    for yr in range(1999,2021):
        pv_set = set(links_pv.loc[links_pv[str(yr)]>0, 'Subsidiary BvD ID'].unique())
        pv_fd_set = set(links_pv_fd.loc[links_pv_fd[str(yr)]>0, 'Subsidiary BvD ID'].unique())
        pv_ori_set = set(links_pv_ori.loc[links_pv_ori[str(yr)]>0, 'Subsidiary BvD ID'].unique())
        df.loc[(df.bvdid.isin(pv_set)) & (df.closdate_year==yr), 'pv_backed'] = 1
        df.loc[(df.bvdid.isin(pv_fd_set)) & (df.closdate_year==yr), 'pv_fd_backed'] = 1
        df.loc[(df.bvdid.isin(pv_ori_set)) & (df.closdate_year==yr), 'pv_ori_backed'] = 1
    return df


def mergeZephyr(df_panel, df_header, zp):
    '''Merge Zephyr data to the panel data'''
    # get year
    df_panel['year'] = df_panel.closdate_year
    df_panel = zp.mergePanel(df_panel)

    # merge dateinc_year
    df_panel = df_panel.merge(df_header[['bvdid', 'dateinc_year']], how='left', 
                            left_on='zephyr_mbo_acquiror_bvdid', right_on='bvdid', suffixes=[None, '_mbo'], validate='m:1')
    df_panel = df_panel.merge(df_header[['bvdid', 'dateinc_year']], how='left', 
                            left_on='zephyr_mbi_acquiror_bvdid', right_on='bvdid', suffixes=[None, '_mbi'], validate='m:1')
    df_panel = df_panel.merge(df_header[['bvdid', 'dateinc_year']], how='left', 
                        left_on='zephyr_mg_target_bvdid', right_on='bvdid', suffixes=[None, '_mg_acq'], validate='m:1')
    df_panel = df_panel.merge(df_header[['bvdid', 'dateinc_year']], how='left', 
                        left_on='zephyr_acq_acquiror_bvdid', right_on='bvdid', suffixes=[None, '_acq'], validate='m:1')

    # get age
    df_panel['age_acq'] = df_panel.closdate_year - df_panel.dateinc_year_acq
    df_panel['age_mg_acq'] = df_panel.closdate_year - df_panel.dateinc_year_mg_acq
    df_panel['age_mbo'] = df_panel.closdate_year - df_panel.dateinc_year_mbo
    df_panel['age_mbi'] = df_panel.closdate_year - df_panel.dateinc_year_mbi
    return df_panel


def secondFilter(ctry=ctry):
    '''Filter out companies that are not VC/PE backed / listed / delisted / UK Medium Sized (or above) Companies'''
    # get VC/PE backed companies
    df_inv = pd.read_csv(own_hist_fd +  ctry + '/' + ctry + '_links_inv_extract.csv')
    backed_set = set(df_inv['Subsidiary BvD ID'].unique())

    # # get header and merged financial data
    df = pd.read_csv(fin_hist_fd + ctry + '/' + ctry + '_filter1_merge.csv')

    li_set = set(df[df.listed=='Listed'].bvdid.unique())
    de_set = set(df[df.listed=='Delisted'].bvdid.unique())

    # count the number of companies each year
    bvd_stats = pd.DataFrame(index=list(range(1999,2021)), columns=['filter1', 'ipo', 'delist'])
    for yr in range(1999, 2021):
        bvd_stats.loc[yr, 'filter1'] = len(df[(df.closdate_year==yr)].bvdid.unique())
        bvd_stats.loc[yr, 'ipo'] = len(df[(df.ipo_date.dt.year==yr) & (df.closdate_year==yr)].bvdid.unique())
        bvd_stats.loc[yr, 'delist'] = len(df[(df.delisted_date.dt.year==yr) & (df.closdate_year==yr)].bvdid.unique())
        
    df['turn_gbp'] = df.turn/df.exchrate
    df['opre_gbp'] = df.opre/df.exchrate
    df['cuas_gbp'] = df.cuas/df.exchrate
    df['fias_gbp'] = df.fias/df.exchrate
    df['toas_gbp'] = df.toas/df.exchrate

    df['turnover_flag_2'] = (df.turn_gbp >= 6500000) | (df.opre_gbp >= 6500000)
    df['balance_sheet_flag_2'] = ((df.cuas_gbp + df.fias_gbp) >= 3260000)
    df['empl_flag_2'] = df.empl >= 50
    df['medium_flag_2'] = df['turnover_flag_2'].astype(int) + df['balance_sheet_flag_2'].astype(int) + df['empl_flag_2'].astype(int)

    # set medium flag
    empl_idx = df.empl.notna()
    sale_idx = df.turn_gbp.notna() | df.opre_gbp.notna()
    asset_idx = df.cuas_gbp.notna() & df.fias_gbp.notna()

    full_idx = empl_idx & sale_idx & asset_idx
    nan_idx = ~full_idx

    full_m_idx = full_idx & (df.medium_flag_2>=2)
    nan_m_idx = nan_idx & df.balance_sheet_flag_2

    m_idx = full_m_idx | nan_m_idx

    # get medium, backed, listed, delisted sets
    medium_set = set(df[m_idx].bvdid.unique())
    inv_rp_set = set(df[df.bvdid.isin(backed_set)].bvdid.unique())
    li_rp_set = set(df[df.bvdid.isin(li_set)].bvdid.unique())
    de_rp_set = set(df[df.bvdid.isin(de_set)].bvdid.unique())

    for yr in range(1999, 2021):
        bvd_stats.loc[yr, 'medium'] = len(df[(df.closdate_year==yr) & (df.bvdid.isin(medium_set))].bvdid.unique())
        bvd_stats.loc[yr, 'backed'] = len(df[(df.closdate_year==yr) & (df.bvdid.isin(backed_set))].bvdid.unique())
        bvd_stats.loc[yr, 'current_listed'] = len(df[(df.closdate_year==yr) & (df.bvdid.isin(li_set))].bvdid.unique())
        bvd_stats.loc[yr, 'current_delisted'] = len(df[(df.closdate_year==yr) & (df.bvdid.isin(de_set))].bvdid.unique())
        bvd_stats.loc[yr, 'listed'] = len(df[(df.closdate_year==yr) & (df.listed_status==1)].bvdid.unique())

    df_sub = df[(df.bvdid.isin(medium_set)) | (df.bvdid.isin(backed_set)) | (df.bvdid.isin(li_set)) | (df.bvdid.isin(de_set))]

    for yr in range(1999, 2021):
        bvd_stats.loc[yr, 'after'] = len(df_sub[df_sub.closdate_year==yr].bvdid.unique())
        bvd_stats.loc[yr, 'after-ipo-f'] = len(df_sub[(df_sub.ipo_date.dt.year==yr) & (df_sub.closdate_year==yr)].bvdid.unique())
        # bvd_stats.loc[yr, 'after-ipo'] = len(df_sub[(df_sub.ipo_date.dt.year==yr)].bvdid.unique())
        bvd_stats.loc[yr, 'after-delist-f'] = len(df_sub[(df_sub.delisted_date.dt.year==yr) & (df_sub.closdate_year==yr)].bvdid.unique())
        # bvd_stats.loc[yr, 'after-delist'] = len(df_sub[(df_sub.delisted_date.dt.year==yr)].bvdid.unique())
        bvd_stats.loc[yr, 'after-listed-f'] = len(df_sub[(df_sub.closdate_year==yr) & (df_sub.listed_status==1)].bvdid.unique())

    bvd_stats.to_csv(output_fd + ctry + '_filter_2_stats.csv')
    df_sub.to_csv(fin_hist_fd +  ctry + '/' + ctry + '_filter2.csv', index=False)


def consolidateFin(ctry=ctry):
    '''choose financials for each report date'''
    # df = pd.read_csv(fin_hist_fd +  ctry + '/' + ctry + '_filter2.csv')
    df = pd.read_csv(fin_hist_fd + ctry + '/' + ctry + '_filter1_merge.csv')

    print('Before merging unconsolidate and consolidate')
    print(df.conscode.value_counts(dropna=False))

    # remove limited financials
    df = df[df.conscode!='LF']

    # round date to the closest month start date so that nearby dates can cluster
    df['closdate_m'] = pd.to_datetime(df['closdate'])
    df['closdate_d'] = df['closdate_m'].dt.day
    df.loc[df.closdate_d<=15, 'closdate_m'] = df.loc[df.closdate_d<=15, 'closdate_m'] + pd.offsets.MonthBegin(-1)
    df.loc[df.closdate_d>15, 'closdate_m'] = df.loc[df.closdate_d>15, 'closdate_m'] + pd.offsets.MonthBegin(0)

    # there can be multiple C1 (due to different units)
    df['nan_count'] = df.isnull().sum(axis=1)

    # setup bvdid-year pair, which should be unique at the end
    df['bvdid_yr'] = list(zip(df['bvdid'],df['closdate_m'])) # previously closdate

    # remove conscode duplicates, e.g. multiple U1
    print('Removing conscode duplicates using number of nans')
    df = df.sort_values('nan_count').groupby(['bvdid_yr', 'conscode']).head(1).reset_index(drop=True)
    print(df.conscode.value_counts(dropna=False))

    ## for U1-U2 and C1-C2 duplicates, select the one with fewer nans
    uncon_dup = set(df[df.conscode=='U1'].bvdid_yr.unique()) & set(df[df.conscode=='U2'].bvdid_yr.unique())
    print('Number of U1-U2', len(uncon_dup))

    con_dup = set(df[df.conscode=='C1'].bvdid_yr.unique()) & set(df[df.conscode=='C2'].bvdid_yr.unique())
    print('Number of C1-C2', len(con_dup))

    # for bvdid-year that have both U1 and U2, select the one with fewer nans
    uncon_dup_idx = (df.bvdid_yr.isin(uncon_dup)) & (df.conscode.isin(['U1', 'U2']))
    df_u1u2 = df[uncon_dup_idx].copy()
    df = df[~uncon_dup_idx]
    df_u1u2 = df_u1u2.loc[df_u1u2.groupby('bvdid_yr').nan_count.idxmin()].reset_index(drop=True)

    # for bvdid-year that have both C1 and C2, select the one with fewer nans
    con_dup_idx = (df.bvdid_yr.isin(con_dup)) & (df.conscode.isin(['C1', 'C2']))
    df_c1c2 = df[con_dup_idx].copy()
    df = df[~con_dup_idx]
    df_c1c2 = df_c1c2.loc[df_c1c2.groupby('bvdid_yr').nan_count.idxmin()].reset_index(drop=True)

    # combine
    print('Number of U1-U2 duplicates', len(df_u1u2))
    print('Number of C1-C2 duplicates', len(df_c1c2))
    df = pd.concat([df, df_u1u2, df_c1c2], axis=0)
    print(df.conscode.value_counts(dropna=False))

    # remap U1/U2, C1/C2
    df['report_count'] = df.groupby('bvdid_yr')["bvdid"].transform('count')
    df = df.sort_values('bvdid_yr')

    print('Before remapping, number of single U2:', ((df.report_count==1) & (df.conscode=='U2')).sum())
    df.loc[df.report_count>1, 'conscode'] = df.loc[df.report_count>1, 'conscode'].str.replace('1', '2')
    df.loc[df.report_count==1, 'conscode'] = df.loc[df.report_count==1, 'conscode'].str.replace('2', '1')
    print('After remapping, number of single U2:', ((df.report_count==1) & (df.conscode=='U2')).sum())
    print(df.conscode.value_counts(dropna=False))

    # remove U2 since we already have C2
    df = df[df.conscode!='U2']

    df = df.reset_index(drop=True)
    df.drop(columns=['dup_count', 'closdate_d', 'bvdid_yr', 'report_count'], inplace=True)
    # df.to_csv(fin_hist_fd +  ctry + '/' + ctry + '_cons.csv', index=False)
    df.to_csv(fin_hist_fd +  ctry + '/' + ctry + '_filter1_merge_cons.csv', index=False)


def chooseReportInYear(ctry):
    '''Choose the report with the most months in the year'''
    df = pd.read_csv(fin_hist_fd +  ctry + '/' + ctry + '_filter1_merge_cons.csv')
    df = df.loc[df.groupby(['bvdid', 'closdate_year']).nr_months.idxmax()].reset_index(drop=True)
    df.to_csv(fin_hist_fd +  ctry + '/' + ctry + '_filter1_merge_cons_yr.csv', index=False)

    
def expandReportYear(ctry, start_yr=1990):
    '''Expand the report year to the full year'''
    df = pd.read_csv(fin_hist_fd +  ctry + '/' + ctry + '_filter1_merge_cons_yr.csv')

    # get the first and the last report year
    df['first_yr'] = df.groupby('bvdid')['closdate_year'].transform('min')
    df['last_yr'] = df.groupby('bvdid')['closdate_year'].transform('max')

    # merge header to get dateinc_year and use the earlier of the dateinc_year and first_yr
    df_header = getMergedHeader(ctry)
    df = mergeHeader(df, df_header)
    df['first_yr_ori'] = df['first_yr'].copy()
    df['first_yr_2'] = df[['dateinc_year', 'first_yr']].min(axis=1)
    df['dateinc_yr_new'] = df['first_yr_2'].copy()
    df['first_yr_2'] = df['first_yr_2'].clip(lower=start_yr)
    print('Before expanding:', len(df))

    # convert ot a panel format
    df = df[df['closdate_year']>=start_yr]
    df.set_index(['bvdid', 'closdate_year'], inplace=True)
    index = pd.MultiIndex.from_product(df.index.levels)
    df = df.reindex(index)
    df = df.reset_index()
    print('After expanding for all years:', len(df))

    # propagate the first and the last report year
    df['first_yr_2'] = df.groupby('bvdid')['first_yr_2'].transform('min')
    df['last_yr'] = df.groupby('bvdid')['last_yr'].transform('max')
    df = df[(df.closdate_year>=df.first_yr_2) & (df.closdate_year<=df.last_yr)]
    print('After expanding for years with reports:', len(df))
    df.to_csv(fin_hist_fd +  ctry + '/' + ctry + '_filter1_merge_cons_yr_panel.csv', index=False)


def uksicLabel(ent, idx, sic_list, type_list, assign_ent, preserve=True, preserve_list=[], assign=True):
    '''helper function to assign entities'''
    if assign:
        ent.loc[idx & (ent.uksic.isin(sic_list)) & (ent['ent_type'].isin(type_list)), 'ent_final'] = assign_ent
    if preserve:
        if len(preserve_list)==0:
            preserve_idx = (~ent['ent_type'].isin(type_list))
        else:
            preserve_idx = ent['ent_type'].isin(preserve_list)
        temp_idx = idx & (ent.uksic.isin(sic_list)) & preserve_idx
        ent.loc[temp_idx, 'ent_final'] = ent.loc[temp_idx, 'ent_type']
    idx = idx & (ent.ent_final.isna())   
    return ent, idx


def uksicToEnt(ent):
    '''Assign entities based on uksic'''
    # remove invalid uksic
    ent['uksic'] = ent['uksic'].replace('791', np.nan)
    ent['uksic'] = ent['uksic'].replace('80', np.nan)
    ent['uksic'] = ent['uksic'].replace('772', np.nan)
    ent['uksic'] = ent['uksic'].replace('711', '99999')
    
    # add leading zeros 
    temp_idx = ent.uksic.str.len()<=4
    ent.loc[temp_idx, 'uksic'] = '0' + ent.loc[temp_idx, 'uksic']
   
    ent['ent_final'] = np.nan
    idx = ent['uksic'].notna()
    full_ent_type = list(ent['ent_type'].unique())

    # preserve QJS
    temp_idx = idx & (ent.ent_type.isin(['Q', 'J', 'S']))
    ent.loc[temp_idx, 'ent_final'] = ent.loc[temp_idx, 'ent_type']
    idx = idx & (ent.ent_final.isna())

    # label banks
    ent.loc[idx & (ent.uksic.isin(['64110','64191'])) & (~ent['ent_type'].isin(['V','P'])), 'ent_final'] = 'B'
    temp_idx = idx & (ent.uksic.isin(['64110','64191'])) & (ent['ent_type'].isin(['V','P']))
    ent.loc[temp_idx, 'ent_final'] = ent.loc[temp_idx, 'ent_type']
    idx = idx & (ent.ent_final.isna())

    # label insurance companies
    fc_idx = ent.bvdid.str[:4]=='GBFC'
    ent.loc[idx & (~fc_idx) & (ent.uksic.isin(['65110','65120','65201','65202'])), 'ent_final'] = 'A'
    temp_idx = idx & fc_idx & (ent.uksic.isin(['65110','65120','65201','65202']))
    ent.loc[temp_idx, 'ent_final'] = ent.loc[temp_idx, 'ent_type']
    idx = idx & (ent.ent_final.isna())

    # label foundations and research institutes
    ent, idx = uksicLabel(ent, idx, ['72200', '85100', '85200', '85310', '85421', '85422', '85520', '85560', '85590'], 
                          type_list=full_ent_type, assign_ent='J', preserve=False)

    # label public authorities, states, governments
    ent, idx = uksicLabel(ent, idx, ['84110', '84120', '84130', '84210', '84230'], type_list=full_ent_type, assign_ent='S', preserve=False)

    # label ventures
    ent.loc[idx & (ent.uksic.str[:5]=='64303') & (ent['ent_type']!='P'), 'ent_final'] = 'V'
    ent.loc[idx & (ent.uksic.str[:5]=='64303') & (ent['ent_type']=='P'), 'ent_final'] = 'P'
    idx = idx & (ent.ent_final.isna())

    ent, idx = uksicLabel(ent, idx, ['64205', '64302', '64304', '64305', '82911', '64910', '64929', '64991', '64992', '64999', '66290', '64192', '70221'], 
                          type_list=['C'], assign_ent='F', preserve=True, preserve_list=['E','P','V','Y','F'])
    
    ent, idx = uksicLabel(ent, idx, ['64301', '64306', '64921', '66190'], type_list=['B', 'C'], assign_ent='F', preserve=True)
    ent, idx = uksicLabel(ent, idx, ['64922', '66120'], type_list=['A', 'B', 'C'], assign_ent='F', preserve=True)
    ent, idx = uksicLabel(ent, idx, ['65300', '66300'], type_list=['C'], assign_ent='E', preserve=True)
    ent, idx = uksicLabel(ent, idx, ['66110'], type_list=['C','A','B','P','V','F'], assign_ent='F', preserve=True, preserve_list=['E'])
    ent, idx = uksicLabel(ent, idx, ['64209', '85600', '93199'], type_list=['F'], assign_ent='C', preserve=True)
    ent, idx = uksicLabel(ent, idx, ['68201'], type_list=['F','A','B','C'], assign_ent='C', preserve=True, preserve_list=['E','Q'])
    ent, idx = uksicLabel(ent, idx, ['68202'], type_list=['F','B','C'], assign_ent='C', preserve=True, preserve_list=['E','Q'])
    ent, idx = uksicLabel(ent, idx, ['86900'], type_list=['E','F','A','C'], assign_ent='C', preserve=False)
    ent, idx = uksicLabel(ent, idx, ['90010'], type_list=['E','F'], assign_ent='C', preserve=True)
    ent, idx = uksicLabel(ent, idx, ['90020','85410'], type_list=['E'], assign_ent='J', preserve=False)
    ent, idx = uksicLabel(ent, idx, ['64201', '64202', '64203', '64204', '66210', '66220', '85320', '85410', '85510', '90020'], 
                          type_list=full_ent_type, assign_ent='C', preserve=False)
    ent, idx = uksicLabel(ent, idx, ['70229'], type_list=['B','A'], assign_ent='F', preserve=True)
    ent, idx = uksicLabel(ent, idx, ['68100', '68209', '68310', '68320', '72110', '72190', '90030', '84220', '84240', '84250', '08990', '09900'], 
                          type_list=[], assign_ent='', preserve=True, preserve_list=[], assign=False)

    # label C
    temp_list = ['0'+str(i) for i in range(10)] + [str(i) for i in range(10,64)]
    ent.loc[idx & (ent.uksic.str[:2].isin(temp_list)), 'ent_final'] = 'C'
    idx = idx & (ent.ent_final.isna())

    temp_list = ['64205','64910','64991','64992','64999','66290','70100','70221','74909','82990','94200','96090',
                '97000', '98000', '98100', '98200', '99000', '99999']
    ent, idx = uksicLabel(ent, idx, sic_list=temp_list, type_list=[], assign_ent='', preserve=True, preserve_list=[], assign=False)

    temp_list = ['69101','69102','69109','69201','69202','69203', '70210','71111', '71112', '71121','71122', '71129','71200',
                    '73110', '73120', '73200', '74100', '74201', '74202', '74203', '74209', '74300', '74901', '74902', '75000',
                    '77110', '77120', '77210', '77220', '77291', '77299', '77310', '77320', '77330', '77341', '77342', '77351', '77352', '77390', '77400',
                    '78100', '78101', '78109', '78200', '78300', '79110', '79120', '79901', '79909',
                    '80100', '80200', '80300', '81100', '81210', '81221', '81222', '81223', '81229', '81291', '81299', '81300',
                    '82110', '82190', '82200', '82301', '82302', '82912', '82920', '84300', '85530', '86101', '86102', '86210', '86220', '86230', 
                    '87100', '87200', '87300', '87900', '88100', '88910', '88990', '90040', '91011', '91012', '91040', '92000',
                    '93110', '93120', '93130', '93191', '93210', '93290', '95110', '95120', '95210', '95220', '95230', '95240', '95250', '95290',
                    '96010', '96020', '96030', '96040']
    ent, idx = uksicLabel(ent, idx, sic_list=temp_list, type_list=full_ent_type, assign_ent='C', preserve=False)    
    ent, idx = uksicLabel(ent, idx, ['91020', '91030', '94120', '94910', '94920', '94990'], type_list=full_ent_type, assign_ent='J', preserve=False)
    ent, idx = uksicLabel(ent, idx, ['94110'], type_list=['C'], assign_ent='J', preserve=True)

    temp_idx = ent.ent_final.isna()
    ent.loc[temp_idx, 'ent_final'] = ent.loc[temp_idx, 'ent_type']

    return ent


def mergeIndustries(ctry=ctry):
    '''Merge the industries to the panel'''
    df = pd.read_csv(fin_hist_fd +  ctry + '/' + ctry + '_filter1_merge_cons_yr_panel.csv')
    # read and merge combined industry classifications (wrds + historical data)
    sic = pd.read_csv(des_hist_fd + ctry + '/' + ctry + '_sic.csv')
    naics = pd.read_csv(des_hist_fd + ctry + '/' + ctry + '_naics.csv')
    uksic = pd.read_csv(des_hist_fd + ctry + '/' + ctry + '_uksic.csv')

    sic = sic.groupby('bvdid').nth(0).reset_index()
    naics = naics.groupby('bvdid').nth(0).reset_index()
    uksic = uksic.groupby('bvdid').nth(0).reset_index()

    df = df.merge(sic, how='left', on='bvdid', validate='m:1')
    df = df.merge(naics, how='left', on='bvdid', validate='m:1')
    df = df.merge(uksic, how='left', on='bvdid', validate='m:1')

    # read and merge zephyr industry classifications (by deal year)
    zp = processZephyr.Zephyr()
    zp_tag = zp.df_deal[['year', 'Target BvD ID number', 'Target primary UK SIC (2007) code', 'Target primary NAICS 2017 code', 'Target primary US SIC code', 'Target primary NACE Rev.2 code']]
    zp_acq = zp.df_deal[['year', 'Acquiror BvD ID number', 'Acquiror primary UK SIC (2007) code', 'Acquiror primary NAICS 2017 code', 'Acquiror primary US SIC code', 'Acquiror primary NACE Rev.2 code']]

    zp_tag.rename(columns={'Target BvD ID number':'bvdid',
                        'Target primary UK SIC (2007) code':'zp_uksic', 'Target primary NAICS 2017 code':'zp_naics', 
                        'Target primary US SIC code':'zp_ussic', 'Target primary NACE Rev.2 code':'zp_nace'}, inplace=True)

    zp_acq.rename(columns={'Acquiror BvD ID number':'bvdid', 
                        'Acquiror primary UK SIC (2007) code':'zp_uksic', 'Acquiror primary NAICS 2017 code':'zp_naics', 
                        'Acquiror primary US SIC code':'zp_ussic', 'Acquiror primary NACE Rev.2 code':'zp_nace'}, inplace=True)

    zp_all = pd.concat([zp_tag, zp_acq], axis=0)
    zp_all = zp_all[zp_all.bvdid.str.contains('GB', na=False)]
    zp_all = zp_all.groupby(['bvdid', 'year']).nth(0).reset_index()
    zp_all['zp_uksic'] = zp_all['zp_uksic'].str[:5]
    zp_all['zp_naics'] = zp_all['zp_naics'].str[:5]
    zp_all['zp_ussic'] = zp_all['zp_ussic'].str[:4]
    zp_all['zp_nace'] = zp_all['zp_nace'].str[:4]
    zp_all.rename(columns={'year':'closdate_year'}, inplace=True)

    # merge by bvdid and deal year
    df = df.merge(zp_all, how='left', on=['bvdid', 'closdate_year'], validate='m:1')
    for col in ['zp_uksic', 'zp_naics', 'zp_ussic', 'zp_nace']:
        df[col] = df[col].astype(float)

    # get combined SIC code (wrds + historical data + zephyr)
    # backward fill the zephyr industry classifications from the deal year
    df['zp_uksic'] = df.groupby(['bvdid']).zp_uksic.bfill()
    df['sic_comb'] = df['zp_uksic']
    df.loc[df.sic_comb.isna(), 'sic_comb'] = df.loc[df.sic_comb.isna(), 'uksic']

    # cast type to string and assign back to uksic
    df['uksic'] = df['sic_comb']
    df.drop(columns=['sic_comb'], inplace=True)
    idx = df['uksic'].notna()
    df.loc[idx, 'uksic'] = df.loc[idx, 'uksic'].astype(int).astype(str)    

    # merge the entity type
    ent = pd.read_csv(own_hist_fd + ctry + '/entities_' + ctry + '.csv')
    ent.rename(columns={'BvD ID of the subsidiary or shareholder':'bvdid','Entity type':'ent_type'},inplace=True)
    ent = ent[['bvdid', 'ent_type']]
    df = df.merge(ent, how='left', on='bvdid', validate='m:1')

    # use uksic to assign the entity type
    df = uksicToEnt(df)

    df.to_csv(fin_hist_fd +  ctry + '/' + ctry + '_filter1_merge_cons_yr_panel_ind.csv', index=False)


def getEntityIndustries(ctry):
    '''merge industries to the entity file'''
    # read the processed SIC file
    uksic = pd.read_csv(des_hist_fd + ctry + '/' + ctry + '_uksic.csv')
    uksic = uksic.groupby('bvdid').nth(0).reset_index()

    # read the entity file
    ent = pd.read_csv(own_hist_fd + ctry + '/entities_' + ctry + '.csv')
    ent.rename(columns={'BvD ID of the subsidiary or shareholder':'bvdid','Entity type':'ent_type', 'Name':'name'},inplace=True)
    ent = ent[['bvdid', 'name', 'ent_type']]
    ent = ent.merge(uksic, how='left', on='bvdid', validate='m:1')

    # cast type to string and assign back to uksic
    idx = ent['uksic'].notna()
    ent.loc[idx, 'uksic'] = ent.loc[idx, 'uksic'].astype(int).astype(str)

    # use uksic to assign the entity type
    ent = uksicToEnt(ent)

    # save the file
    ent.to_csv(own_hist_fd + ctry + '/' + ctry + '_entities_sic.csv', index=False)


def toPanel(ctry=ctry, start_yr=2002):
    # get header, entities, df
    ind_set, inv_set, entity = getEntitiesSets(ctry)
    entity.set_index('BvD ID of the subsidiary or shareholder', inplace=True)
    df = pd.read_csv(fin_hist_fd +  ctry + '/' + ctry + '_cons.csv')

    li_set = set(df[df.listed=='Listed'].bvdid.unique())
    de_set = set(df[df.listed=='Delisted'].bvdid.unique())

    # merged
    df = df.merge(entity['Entity type'], how='left', left_on='bvdid', right_index=True)

    # medium flag
    # change currency
    df['turn_gbp'] = df.turn/df.exchrate
    df['opre_gbp'] = df.opre/df.exchrate
    df['cuas_gbp'] = df.cuas/df.exchrate
    df['fias_gbp'] = df.fias/df.exchrate
    df['toas_gbp'] = df.toas/df.exchrate

    # get UK medium indicators
    df['turnover_flag_2'] = (df.turn_gbp >= 6500000) | (df.opre_gbp >= 6500000)
    df['balance_sheet_flag_2'] = ((df.cuas_gbp + df.fias_gbp) >= 3260000)
    df['empl_flag_2'] = df.empl >= 50
    df['medium_flag_2'] = df['turnover_flag_2'].astype(int) + df['balance_sheet_flag_2'].astype(int) + df['empl_flag_2'].astype(int)

    # set medium flag
    empl_idx = df.empl.notna()
    sale_idx = df.turn_gbp.notna() | df.opre_gbp.notna()
    asset_idx = df.cuas_gbp.notna() & df.fias_gbp.notna()

    full_idx = empl_idx & sale_idx & asset_idx
    nan_idx = ~full_idx

    full_m_idx = full_idx & (df.medium_flag_2>=2)
    nan_m_idx = nan_idx & df.balance_sheet_flag_2

    m_idx = full_m_idx | nan_m_idx

    # set the flag
    df['m_flag'] = 0
    df.loc[m_idx, 'm_flag'] = 1

    # setup our own definition of size
    df['category_of_company_2'] = 'SMALL COMPANY'
    df.loc[(df.toas>=2600000) | (df.empl>=15) | (df.opre>=1300000), 'category_of_company_2'] = 'MEDIUM SIZED COMPANY'
    df.loc[(df.toas>=26000000) | (df.empl>=150) | (df.opre>=13000000), 'category_of_company_2'] = 'LARGE COMPANY'
    df.loc[(df.toas>=260000000) | (df.empl>=1000) | (df.opre>=130000000), 'category_of_company_2'] = 'VERY LARGE COMPANY'

    # dataframes for storing statistics
    up_check = pd.DataFrame(index=list(range(start_yr,2021)), columns=['up-f', 'small', 'medium', 'large', 'very_large', 'uk_s', 'uk_m', 'listed'])
    ch_check = pd.DataFrame(index=list(range(start_yr,2021)), columns=['ch-f', 'small', 'medium', 'large', 'very_large', 'uk_s', 'uk_m', 'listed'])
    ip_check = pd.DataFrame(index=list(range(start_yr,2021)), columns=['ip-f', 'ip-ind', 'small', 'medium', 'large', 'very_large', 'uk_s', 'uk_m', 'listed'])

    # start looping through each year
    df_proc = []
    min_links_yr = 1999

    for yr in range(start_yr, 2021): 
        print(yr)
        df_yr = df[df.closdate_year==yr].copy()
        df_yr = df_yr[df_yr.bvdid.isin(ind_set)].copy()
        
        total_set = set(df_yr.bvdid.unique())
        
        # get one entry per year
        df_yr = df_yr.reset_index(drop=True)
        df_yr = df_yr.loc[df_yr.groupby('bvdid').nr_months.idxmax()].reset_index(drop=True)  
        
        # get the combined links
        links_yr = yr if yr >= min_links_yr else min_links_yr
        df_comb = pd.read_csv(own_hist_fd +  ctry + '/comb_' + ctry + '_' + str(links_yr) + '.csv')
        up_set = set(df_comb['up_bvdid'].unique())
        ch_set = set(df_comb['Subsidiary BvD ID'].unique())
        
        ch_up = df_comb.groupby('Subsidiary BvD ID').nth(-1)
        ch_up.index.name = 'bvdid'
        
        # get the independent set
        ip_set = total_set - up_set - ch_set
        df_ip = df_yr[df_yr.bvdid.isin(ip_set&ind_set)].copy()

        # label for whether it is ip or up, or ch
        print('before', len(df_yr))
        df_yr = df_yr.merge(ch_up['up_bvdid'], how='left', left_on='bvdid', right_index=True)
        print('after', len(df_yr))
        df_yr['upchip'] = 0
        df_yr.loc[df_yr.bvdid.isin(up_set), 'upchip'] = 1
        df_yr.loc[df_yr.bvdid.isin(ch_set), 'upchip'] = 2
        
        # for independent companies, select the report with fewer nans for each year 
        df_ip['independent'] = 1

        # get the ultimate parents
        df_up = df_yr[df_yr.bvdid.isin(up_set)].copy()
        df_ch = df_yr[df_yr.bvdid.isin(ch_set)].copy()
        df_up['independent'] = 0
        
        # combine up and ip
        # df_yr_proc = pd.concat([df_up, df_ip], axis=0)
        
        # add per year data
        # df_proc.append(df_yr_proc)
        df_proc.append(df_yr)

        # fill in check for up
        up_check.loc[yr, 'up-f'] = len(df_up)
        up_check.loc[yr, 'small'] = len(df_up[df_up.category_of_company_2=='SMALL COMPANY'])
        up_check.loc[yr, 'medium'] = len(df_up[df_up.category_of_company_2=='MEDIUM SIZED COMPANY'])
        up_check.loc[yr, 'large'] = len(df_up[df_up.category_of_company_2=='LARGE COMPANY'])
        up_check.loc[yr, 'very_large'] = len(df_up[df_up.category_of_company_2=='VERY LARGE COMPANY'])
        up_check.loc[yr, 'uk_s'] = len(df_up[df_up.m_flag==0])
        up_check.loc[yr, 'uk_m'] = len(df_up[df_up.m_flag==1])
        # up_check.loc[yr, 'listed'] = len(df_up[df_up.bvdid.isin(li_set)])
        # up_check.loc[yr, 'delisted'] = len(df_up[df_up.bvdid.isin(de_set)])
        up_check.loc[yr, 'listed'] = len(df_up[df_up.listed_status==1])

        # fill in check for ch
        ch_check.loc[yr, 'ch-f'] = len(df_ch)
        ch_check.loc[yr, 'small'] = len(df_ch[df_ch.category_of_company_2=='SMALL COMPANY'])
        ch_check.loc[yr, 'medium'] = len(df_ch[df_ch.category_of_company_2=='MEDIUM SIZED COMPANY'])
        ch_check.loc[yr, 'large'] = len(df_ch[df_ch.category_of_company_2=='LARGE COMPANY'])
        ch_check.loc[yr, 'very_large'] = len(df_ch[df_ch.category_of_company_2=='VERY LARGE COMPANY'])
        ch_check.loc[yr, 'uk_s'] = len(df_ch[df_ch.m_flag==0])
        ch_check.loc[yr, 'uk_m'] = len(df_ch[df_ch.m_flag==1])
        # ch_check.loc[yr, 'listed'] = len(df_ch[df_ch.bvdid.isin(li_set)])
        # ch_check.loc[yr, 'delisted'] = len(df_ch[df_ch.bvdid.isin(de_set)])
        ch_check.loc[yr, 'listed'] = len(df_ch[df_ch.listed_status==1])
        
        # fill in check for ip
        ip_check.loc[yr, 'ip-f'] = len(df_yr[df_yr.bvdid.isin(ip_set)])
        ip_check.loc[yr, 'ip-ind'] = len(df_ip)
        ip_check.loc[yr, 'small'] = len(df_ip[df_ip.category_of_company_2=='SMALL COMPANY'])
        ip_check.loc[yr, 'medium'] = len(df_ip[df_ip.category_of_company_2=='MEDIUM SIZED COMPANY'])
        ip_check.loc[yr, 'large'] = len(df_ip[df_ip.category_of_company_2=='LARGE COMPANY'])
        ip_check.loc[yr, 'very_large'] = len(df_ip[df_ip.category_of_company_2=='VERY LARGE COMPANY'])
        ip_check.loc[yr, 'uk_s'] = len(df_ip[df_ip.m_flag==0])
        ip_check.loc[yr, 'uk_m'] = len(df_ip[df_ip.m_flag==1])
        # ip_check.loc[yr, 'listed'] = len(df_ip[df_ip.bvdid.isin(li_set)])
        # ip_check.loc[yr, 'delisted'] = len(df_ip[df_ip.bvdid.isin(de_set)])
        ip_check.loc[yr, 'listed'] = len(df_ip[df_ip.listed_status==1])
        
    df_proc = pd.concat(df_proc, axis=0)     
    df_proc.to_csv(fin_hist_fd +  ctry + '/' + ctry + '_proc_label_' + str(start_yr) + '.csv', index=False)

    # save statistics
    up_check.to_csv(output_fd + ctry + '_up_after_filter_check.csv')
    ch_check.to_csv(output_fd + ctry + '_ch_after_filter_check.csv')
    ip_check.to_csv(output_fd + ctry + '_ip_after_filter_check.csv')


def concatRestructuring(df_panel, age_up_cap=2):
    '''Concatenate the restructuring firms'''
    df_panel = df_panel[df_panel.bvdid.str[:2]=='GB'] #TODO: this line should appear earlier in the pipeline
    df_panel['closdate_year'] = df_panel['closdate_year'].astype(float)
    df_panel.sort_values(['bvdid', 'closdate_year'], inplace=True)
    
    df_panel['first_yr'] = df_panel.groupby('bvdid')['closdate_year'].transform('min')
    df_panel['last_yr'] = df_panel.groupby('bvdid')['closdate_year'].transform('max')
    # df_panel.loc[:, 'rst_pr'] = np.nan

    # process uksic as it will be read as float
    idx = df_panel['uksic'].notna()
    df_panel.loc[idx, 'uksic'] = df_panel.loc[idx, 'uksic'].astype(int).astype(str)
    temp_idx = df_panel['uksic'].str.len()<=4
    df_panel.loc[temp_idx, 'uksic'] = '0' + df_panel.loc[temp_idx, 'uksic']
    d4_dict = {'01812':'18120', '02041':'20410', '03320':'33200', 
            '04120':'41200', '04511':'45110', '04671':'46710', '05120':'65120', '06499':'64302', '07420':'74200'}
    df_panel['uksic'] = df_panel['uksic'].replace(d4_dict)

    # read in the restructuring data
    rstr_df = pd.read_csv('additional_data/cpst_rstr.csv')
    rstr_merge = rstr_df[['bvdid', 'rst_pr', 'rst_yr']] 
    df_panel = df_panel.merge(rstr_merge, how='left', on=['bvdid'])

    # add a flag for cpst
    df_panel.loc[df_panel['rst_pr'].notna(), 'cpst_rstr_flag'] = 1
    # take the min of the last year and the delist year as the restructuring year
    df_panel['rst_yr'] = df_panel[['rst_yr', 'last_yr']].min(axis=1)
    # clear other entries except for the restructuring year
    df_panel.loc[df_panel['closdate_year']!=df_panel['rst_yr'], 'rst_pr'] = np.nan

    # get index for restructuring firms
    hc_acq_idx = df_panel.hc_acq_reason1.notna()
    rst_acq_idx = (df_panel.zephyr_acq_deal_subtype.str.contains('restructuring', na=False)) & (df_panel.zephyr_acq_acquiror_bvdid.str.contains('GB', na=False)) & (df_panel.age_acq==0)
    rst_mg_idx = (df_panel.zephyr_mg_acq == 1) & (df_panel.zephyr_mg_target_bvdid.str.contains('GB', na=False)) & (df_panel.age_mg_acq==0)
    rst_mbo_idx = (df_panel.zephyr_mbo == 1) & (df_panel.zephyr_mbo_acquiror_bvdid.str.contains('GB', na=False)) & (df_panel.age_mbo==0)
    rst_mbi_idx = (df_panel.zephyr_mbi == 1) & (df_panel.zephyr_mbi_acquiror_bvdid.str.contains('GB', na=False)) & (df_panel.age_mbi==0)
    lc_idx = (df_panel.prev_up_bvdid!=df_panel.up_bvdid) & (df_panel.up_bvdid.notna()) & (df_panel.closdate_year!=df_panel.first_yr)
    lc_rst_idx = lc_idx & (df_panel.age_up==0) #TODO: consider age <=1 and no acq deals
    # add one more condition for special industries
    # merge the industries of up_bvdid
    df_panel = df_panel.merge(df_panel[['bvdid', 'closdate_year', 'uksic']], how='left', left_on=['up_bvdid','closdate_year'], right_on=['bvdid','closdate_year'], suffixes=('', '_up'), validate='m:1')
    # take the first two characters of the uksic code
    temp_idx = df_panel.uksic_up.notna()
    df_panel.loc[temp_idx, 'uksic_up'] = df_panel.loc[temp_idx, 'uksic_up'].astype(str).str[:2]
    # get the index for special industries (64, 70, 82)
    lc_rst_idx = lc_rst_idx | (lc_idx & (df_panel.age_up<=age_up_cap) & (df_panel['uksic_up'].isin(['64', '70', '82'])))

    # get parents of the restructuring firms
    df_panel.loc[rst_acq_idx & (df_panel.rst_pr.isna()), 'rst_pr'] = df_panel.loc[rst_acq_idx, 'zephyr_acq_acquiror_bvdid']
    df_panel.loc[rst_mg_idx & (df_panel.rst_pr.isna()), 'rst_pr'] = df_panel.loc[rst_mg_idx & (df_panel.rst_pr.isna()), 'zephyr_mg_target_bvdid']
    df_panel.loc[rst_mbo_idx & (df_panel.rst_pr.isna()), 'rst_pr'] = df_panel.loc[rst_mbo_idx & (df_panel.rst_pr.isna()), 'zephyr_mbo_acquiror_bvdid']
    df_panel.loc[rst_mbi_idx & (df_panel.rst_pr.isna()), 'rst_pr'] = df_panel.loc[rst_mbi_idx & (df_panel.rst_pr.isna()), 'zephyr_mbi_acquiror_bvdid']
    df_panel.loc[lc_rst_idx & (df_panel.rst_pr.isna()), 'rst_pr'] = df_panel.loc[lc_rst_idx & (df_panel.rst_pr.isna()), 'up_bvdid']

    # ignore hand checked files if the 0-age firm has a subsidiary
    rst_pr_temp_set = set(df_panel.rst_pr.dropna().unique())
    df_panel.loc[hc_acq_idx & (df_panel.rst_pr.isna()) & (~df_panel.hc_acq_bvdid.isin(rst_pr_temp_set)), 'rst_pr'] = df_panel.loc[hc_acq_idx & (df_panel.rst_pr.isna()), 'hc_acq_bvdid']

    # remove incorrect rst_pr
    df_panel.loc[df_panel.rst_pr==df_panel.bvdid, 'rst_pr'] = np.nan

    # extract the restructuring firms (including subsidiaries and parents)
    temp_set = set(df_panel[df_panel.rst_pr.notna()].bvdid.unique()) | set(df_panel[df_panel.rst_pr.notna()].rst_pr.unique())
    temp = df_panel[df_panel.bvdid.isin(temp_set)].copy()
 
    # skip one outlier (This company has a ZP deal in 2012, but the actual restructuring year is 2010 and they point to the same parent company.)
    temp.loc[(temp['bvdid']=='GB00251977') & (temp['closdate_year']==2012), ['rst_pr', 'rst_yr']] = (np.nan, np.nan)

    # treat the cpst firms differently
    cpst_rst_pr_set = list(df_panel.loc[df_panel['cpst_rstr_flag']==1, 'rst_pr'].unique())
    # if there are more than one subsidiaries, get the one with the largest toas
    temp_idx = temp.groupby(['rst_pr', 'closdate_year'])['toas'].transform(max) == temp['toas']
    temp_idx = (temp_idx & (~df_panel['rst_pr'].isin(cpst_rst_pr_set))) | ( (df_panel['cpst_rstr_flag']==1) & (df_panel['rst_pr'].notna()) )
    temp = temp[temp_idx]

    # if age_up_cap>0, pick the youngest subsidiary
    if age_up_cap>0:
        # temp_idx = temp.groupby(['rst_pr'])['closdate_year'].transform(min) == temp['closdate_year']
        # temp = temp[temp_idx]
        temp = temp.sort_values(['rst_pr', 'closdate_year'])
        temp = temp.groupby(['rst_pr']).nth(0).reset_index()

    # make sure one up only has one subsidiary
    assert(len(temp.groupby(['rst_pr', 'closdate_year']).nth(0).reset_index()) == len(temp.groupby(['rst_pr']).nth(0).reset_index()))
    temp = temp.groupby(['rst_pr', 'closdate_year']).nth(0).reset_index()[['bvdid', 'closdate_year', 'rst_pr']]
    temp = temp[temp.rst_pr.str.contains('GB')]

    # start the propagation algorithm
    prop = temp[['rst_pr', 'bvdid']].copy()

    # get ultimate parents
    sub_set = set(prop['bvdid'].unique())
    pr_set = set(prop['rst_pr'].unique())
    up_set = pr_set - sub_set

    # keep merging until all subsidiaries are found
    prop = prop.merge(prop[['rst_pr', 'bvdid']].rename(columns={'rst_pr':'bvdid','bvdid':'bvdid2'}), how='left', on='bvdid')
    prop = prop.merge(prop[['rst_pr', 'bvdid']].rename(columns={'rst_pr':'bvdid2','bvdid':'bvdid3'}), how='left', on='bvdid2')
    prop = prop.merge(prop[['rst_pr', 'bvdid']].rename(columns={'rst_pr':'bvdid3','bvdid':'bvdid4'}), how='left', on='bvdid3')
    if age_up_cap==0:
        assert(prop.bvdid4.notna().sum()==0)

    # split the links
    prop = prop[prop['rst_pr'].isin(up_set)]
    prop.rename(columns={'rst_pr':'rst_up'}, inplace=True)
    prop2 = prop.loc[prop['bvdid2'].notna(), ['rst_up', 'bvdid', 'bvdid2']].rename(columns={'bvdid2':'bvdid', 'bvdid':'rst_dir_pr'})
    prop3 = prop.loc[prop['bvdid3'].notna(), ['rst_up', 'bvdid2', 'bvdid3']].rename(columns={'bvdid3':'bvdid', 'bvdid2':'rst_dir_pr'})
    prop = prop.drop(columns=['bvdid2', 'bvdid3', 'bvdid4'])
    prop['rst_dir_pr'] = prop['rst_up']
    prop4 = pd.concat([prop, prop2, prop3], axis=0)
    # print(len(prop.rst_up.unique()), len(prop))
    assert(len(prop.rst_up.unique()) == len(prop))

    # split the cases
    prop4['id_count'] = prop4.groupby('bvdid')['rst_up'].transform('count')
    if age_up_cap==0:
        assert(prop4.id_count.max()<=3) #There is only one case with 3 up firms, should be fine to ignore
    prop5 = prop4[prop4.id_count>1].sort_values('bvdid')
    prop_col1 = prop5.groupby('bvdid').nth(0).reset_index()[['bvdid', 'rst_up', 'rst_dir_pr']]
    prop_col2 = prop5.groupby('bvdid').nth(-1).reset_index()[['bvdid', 'rst_up', 'rst_dir_pr']]
    prop5 = prop_col1.merge(prop_col2, how='left', on='bvdid', suffixes=[None,'_2'], validate='1:1')

    # merge the cases
    prop6 = prop4[prop4.id_count<2].drop(columns='id_count')
    prop_merge = pd.concat([prop5, prop6], axis=0)
    assert(len(prop_merge) == len(prop_merge.bvdid.unique()))

    # merge the results
    df_panel = df_panel.merge(prop_merge, how='left', on='bvdid', validate='m:1')
    # fill the ultimate parents
    temp_idx = df_panel.bvdid.isin(up_set)
    df_panel.loc[temp_idx, 'rst_up'] = df_panel.loc[temp_idx, 'bvdid']    

    # get the first year of the two direct parents
    rst_first_yr = df_panel.groupby('bvdid')['first_yr'].nth(0).reset_index()
    df_panel = df_panel.merge(rst_first_yr.rename(columns={'bvdid':'rst_dir_pr','first_yr':'rst_pr_first_yr'}), how='left', on='rst_dir_pr', validate='m:1')
    df_panel = df_panel.merge(rst_first_yr.rename(columns={'bvdid':'rst_dir_pr_2','first_yr':'rst_pr_first_yr_2'}), how='left', on='rst_dir_pr_2', validate='m:1')

    # add a column for the new bvdid
    temp_idx = df_panel.rst_up.notna()
    df_panel.loc[temp_idx, 'rst_ct_flag'] = 1
    df_panel.loc[temp_idx, 'bvdid_r1'] = df_panel.loc[temp_idx, 'rst_up']

    # label the subsidiaries after restructuring
    temp_idx = temp_idx & (df_panel.bvdid!=df_panel.rst_up) & (df_panel.closdate_year>=df_panel.rst_pr_first_yr) 
    df_panel.loc[temp_idx, 'rst_ct_flag'] = 0
    df_panel.loc[temp_idx, 'bvdid_r1'] = np.nan

    # handle the second columns
    temp_idx = df_panel.rst_up_2.notna()
    df_panel.loc[temp_idx, 'rst_ct_flag_2'] = 1
    df_panel.loc[temp_idx, 'bvdid_r2'] = df_panel.loc[temp_idx, 'rst_up_2']

    temp_idx = temp_idx & (df_panel.bvdid!=df_panel.rst_up_2) & (df_panel.closdate_year>=df_panel.rst_pr_first_yr_2) 
    df_panel.loc[temp_idx, 'rst_ct_flag_2'] = 0
    df_panel.loc[temp_idx, 'bvdid_r2'] = np.nan

    # label the restructuring year
    temp_idx = (df_panel.closdate_year==df_panel.first_yr) & (df_panel.rst_up.notna())
    df_panel.loc[temp_idx, 'rst_yr_flag'] = 1

    return df_panel


def concatRTO(df_panel, zp):
    '''Concatenate reports involved in the RTO deals'''
    # get rto deals
    rto = zp.deal_dic['rto']
    rto = rto[(rto['Target BvD ID number'].str.contains('GB', na=False)) & (rto['Acquiror BvD ID number'].str.contains('GB', na=False))]
    rto = rto[rto['Target BvD ID number']!=rto['Acquiror BvD ID number']]
    
    # extract essential columns
    temp = rto[['Deal Number', 'Completed date', 'year', 'Target name', 'Target BvD ID number', 'Acquiror name', 'Acquiror BvD ID number']]
    temp.rename(columns={'Target BvD ID number':'tag_bvdid','Acquiror BvD ID number':'acq_bvdid','Target name':'tag_name','Acquiror name':'acq_name'}, inplace=True)
    temp['tag_count'] = temp.groupby('tag_bvdid')['Deal Number'].transform('count')
    temp['acq_count'] = temp.groupby('acq_bvdid')['Deal Number'].transform('count')

    # remove outliers
    tag_set = set(temp.tag_bvdid.unique())
    acq_set = set(temp.acq_bvdid.unique())
    #TODO: there are two rtos to investigate
    temp2 = temp[~((temp.tag_bvdid.isin(tag_set & acq_set)) | (temp.acq_bvdid.isin(tag_set & acq_set)))]

    # get the previous and the next deal year
    temp2['prev_acq_yr'] = temp2.sort_values('year').groupby('acq_bvdid')['year'].shift(periods=1)
    temp2['next_acq_yr'] = temp2.sort_values('year').groupby('acq_bvdid')['year'].shift(periods=-1)
    assert(temp2.tag_count.max()<2)

    # start the loop
    rst_set = set(df_panel[df_panel.rst_up.notna()].bvdid.unique())
    temp2 = temp2.reset_index(drop=True)
    outliers = []
    for index, row in temp2.iterrows():
        if index%10==0:
            print(index)
        deal_yr = row['year']
        prev_deal_yr = row['prev_acq_yr']
        prev_deal_yr = 0 if np.isnan(prev_deal_yr) else prev_deal_yr
        next_deal_yr = row['next_acq_yr']
        next_deal_yr = 3000 if np.isnan(next_deal_yr) else next_deal_yr
        
        # ignore those that already have restructuring
        if (row.tag_bvdid in rst_set) or (row.acq_bvdid in rst_set):
            outliers.append(row)
            continue
            
        # handle target
        bvdid_idx = df_panel.bvdid==row.tag_bvdid
        temp_idx = (df_panel.closdate_year < deal_yr) & bvdid_idx
        df_panel.loc[temp_idx, 'bvdid_r1'] = row.tag_bvdid + 'r'
        df_panel.loc[temp_idx, 'rst_rto_flag'] = 1
        temp_idx = (df_panel.closdate_year >= deal_yr) & bvdid_idx
        # df_panel.loc[temp_idx, 'bvdid_r1'] = row.tag_bvdid
        df_panel.loc[temp_idx, 'rst_rto_flag'] = 0

        # handle acquiror
        bvdid_idx = (df_panel.bvdid==row.acq_bvdid) & (df_panel.closdate_year<next_deal_yr)
        if prev_deal_yr==0:
            temp_idx = (df_panel.closdate_year < deal_yr) & bvdid_idx
            # df_panel.loc[temp_idx, 'bvdid_r1'] = row.acq_bvdid
            df_panel.loc[temp_idx, 'rst_rto_flag'] = 0
        temp_idx = (df_panel.closdate_year >= deal_yr) & bvdid_idx
        df_panel.loc[temp_idx, 'bvdid_r1'] = row.tag_bvdid + 'r'
        df_panel.loc[temp_idx, 'rst_rto_flag'] = 1
        
        # label rto deals
        df_panel.loc[(df_panel.bvdid==row.acq_bvdid) & (df_panel.closdate_year==deal_yr-1), 'next_yr_rto'] = 1

    return df_panel


def unifyID(df):
    '''get a unified bvdid'''
    df['bvdid_r3'] = df['bvdid_r1'].copy()
    temp_idx = (df.rst_pr_first_yr<df.rst_pr_first_yr_2) & (df.bvdid_r1.isna()) & (df.bvdid_r2.notna())
    df.loc[temp_idx, 'bvdid_r3'] = df.loc[temp_idx, 'bvdid_r2']

    temp_idx = (df.rst_pr_first_yr>df.rst_pr_first_yr_2) & (df.bvdid_r2.notna())
    df.loc[temp_idx, 'bvdid_r3'] = df.loc[temp_idx, 'bvdid_r2']

    # here is an example: we can the compare rst_pr_first_yr and rst_pr_first_yr_2 to decide which one to keep
    # df[df.bvdid=='GB01359357']

    # for firms that are not restructured or rto, use the original bvdid
    temp_idx = (df.rst_ct_flag!=1) & (df.rst_ct_flag_2!=1) & (df.rst_rto_flag!=1)
    df.loc[temp_idx, 'bvdid_r3'] = df.loc[temp_idx, 'bvdid']
    df.sort_values(by=['bvdid_r3', 'closdate_year'], inplace=True)
    return df


def mergeZephyrHeader(ctry):
    '''merge zephyr ipo data with the header data'''
    # read cpst data
    secu = pd.read_csv('additional_data/securities_yearly_v2.csv')
    secu = secu[secu['curcdd'].notna()]
    secu = secu[secu['exchg']==194]
    secu_first_yr = secu.groupby('isin')['year'].min()
    secu_last_yr = secu.groupby('isin')['year'].max()
    secu_first_yr.rename('cpst_first_yr', inplace=True)
    secu_last_yr.rename('cpst_last_yr', inplace=True)
    secu_yr = pd.concat([secu_first_yr, secu_last_yr], axis=1)

    # read header and zephyr data
    df_header = getMergedHeader(ctry)
    df_header = df_header[df_header['bvdid'].str[:2]=='GB']

    zp = processZephyr.Zephyr()
    zp.extractDeals()
    zp.removeDuplicates()
    zp.readHandCheckData()    

    # start to merge header with Zephyr
    zp.merge_dic['ipo'] = zp.refineColumns('ipo')
    zp_ipo = zp.merge_dic['ipo'].sort_values('zephyr_ipo_dt').groupby('bvdid').nth(0).reset_index()
    zp_ipo.rename(columns={'year':'zephyr_ipo_yr'}, inplace=True)
    zp_cols = ['bvdid', 'zephyr_ipo_dt', 'zephyr_ipo_yr', 'zephyr_ipo_exch']
    df_header = df_header.merge(zp_ipo[zp_cols], how='left', on=['bvdid'], validate='m:1')

    # fuse together
    temp_idx = (df_header.ipo_date_year.isna()) & (df_header.zephyr_ipo_yr.notna())
    df_header.loc[temp_idx, 'ipo_date_year'] = df_header.loc[temp_idx, 'zephyr_ipo_yr']
    temp_idx = (df_header.ipo_date.isna()) & (df_header.zephyr_ipo_dt.notna())
    df_header.loc[temp_idx, 'ipo_date'] = df_header.loc[temp_idx, 'zephyr_ipo_dt']

    # merge hand check data
    df_header = df_header.merge(zp.hand_check[['bvdid','hc_exch']], how='left', on=['bvdid'], validate='m:1')

    # update exchange from zephyr
    col_name = 'zp_exch_mod'
    exch_col = 'zephyr_ipo_exch'
    df_header.loc[df_header[exch_col].notna(), col_name] = 'otc'
    df_header.loc[df_header[exch_col]=='London Stock Exchange', col_name] = 'lse'
    df_header.loc[df_header[exch_col]=='London AIM Stock Exchange', col_name] = 'aim'
    others_list = ['New York Stock Exchange (NYSE)', 'NASDAQ International', 'Toronto Stock Exchange', 'Australian Stock Exchange', 'Hong Kong Stock Exchange']
    df_header.loc[df_header[exch_col].isin(others_list), col_name] = 'others'

    # update exchange from the header, note that here we don't have aim, so we need to use aim from zp to replace
    col_name = 'bvd_exch_mod'
    exch_col = 'mainexch'
    df_header.loc[(df_header[exch_col].notna()) & (~df_header[exch_col].isin(['Delisted','Unlisted'])), col_name] = 'otc'
    df_header.loc[df_header[exch_col]=='London Stock Exchange', col_name] = 'lse'
    others_list = ['New York Stock Exchange (NYSE)', 'NYSE MKT', 'NASDAQ National Market', 'Toronto Stock Exchange', 'Australian Securities Exchange', 'Hong Kong Stock Exchange']
    df_header.loc[df_header[exch_col].isin(others_list), col_name] = 'others'   

    # update exchange from hand check
    col_name = 'hc_exch_mod'
    exch_col = 'hc_exch'
    df_header.loc[df_header[exch_col].notna(), col_name] = 'otc'
    df_header.loc[df_header[exch_col]=='London Stock Exchange', col_name] = 'lse'
    df_header.loc[df_header[exch_col]=='London AIM Stock Exchange', col_name] = 'aim'
    others_list = ['New York Stock Exchange (NYSE)', 'NYSE', 
                'NASDAQ National Market', 'NASDAQ International', 'NASDAQ', 
                'Toronto Stock Exchange', 'Australian Stock Exchange', 'Australian Securities Exchange', 'Hong Kong Stock Exchange']
    df_header.loc[df_header[exch_col].isin(others_list), col_name] = 'others'

    # fuse exchange information together
    temp_idx = df_header['bvd_exch_mod'].notna()
    df_header.loc[temp_idx, 'exch_final'] = df_header.loc[temp_idx, 'bvd_exch_mod']
    temp_idx = df_header['zp_exch_mod'].notna()
    df_header.loc[temp_idx, 'exch_final'] = df_header.loc[temp_idx, 'zp_exch_mod']
    temp_idx = df_header['hc_exch_mod'].notna()
    df_header.loc[temp_idx, 'exch_final'] = df_header.loc[temp_idx, 'hc_exch_mod']

    # handle outliers
    temp_idx = df_header.bvdid=='GB05867160' # in 2007 this company was still in otc
    df_header.loc[temp_idx, 'exch_final'] = 'aim'
    df_header.loc[temp_idx, 'ipo_date_year'] = 2011
    df_header.loc[temp_idx, 'ipo_date'] = '2011-01-01'

    df_header.loc[df_header.bvdid=='GB05304498', 'exch_final'] = 'aim'
    df_header.loc[df_header.bvdid=='GB05098197', 'exch_final'] = 'otc'

    temp_idx = df_header.bvdid=='GB04350858' # in 2003 this company was still in otc
    df_header.loc[temp_idx, 'exch_final'] = 'aim'
    df_header.loc[temp_idx, 'ipo_date_year'] = 2006
    df_header.loc[temp_idx, 'ipo_date'] = '2006-01-01'

    df_header.loc[df_header.bvdid=='GB04271085', 'exch_final'] = 'aim'

    temp_idx = df_header.bvdid=='GBIM117232C' # this company does not have ipo date so I bing it
    df_header.loc[temp_idx, 'exch_final'] = 'aim'
    df_header.loc[temp_idx, 'ipo_date_year'] = 2008
    df_header.loc[temp_idx, 'ipo_date'] = '2008-07-14'

    # update ipo date for some of the hand-checked data
    temp = pd.read_excel('additional_data/ipo_target_unlist_ready.xlsx', sheet_name='Sheet1')
    temp2 = temp[temp['Comment']=='plc']
    temp3 = temp[temp['Comment']!='plc']

    for index, row in temp2.iterrows():
        pr_id = row['Comment2']
        row_id = row['bvdid']
        df_header.loc[df_header.bvdid==row_id, 'listed'] = 'Unlisted'
        if str(pr_id)!='nan':
            temp_idx = df_header.bvdid==pr_id
            pr_temp = df_header[temp_idx]
            if len(pr_temp)>0:
                pr_temp = pr_temp.iloc[0]
                listed_txt = row['listed']
                listed_txt = 'Listed' if listed_txt in ['listed', 'lised'] else 'Delisted'
                ipo_dt = row['zephyr_ipo_dt']
                if pr_temp['listed'] == 'Unlisted':
                    df_header.loc[temp_idx, 'listed'] = listed_txt
                    df_header.loc[temp_idx, 'ipo_date'] = row['zephyr_ipo_dt']
                    df_header.loc[temp_idx, 'ipo_date_year'] = row['zephyr_ipo_yr']
                    df_header.loc[temp_idx, 'delisted_date'] = row['delisted_date']
                    df_header.loc[temp_idx, 'delisted_date_year'] = row['delisted_date'].year
                else:
                    if str(pr_temp['ipo_date_year'])=='nan':
                        df_header.loc[temp_idx, 'ipo_date'] = row['zephyr_ipo_dt']
                        df_header.loc[temp_idx, 'ipo_date_year'] = row['zephyr_ipo_yr']
                    if str(pr_temp['delisted_date_year'])=='nan':
                        df_header.loc[temp_idx, 'delisted_date'] = row['delisted_date']
                        df_header.loc[temp_idx, 'delisted_date_year'] = row['delisted_date'].year

    for index, row in temp3.iterrows():
        row_id = row['bvdid']
        listed_txt = row['listed']
        if listed_txt in ['listed', 'lised']:
            listed_txt = 'Listed' 
        elif listed_txt in ['delisted', 'delsted']:
            listed_txt = 'Delisted'

        temp_idx = df_header.bvdid==row_id
        header_temp = df_header[temp_idx]
        if len(header_temp)>0:
            header_temp = header_temp.iloc[0]
            header_listed = header_temp['listed']
            if header_listed not in ['Listed', 'Delisted']:
                df_header.loc[temp_idx, 'ipo_date'] = row['zephyr_ipo_dt']
                df_header.loc[temp_idx, 'ipo_date_year'] = row['zephyr_ipo_yr']
                df_header.loc[temp_idx, 'delisted_date'] = row['delisted_date']
                df_header.loc[temp_idx, 'delisted_date_year'] = row['delisted_date'].year

                
    # start to merge cpst          
    # add ticker count
    df_header['sd_ticker_count'] = df_header.groupby('sd_ticker')['bvdid'].transform('count')

    df_header = df_header.merge(secu_yr, how='left', left_on='sd_isin', right_index=True)

    # update exchange
    temp_idx = (df_header['exch_final'].isna()) & (df_header['cpst_first_yr'].notna())
    df_header.loc[temp_idx, 'exch_final'] = 'lse'

    # update nan ipo years
    temp_idx = (df_header['cpst_first_yr'].notna()) & (df_header['ipo_date_year'].isna()) & (df_header['cpst_first_yr']>=df_header['dateinc_year'])
    df_header.loc[temp_idx, 'ipo_date_year'] = df_header.loc[temp_idx, 'cpst_first_yr'] 

    # for those that have cpst ipo years earlier than dateinc, clip at dateinc and handover to later process
    temp_idx = (df_header['cpst_first_yr'].notna()) & (df_header['ipo_date_year'].isna()) & (df_header['cpst_first_yr']<df_header['dateinc_year'])
    df_header.loc[temp_idx, 'ipo_date_year'] = df_header.loc[temp_idx, 'dateinc_year'] 

    # if the bvd exchange is otc/others and it has cpst, then it has a transition from otc/others to lse
    temp_idx = (df_header['cpst_first_yr'].notna()) & (df_header['sd_isin'].notna())
    temp_idx = temp_idx & (df_header['cpst_first_yr']>df_header['ipo_date_year']) & (df_header['ipo_date_year']>=1985) & (df_header['exch_final'].isin(['otc','others']))
    df_header.loc[temp_idx, 'exch_final'] = 'lse'
    df_header.loc[temp_idx, 'ipo_date_year'] = df_header.loc[temp_idx, 'cpst_first_yr']

    # The conclusion is to use bvd's delisted year if there are conflicts. So we only use cpst to fill nans.
    de_idx = (df_header['delisted_date_year'].isna()) & (df_header['cpst_last_yr'].notna())
    df_header.loc[de_idx, 'delisted_date_year'] = df_header.loc[de_idx, 'cpst_last_yr']

    # start to handle rstr
    temp_idx = (df_header['cpst_first_yr']<df_header['ipo_date_year'])
    rstr_isin_idx = temp_idx & (df_header['sd_ticker_count']>1) & (df_header['cpst_first_yr']<df_header['dateinc_year'])
    rstr_isin_set = list(df_header.loc[rstr_isin_idx, 'sd_isin'].unique())
    rstr_tikcer_set = list(df_header.loc[rstr_isin_idx, 'sd_ticker'].unique())

    header_cols = ['bvdid', 'name_native', 'sd_isin', 'sd_ticker', 'exch_final', 'dateinc_year', 'ipo_date_year', 'delisted_date_year', 'cpst_first_yr', 'cpst_last_yr', 'listed', 'sd_ticker_count']

    if np.nan in rstr_tikcer_set:
        print('nan')
        rstr_tikcer_set.remove(np.nan)
    cand = df_header[(df_header['sd_ticker'].isin(rstr_tikcer_set))]


    # update ipo year and delisted year with CPST
    succeed_list = []
    for isin in rstr_isin_set:
    # for isin in ['GB00B082RF11']:
        isin_idx = cand['sd_isin']==isin
        row = cand.loc[isin_idx]
        ticker = row['sd_ticker'].iloc[0]
        ipo_yr = row['ipo_date_year'].iloc[0]
        cpst_first_yr = row['cpst_first_yr'].iloc[0]
        cpst_last_yr = row['cpst_last_yr'].iloc[0]
        bvdid = row['bvdid'].iloc[0]
        ticker_count = row['sd_ticker_count'].iloc[0]
        listed = row['listed'].iloc[0]
        bvdid_list = []
        temp_bvdid = bvdid
        for c in range(int(ticker_count)-1):
            temp_idx = (cand['sd_ticker']==ticker) & (cand['delisted_date_year']==ipo_yr)
            if temp_idx.sum()==0:
                break
            succeed_list.append(bvdid)
            cand.loc[temp_idx, 'rst_pr'] = temp_bvdid
            cand.loc[temp_idx, 'rst_yr'] = cand.loc[temp_idx, 'delisted_date_year'].iloc[0]
            temp_bvdid = cand.loc[temp_idx, 'bvdid'].iloc[0]
            cand.loc[isin_idx, 'rst_pr'] = bvdid
            cand.loc[temp_idx, 'cpst_first_yr'] = cpst_first_yr
            cand.loc[temp_idx, 'cpst_last_yr'] = cpst_last_yr
            cand.loc[temp_idx, 'exch_final'] = 'lse'
            ipo_yr = cand.loc[temp_idx, 'ipo_date_year'].iloc[0]
            if np.isnan(ipo_yr):
                ipo_yr = cand.loc[temp_idx, 'dateinc_year'].iloc[0]
            bvdid_list.append(temp_bvdid)
        
        # correct ipo year and delist year for those rstr firms
        if len(bvdid_list)!=0:
            temp_idx = cand['bvdid'].isin(bvdid_list)
            cand.loc[temp_idx, 'ipo_date_year'] = cpst_first_yr
            cand.loc[temp_idx, 'delisted_date_year'] = cpst_last_yr
            cand.loc[temp_idx, 'listed'] = listed

        
    # constrcut rstr df for rstr purpose
    temp_idx = cand['rst_pr'].notna()
    rstr_df = cand.loc[temp_idx, header_cols+['rst_pr', 'rst_yr']].copy()

    rstr_hc_set = ['GB01190025', 'GB00096356', 'GB00324504', 'GB01190025', 'GB00054802', 'GB00019739', 
                'GB00097878', 'GB00233112', 'GB00969618', 'GB00220499', 'GB02366970', 'GB00058025',
                'GB00403968', 'GB00256111', 'GB00621482', 'GB02367004', 'GB00029423', 'GB01800000',
                'GB01971312', 'GB00214436', 'GB02902525', 'GB00027657', 'GB02662226', 'GB00238525',
                'GB00251977', 'GB06694512', 'GB02235761', 'GBSC042391', 'GBSC192666']

    rstr_hc_df = df_header.loc[df_header['bvdid'].isin(rstr_hc_set), header_cols].copy()
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB01190025', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB02688411', 1992, 1987, 2000, 'Delisted')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB00096356', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB02695034', 1992, 1985, 2001, 'Delisted')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB00324504', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB03140769', 1996, 1985, 2023, 'Listed')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB01190025', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB03142685', 1996, 1994, 2016, 'Delisted')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB00054802', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB03207774', 1996, 1985, 1998, 'Delisted')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB00019739', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB03215874', 1996, 1985, 2006, 'Delisted')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB00097878', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB03299608', 1997, 1985, 2023, 'Listed')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB00233112', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB03407696', 1997, 1985, 2023, 'Listed')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB00969618', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB03539413', 1998, 1987, 2023, 'Listed')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB00220499', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB03548826', 1998, 1987, 2003, 'Delisted')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB02366970', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB03586615', 1998, 1992, 2002, 'Delisted')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB00689729', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB03771147', 1999, 1985, 2005, 'Delisted')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB00058025', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB03834125', 1999, 1985, 2023, 'Listed')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB00403968', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB03870488', 1999, 1985, 1998, 'Delisted')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB00256111', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB03899848', 1999, 1985, 2023, 'Listed')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB00621482', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB03912506', 2000, 1985, 2011, 'Delisted')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB02367004', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB04031152', 2000, 1995, 2023, 'Listed')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB00029423', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB04120344', 2000, 1987, 2023, 'Listed')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB01800000', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB04190816', 2001, 1985, 2023, 'Listed')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB01971312', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB04191122', 2001, 1999, 2004, 'Delisted')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB00214436', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB04256886', 2001, 1985, 2023, 'Listed')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB02902525', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB04316684', 2001, 1994, 2017, 'Delisted')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB00027657', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB04452715', 2002, 1985, 2007, 'Delisted')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB02662226', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB04789566', 2003, 1991, 2006, 'Delisted')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB00238525', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB07145051', 2010, 1985, 2016, 'Delisted')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB00251977', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB08217766', 2010, 1951, 2023, 'Listed')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB06694512', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GB10013770', 2016, 1998, 2018, 'Delisted')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GB02235761', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GBSC157176', 1995, 1993, 2023, 'Listed')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GBSC042391', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GBSC203873', 2000, 1985, 2023, 'Listed')
    rstr_hc_df.loc[rstr_hc_df['bvdid']=='GBSC192666', ['rst_pr', 'rst_yr', 'ipo_date_year', 'delisted_date_year', 'listed']] = ('GBSC226738', 2002, 1999, 2008, 'Delisted')

    rstr_df = pd.concat([rstr_df, rstr_hc_df], axis=0)

    rstr_df.to_csv('additional_data/cpst_rstr.csv', index=False)

    # concat back to df_header
    cand.drop(columns=['rst_pr', 'rst_yr'], inplace=True)
    temp_idx = (df_header['sd_ticker'].isin(rstr_tikcer_set)) | (df_header['bvdid'].isin(rstr_hc_set))
    df_header = df_header[~temp_idx]
    df_header = pd.concat([df_header, cand, rstr_hc_df], axis=0)

    # merge with sdc
    sdc_ipo = pd.read_csv('additional_data/uk_ipo.csv')
    sdc_ipo.rename(columns={'Exchange\nWhere Issue\nWill Be Listed':'sdc_exch', 'Ticker\nSymbol':'sd_ticker', 'ISIN':'sd_isin'},inplace=True)
    sdc_ipo.loc[sdc_ipo['sdc_exch'].isin(['NASDQ', 'NYSE', 'AUSLA', 'TORON']), 'sdc_exch'] = 'others'
    sdc_ipo.loc[sdc_ipo['sdc_exch']=='AIM', 'sdc_exch'] = 'aim'
    sdc_ipo.loc[sdc_ipo['sdc_exch']=='LONDN', 'sdc_exch'] = 'lse'
    sdc_ipo.loc[(~sdc_ipo['sdc_exch'].isin(['lse', 'aim', 'others'])) & (sdc_ipo['sdc_exch'].notna()), 'sdc_exch'] = 'otc'
    sdc_ipo['sdc_ipo_yr'] = pd.to_datetime(sdc_ipo['Issue\nDate']).dt.year
    sdc_ipo.loc[sdc_ipo['IPO\nFlag\n(Y/N)']!='Yes', 'sdc_ipo_yr'] = np.nan
    # merge by isin
    sdc_mg = sdc_ipo[sdc_ipo['sd_isin'].notna()]
    sdc_mg = sdc_mg.sort_values('sdc_ipo_yr')
    sdc_mg = sdc_mg.groupby('sd_isin').nth(0).reset_index()
    df_header = df_header.merge(sdc_mg[['sd_isin', 'sdc_exch', 'sdc_ipo_yr']], how='left', on='sd_isin', validate='m:1')
    # merge by ticker
    sdc_mg = sdc_ipo[sdc_ipo['sd_ticker'].notna()]
    sdc_mg = sdc_mg.sort_values('sdc_ipo_yr')
    sdc_mg = sdc_mg.groupby('sd_ticker').nth(0).reset_index()
    sdc_mg.rename(columns={'sdc_exch':'sdc_exch_2', 'sdc_ipo_yr':'sdc_ipo_yr_2'}, inplace=True)
    df_header = df_header.merge(sdc_mg[['sd_ticker', 'sdc_exch_2', 'sdc_ipo_yr_2']], how='left', on='sd_ticker', validate='m:1') 
    # combine info
    df_header['sdc_exch'] = df_header['sdc_exch'].fillna(df_header['sdc_exch_2'])
    df_header['sdc_ipo_yr'] = df_header['sdc_ipo_yr'].fillna(df_header['sdc_ipo_yr_2'])
    df_header.drop(columns=['sdc_exch_2', 'sdc_ipo_yr_2'], inplace=True)
    # update exchange
    temp_idx = (~df_header['exch_final'].isin(['lse','aim'])) & (df_header['sdc_exch'].isin(['lse','aim']))
    temp_idx = temp_idx | (df_header['exch_final'].isna())
    df_header.loc[temp_idx, 'exch_final'] = df_header.loc[temp_idx, 'sdc_exch']
    df_header.loc[df_header['sdc_exch']=='aim', 'exch_final'] = 'aim'
    # update ipo date year
    temp_idx = df_header['ipo_date_year'].isna()
    df_header.loc[temp_idx, 'ipo_date_year'] = df_header.loc[temp_idx, 'sdc_ipo_yr']


    # update listed status
    df_header.loc[df_header['delisted_date_year'] >= 2020, 'listed'] = 'Listed'
    df_header.loc[df_header['delisted_date_year'] < 2020, 'listed'] = 'Delisted'

    temp_idx = (df_header['ipo_date_year'].notna()) & (df_header['listed']=='Unlisted')
    df_header.loc[temp_idx, 'listed'] = 'Listed'

    # set ipo dates of firms with delisted date but no ipo date to be 1997
    temp_idx = (df_header.ipo_date_year.isna()) & (df_header.delisted_date_year.notna())
    df_header.loc[temp_idx, 'ipo_date_year'] = 1997 
    df_header.loc[temp_idx, 'ipo_date'] = "1997-01-01" 

    # set all otc firms to be unlisted 
    df_header.loc[df_header['exch_final']=='otc', 'listed'] = 'Unlisted' 
    # save data
    df_header.to_csv(des_hist_fd + ctry + '/' + ctry + '_header_zp_cpst.csv', index=False)


def updatePanelListedStatus(ctry, age_test=''):
    '''Update the listed status of the firm in the panel data'''
    # read new merged header
    df_header = pd.read_csv(des_hist_fd + ctry + '/' + ctry + '_header_zp_cpst.csv')
    header_cols = ['bvdid', 'listed', 'ipo_date', 'ipo_date_year', 'delisted_date', 'delisted_date_year', 'exch_final', 'dateinc_year']
    df_header = df_header[header_cols]
    print("finish reading header")

    # read panel data
    df = pd.read_csv(f'{fin_hist_fd}{ctry}/{ctry}_filter1_merge_cons_yr_panel_ind_link_zp_rst{age_test}.csv')
    df = df[df.bvdid.str[:2]=='GB'] #TODO: this line should be done earlier in the pipeline
    print("finish reading panel data")

    # drop columns related to listed status
    df.drop(columns=['listed', 'ipo_date', 'ipo_date_year', 'delisted_date', 'delisted_date_year'], inplace=True)
    
    # merge with header again
    df = df.merge(df_header[['bvdid', 'listed', 'ipo_date', 'ipo_date_year', 'delisted_date', 'delisted_date_year', 'exch_final']], how='left', on='bvdid', validate='m:1')
    # drop columns related to listed status for ultimate parents
    df.drop(columns=['listed_up', 'ipo_date_year_up', 'delisted_date_year_up', 'dateinc_year_up'], inplace=True)
    # merge with header again
    df = df.merge(df_header[['bvdid', 'listed', 'ipo_date_year', 'delisted_date_year', 'dateinc_year']], how='left', 
                        left_on='up_bvdid', right_on='bvdid', suffixes=[None, '_up'], validate='m:1')
    print("finish merging with header")

    # cast types
    temp_idx = df.ipo_date_year.notna()
    df.loc[temp_idx, 'ipo_date_year'] = df.loc[temp_idx, 'ipo_date_year'].astype(int)
    temp_idx = df.closdate_year.notna()
    df.loc[temp_idx, 'closdate_year'] = df.loc[temp_idx, 'closdate_year'].astype(float)
    print("finish casting types")

    # update listed status
    df = createListedStatus(df, 'listed_status', 'listed', 'ipo_date_year', 'delisted_date_year')
    df = createListedStatus(df, 'listed_status_up', 'listed_up', 'ipo_date_year_up', 'delisted_date_year_up')
    print("finish updating listed status")
    df.to_csv(f'{fin_hist_fd}{ctry}/{ctry}_filter1_merge_cons_yr_panel_ind_link_zp_rst_listed{age_test}.csv', index=False)
    print("finish saving data")
                                                                                                                                            

def labelPanel(ctry):
    '''label states of the firm in the panel data'''
    df = pd.read_csv(fin_hist_fd +  ctry + '/' + ctry + '_filter1_merge_cons_yr_panel_ind_link_zp_rst_listed.csv')

    # independent flag
    temp_idx = (df['up_bvdid'].isna()) | (df['bvdid']==df['up_bvdid'])
    df.loc[temp_idx, 'idpt_flag'] = 1

    # pv-backed private idpt or up (C)
    temp_idx = (df['idpt_flag']==1) & (df['listed_status']!=1) & (df['pv_fd_backed']==1) & (df['ent_final']=='C')
    df.loc[temp_idx, 'pei_flag'] = 1

    # pv-backed private sub (C)
    temp_idx = (df['idpt_flag']!=1) & (df['listed_status']!=1) & (df['pv_fd_backed']==1) & (df['ent_final']=='C')
    df.loc[temp_idx, 'pes_flag'] = 1    

    # pv-backed private (not C)
    temp_idx = (df['listed_status']!=1) & (df['pv_fd_backed']==1) & (df['ent_final']!='C')
    df.loc[temp_idx, 'pef_flag'] = 1

    # not pv-backed private idpt or up (C)
    temp_idx = (df['idpt_flag']==1) & (df['listed_status']!=1) & (df['pv_fd_backed']!=1) & (df['ent_final']=='C')
    df.loc[temp_idx, 'pc_flag'] = 1

    # public sub (C)
    df['ls_flag'] = np.nan
    temp_idx = (df['idpt_flag']!=1) & (df['listed_status_up']==1) & (df['ent_final']=='C')
    df.loc[temp_idx, 'ls_flag'] = 1

    # private sub (C)
    df['ds_flag'] = np.nan
    temp_idx = (df['idpt_flag']!=1) & (df['listed_status_up']!=1) & (df['ent_final']=='C')
    df.loc[temp_idx, 'ds_flag'] = 1    

    df.to_csv(fin_hist_fd +  ctry + '/' + ctry + '_filter1_merge_cons_yr_panel_ind_link_zp_rst_listed_label.csv', index=False)


def filterPanel(ctry, filter_choice=None, age_test=''):
    '''filter panel data'''
    df = pd.read_csv(f'{fin_hist_fd}{ctry}/{ctry}_filter1_merge_cons_yr_panel_ind_link_zp_rst_listed{age_test}.csv')

    # process uksic as it will be read as float
    idx = df['uksic'].notna()
    df.loc[idx, 'uksic'] = df.loc[idx, 'uksic'].astype(int).astype(str)
    temp_idx = df['uksic'].str.len()<=4
    df.loc[temp_idx, 'uksic'] = '0' + df.loc[temp_idx, 'uksic']
    d4_dict = {'01812':'18120', '02041':'20410', '03320':'33200', 
            '04120':'41200', '04511':'45110', '04671':'46710', '05120':'65120', '06499':'64302', '07420':'74200'}
    df['uksic'] = df['uksic'].replace(d4_dict)

    # get the first two characters of the uksic code
    temp_idx = df['uksic'].notna()
    df.loc[temp_idx, 'uksic_2d'] = df.loc[temp_idx, 'uksic'].astype(str).str[:2]
    # correct the type for uksic_up, here we don't need to do the full type process again because there are only two digits
    temp_idx = df['uksic_up'].notna()
    df.loc[temp_idx, 'uksic_up'] = df.loc[temp_idx, 'uksic_up'].astype(int).astype(str)

    # extract firms that their ultimate parents' industries are in ['64', '70', '82'] and their industries are not in ['64', '70', '82']
    temp = df.loc[(df['uksic_up'].isin(['64', '70', '82'])) & (~df['uksic_2d'].isin(['64', '70', '82'])) & (df['uksic_2d'].notna())].copy()
    # pick those bvdid with the highest toas within the same uksic_up
    temp_idx = temp.groupby(['up_bvdid', 'closdate_year'])['toas'].transform(max) == temp['toas']
    temp = temp[temp_idx]
    # if there are more dupicates, with those with the largest closdate_year
    temp = temp.sort_values(['up_bvdid', 'closdate_year'])
    temp = temp.groupby('up_bvdid').nth(-1).reset_index()
    # rename columns
    temp = temp[['up_bvdid', 'uksic', 'uksic_up', 'bvdid']].rename(columns={'up_bvdid':'bvdid', 'uksic':'uksic_sub', 'bvdid':'bvdid_sub'})

    # merge the industry info back to the panel data
    df = df.merge(temp[['bvdid', 'uksic_sub', 'bvdid_sub']], how='left', on='bvdid', validate='m:1')
    # replace industries in ['64', '70', '82'] with the uksic_sub
    temp_idx = (df['uksic_2d'].isin(['64', '70', '82'])) & (df['uksic_sub'].notna())
    df.loc[temp_idx, 'uksic'] = df.loc[temp_idx, 'uksic_sub']

    # independent flag
    temp_idx = (df['up_bvdid'].isna()) | (df['bvdid']==df['up_bvdid'])
    df.loc[temp_idx, 'idpt_flag'] = 1

    # adjust float variables based on the number of months in the financial report
    fl_cols = ['opre']
    temp_idx = df['nr_months']!=12
    df.loc[temp_idx, fl_cols] = df.loc[temp_idx, fl_cols].mul(12 / df.loc[temp_idx, 'nr_months'], axis=0)

    # set opre_flag to indicate if the firm has opre or turnover >= 6.5m pound or toas >= 3.26m pound
    if filter_choice=='select_sample':
        # find companies that have at least two non-nan opre values
        temp = df[df.opre.notna()].groupby('bvdid_r3')['opre'].count().reset_index()
        temp.rename(columns={'opre':'opre_count'}, inplace=True)
        opre_set = set(temp[temp.opre_count>=2].bvdid_r3.dropna().unique())
    else:
        df.loc[:, 'opre_flag'] = np.nan
        opre_idx = ((df.turn/df.exchrate) >= 6500000) | ((df.opre/df.exchrate) >= 6500000)
        toas_idx = (((df.cuas/df.exchrate) + (df.fias/df.exchrate)) >= 3260000)
        opre_set = set(df[((df.opre.notna()) | (df.turn.notna())) & (opre_idx | toas_idx)].bvdid_r3.dropna().unique())
    df.loc[df.bvdid_r3.isin(opre_set), 'opre_flag'] = 1

    # get the set of firms that were idpt or up for at least one year
    idpt_set = set(df[df['idpt_flag'] == 1].bvdid_r3.dropna().unique())

    # get the set of firms that were industrial for at least one year
    c_set = set(df[df['ent_final'] == 'C'].bvdid_r3.dropna().unique())

    # filter the panel data
    if filter_choice=='before_opre':
        df = df[df.bvdid_r3.isin(idpt_set & c_set)]
    else:
        df = df[df.bvdid_r3.isin(idpt_set & opre_set & c_set)]

    # adjust float variables based on the number of months in the financial report
    fl_cols = ['turn', 'cost', 'gros', 'oope', 'fire', 'fiex', 'fipl', 'taxa', 'plat', 'exre', 'exex', 'extr', 'expt', 'mate', 'staf', 'depr', 'inte', 'rd', 'cf', 'av', 'ebta', 'pl', 'oppl', 'plbt']
    temp_idx = df['nr_months']!=12
    df.loc[temp_idx, fl_cols] = df.loc[temp_idx, fl_cols].mul(12 / df.loc[temp_idx, 'nr_months'], axis=0)

    # sort to make sure the data is sorted by bvdid_r3 and closdate_year
    df.sort_values(['bvdid_r3', 'closdate_year'], inplace=True)

    if filter_choice=='before_opre':
        df.to_csv(f'{fin_hist_fd}{ctry}/{ctry}_before_opre_filter_temp{age_test}.csv', index=False)
    elif filter_choice=='select_sample':
        df.to_csv(f'{fin_hist_fd}{ctry}/{ctry}_select_sample_temp{age_test}.csv', index=False)
    else:
        df.to_csv(f'{fin_hist_fd}{ctry}/{ctry}_filter1_merge_cons_yr_panel_ind_link_zp_rst_listed_label_filter{age_test}.csv', index=False)


def nextState(ctry, filter_choice='None', age_test=''):
    '''get current and next state of the firm'''
    # read panel data
    if filter_choice=='first_filter':
        df = pd.read_csv(f'{fin_hist_fd}{ctry}/{ctry}_filter1_merge_cons_yr_panel_ind_link_zp_rst_listed{age_test}.csv')
        # independent flag
        temp_idx = (df['up_bvdid'].isna()) | (df['bvdid']==df['up_bvdid'])
        df.loc[temp_idx, 'idpt_flag'] = 1
        # get rid of nan in bvdid_r3
        df = df[df['bvdid_r3'].notna()]
        # sort to make sure the data is sorted by bvdid_r3 and closdate_year
        df.sort_values(['bvdid_r3', 'closdate_year'], inplace=True)
    elif filter_choice=='before_opre':
        df = pd.read_csv(f'{fin_hist_fd}{ctry}/{ctry}_before_opre_filter_temp{age_test}.csv')
    elif filter_choice=='select_sample':
        df = pd.read_csv(f'{fin_hist_fd}{ctry}/{ctry}_select_sample_temp{age_test}.csv')
    else:
        df = pd.read_csv(f'{fin_hist_fd}{ctry}/{ctry}_filter1_merge_cons_yr_panel_ind_link_zp_rst_listed_label_filter{age_test}.csv')

    # refine name_native
    temp_idx = df['name_native'].notna()
    df.loc[temp_idx, 'name_mod'] = df.loc[temp_idx, 'name_native'].str.lower()
    df.loc[temp_idx, 'name_mod'] = df.loc[temp_idx, 'name_mod'].apply(remove_brackets)
    df.loc[temp_idx, 'name_mod'] = df.loc[temp_idx, 'name_mod'].str.replace('.', '', regex=False)
    df.loc[temp_idx, 'name_mod'] = df.loc[temp_idx, 'name_mod'].str.replace(',', '', regex=False)
    df.loc[temp_idx, 'name_mod'] = df.loc[temp_idx, 'name_mod'].str.normalize('NFKD').str.encode('ascii', 'ignore').str.decode('utf-8')
    df.loc[temp_idx, 'name_mod'] = df.loc[temp_idx, 'name_mod'].str.strip().replace(r'\s+',' ', regex=True)
    df.loc[temp_idx, 'name_mod'] = df.loc[temp_idx, 'name_mod'].str.replace('limited', 'ltd', regex=False)

    # merge sdc pe backed investments as deals
    sdc_pe = pd.read_csv('additional_data/uk_pe_backed.csv')
    sdc_pe['inv_first_yr'] = pd.to_datetime(sdc_pe['Date Company\nReceived First\nInvestment']).dt.year
    sdc_pe['inv_last_yr'] = pd.to_datetime(sdc_pe['Date Company\nReceived Last\nInvestment']).dt.year
    sdc_pe['sdc_first_yr'] = sdc_pe.groupby('Company Name')['inv_first_yr'].transform(min)
    sdc_pe['sdc_last_yr'] = sdc_pe.groupby('Company Name')['inv_last_yr'].transform(max)
    sdc_pe = sdc_pe.groupby('Company Name').nth(0).reset_index()
    sdc_pe.rename(columns={'Company\nTicker':'sd_ticker'}, inplace=True)
    sdc_pe['name_mod'] = sdc_pe['Company Name']
    # refine company name for matching
    temp_idx = sdc_pe['name_mod'].notna()
    sdc_pe.loc[temp_idx, 'name_mod'] = sdc_pe.loc[temp_idx, 'name_mod'].str.lower()
    sdc_pe.loc[temp_idx, 'name_mod'] = sdc_pe.loc[temp_idx, 'name_mod'].apply(remove_brackets)
    sdc_pe.loc[temp_idx, 'name_mod'] = sdc_pe.loc[temp_idx, 'name_mod'].str.replace('.', '', regex=False)
    sdc_pe.loc[temp_idx, 'name_mod'] = sdc_pe.loc[temp_idx, 'name_mod'].str.replace(',', '', regex=False)
    sdc_pe.loc[temp_idx, 'name_mod'] = sdc_pe.loc[temp_idx, 'name_mod'].str.normalize('NFKD').str.encode('ascii', 'ignore').str.decode('utf-8')
    sdc_pe.loc[temp_idx, 'name_mod'] = sdc_pe.loc[temp_idx, 'name_mod'].str.strip().replace(r'\s+', ' ', regex=True)
    sdc_pe.loc[temp_idx, 'name_mod'] = sdc_pe.loc[temp_idx, 'name_mod'].str.replace('limited', 'ltd', regex=False)
    # merge by ticker
    sdc_pe_mg = sdc_pe[sdc_pe['sd_ticker'].notna()]
    sdc_pe_mg['ticker_count'] = sdc_pe_mg.groupby('sd_ticker')['Fund\nYear'].transform('count')
    sdc_pe_mg = sdc_pe_mg[sdc_pe_mg['ticker_count']==1]
    df = df.merge(sdc_pe_mg[['sd_ticker', 'sdc_first_yr', 'sdc_last_yr']], how='left', on='sd_ticker', validate='m:1')
    # merge company name
    sdc_pe_mg = sdc_pe[sdc_pe['name_mod'].notna()]
    sdc_pe_mg = sdc_pe_mg.groupby('name_mod').nth(-1).reset_index()
    sdc_pe_mg.rename(columns={'sdc_first_yr':'sdc_first_yr_2', 'sdc_last_yr':'sdc_last_yr_2'}, inplace=True)
    df = df.merge(sdc_pe_mg[['name_mod', 'sdc_first_yr_2', 'sdc_last_yr_2']], how='left', on='name_mod', validate='m:1')
    # combine info
    df['sdc_first_yr'] = df['sdc_first_yr'].fillna(df['sdc_first_yr_2'])
    df['sdc_last_yr'] = df['sdc_last_yr'].fillna(df['sdc_last_yr_2'])
    # update sdc_flag
    df['sdc_first_yr'] = df['sdc_first_yr'].astype(float)
    df['sdc_last_yr'] = df['sdc_last_yr'].astype(float)
    temp_idx = (df['sdc_first_yr'] <= df['closdate_year']) & (df['closdate_year'] <= df['sdc_last_yr'])
    df.loc[temp_idx, 'sdc_flag'] = 1

    # merge preqin
    preqin = pd.read_csv('additional_data/preqin_pe_uk.csv')
    preqin = preqin[preqin['Deal_Status']=='Completed']
    preqin['Deal_Date'] = pd.to_datetime(preqin['Deal_Date'])
    preqin['year'] = preqin['Deal_Date'].dt.year
    preqin['preqin_first_yr'] = preqin.groupby('Portfolio_Company_Name')['year'].transform(min)
    preqin['preqin_last_yr'] = preqin.groupby('Portfolio_Company_Name')['year'].transform(max)
    # refine company name for matching
    temp_idx = preqin['Portfolio_Company_Name'].notna()
    preqin.loc[temp_idx, 'name_mod'] = preqin.loc[temp_idx, 'Portfolio_Company_Name'].str.lower()
    preqin.loc[temp_idx, 'name_mod'] = preqin.loc[temp_idx, 'name_mod'].apply(remove_brackets)
    preqin.loc[temp_idx, 'name_mod'] = preqin.loc[temp_idx, 'name_mod'].str.replace('.', '', regex=False)
    preqin.loc[temp_idx, 'name_mod'] = preqin.loc[temp_idx, 'name_mod'].str.replace(',', '', regex=False)
    preqin.loc[temp_idx, 'name_mod'] = preqin.loc[temp_idx, 'name_mod'].str.normalize('NFKD').str.encode('ascii', 'ignore').str.decode('utf-8')
    preqin.loc[temp_idx, 'name_mod'] = preqin.loc[temp_idx, 'name_mod'].str.strip().replace(r'\s+', ' ', regex=True)
    preqin.loc[temp_idx, 'name_mod'] = preqin.loc[temp_idx, 'name_mod'].str.replace('limited', 'ltd', regex=False)
    preqin = preqin.groupby('name_mod').nth(0).reset_index()
    # merge by name
    df = df.merge(preqin[['name_mod', 'preqin_first_yr', 'preqin_last_yr']], how='left', on='name_mod', validate='m:1') 
    # update sdc_flag
    temp_idx = (df['preqin_first_yr'] <= df['closdate_year']) & (df['closdate_year'] <= df['preqin_last_yr'])
    df.loc[temp_idx, 'sdc_flag'] = 1

    # merge sdc deals
    sdc = pd.read_csv('additional_data/uk_pe_deals_sdc.csv')
    sdc['date'] = pd.to_datetime(sdc['  Date\nEffective'])
    sdc['closdate_year'] = sdc['date'].dt.year
    sdc.rename(columns={'Target\nPrimary\nTicker\nSymbol':'sd_ticker'}, inplace=True)
    sdc['sd_ticker'] = sdc['sd_ticker'].str.replace('\n', '')
    sdc['sd_ticker'] = sdc['sd_ticker'].str.replace('/', '')
    sdc['sdc_deal_flag'] = 1
    sdc_mg = sdc.groupby(['sd_ticker', 'closdate_year'])['sdc_deal_flag'].nth(0).reset_index()
    df = df.merge(sdc_mg[['sd_ticker', 'closdate_year', 'sdc_deal_flag']], how='left', on=['sd_ticker', 'closdate_year'], validate='m:1')
    df['sdc_flag'] = df['sdc_flag'].fillna(df['sdc_deal_flag'])

    # merge sdc ipo
    ipo_pe = pd.read_csv('additional_data/pe_ipo.csv')
    ipo_pe['date'] = pd.to_datetime(ipo_pe['Issue\nDate'])
    ipo_pe['closdate_year'] = ipo_pe['date'].dt.year
    ipo_pe.rename(columns={'Ticker\nSymbol':'sd_ticker'}, inplace=True)
    ipo_pe = ipo_pe.groupby(['sd_ticker', 'closdate_year']).nth(0).reset_index()
    ipo_pe['sdc_ipo_flag'] = 1 
    df = df.merge(ipo_pe[['sd_ticker', 'closdate_year', 'sdc_ipo_flag']], how='left', on=['sd_ticker', 'closdate_year'], validate='m:1')
    df['sdc_flag'] = df['sdc_flag'].fillna(df['sdc_ipo_flag'])

    # prepare zephyr pv-backed deals
    zp = processZephyr.Zephyr()
    zp.extractDeals()
    zp.removeDuplicates()
    zp.readHandCheckData()
    # get all pe or venture backed deals
    temp_idx = zp.df_deal['Deal financing'].str.lower().str.contains('private equity', na=False)
    temp_idx = temp_idx | (zp.df_deal['Deal financing'].str.lower().str.contains('venture', na=False))
    # temp_idx = temp_idx & (zp.df_deal['Deal sub-type'].str.lower().str.contains('public take', na=False))
    pe_deals = zp.df_deal[temp_idx]
    pe_deals['Completed date'] = pd.to_datetime(pe_deals['Completed date'])
    pe_deals['year'] = pe_deals['Completed date'].dt.year
    pe_deals = pe_deals[pe_deals['Target BvD ID number'].notna()]
    pe_deals = pe_deals[['Target BvD ID number', 'year']].copy()
    pe_deals.rename(columns={'Target BvD ID number':'bvdid', 'year':'closdate_year'}, inplace=True)
    pe_deals['pe_deal_flag'] = 1
    pe_deals = pe_deals.groupby(['bvdid', 'closdate_year']).nth(0).reset_index()
    # merge pe deals
    df = df.merge(pe_deals, how='left', on=['bvdid', 'closdate_year'], validate='m:1')

    # aggregate deal-level pe flags
    temp_idx = (df['zephyr_ibo'].notna()) | (df['zephyr_mbo'].notna()) | (df['zephyr_mbi'].notna()) | (df['pe_deal_flag'].notna()) | (df['sdc_flag'].notna())
    df.loc[temp_idx, 'zephyr_pv_flag'] = 1
    # get the up of last year
    df['prev_up'] = df.groupby('bvdid_r3')['up_bvdid'].shift(1)
    # propagate the deal-level pv-backed status up to the year of ipo or changing up
    temp_idx = (df['prev_up']!=df['up_bvdid']) & ((df['prev_up'].notna()) | (df['up_bvdid'].notna()))
    temp_idx = temp_idx | (df['ipo_date_year']==df['closdate_year'])
    temp_idx = temp_idx & (df['zephyr_pv_flag']!=1)
    df.loc[temp_idx, 'zephyr_pv_flag'] = 2
    df['zephyr_pv_flag'] = df.groupby('bvdid_r3')['zephyr_pv_flag'].transform(lambda v: v.ffill())
    # update pv-backed status
    temp_idx = df['zephyr_pv_flag']==1
    # df.loc[temp_idx, 'pv_backed'] = 1
    df.loc[temp_idx, 'pv_fd_backed'] = 1
    # df.loc[temp_idx, 'pv_ori_backed'] = 1

    ### start to assgin current state
    # assign status for firms that have listed guo50
    guo_mg = pd.read_csv(own_hist_fd + ctry + '/fg_uo_' + ctry + '_pair_all_guo_listed.csv')
    guo_mg = guo_mg.groupby('Subsidiary BvD ID')[[str(i) for i in range(1999,2021)]].sum().reset_index()
    guo_mg.rename(columns={'Subsidiary BvD ID':'bvdid'}, inplace=True)
    for yr in range(1999,2021):
        guo_li_set = set(guo_mg.loc[guo_mg[str(yr)]>=1.0, 'bvdid'].unique())
        df.loc[(df['bvdid'].isin(guo_li_set)) & (df['closdate_year']==yr), 'listed_status_guo'] = 1

    # handle the time-lag effects of idpt_flag and pv_fd_backed
    temp_idx = (df['up_bvdid'].notna()) & (df['bvdid']!=df['up_bvdid'])
    df.loc[temp_idx, 'dpt_flag'] = 1
    # temp_idx = (df['dpt_flag']==1) & (df.groupby('bvdid_r3')['dpt_flag'].shift(-1)!=1) & (~(df['closdate_year']>=2018))
    # df.loc[temp_idx, 'dpt_flag'] = np.nan        
    # temp_idx = (df['pv_fd_backed']==1) & (df.groupby('bvdid_r3')['pv_fd_backed'].shift(-1)!=1) & (~(df['closdate_year']>=2018))
    # df.loc[temp_idx, 'pv_fd_backed'] = np.nan     

    # assign the pv-backed status to the up
    up_pv_back = df[df['pv_fd_backed']==1].groupby(['bvdid', 'closdate_year'])['pv_fd_backed'].nth(-1).reset_index()
    up_pv_back.rename(columns={'bvdid':'up_bvdid', 'pv_fd_backed':'up_pv_fd_backed'}, inplace=True)
    df = df.merge(up_pv_back, how='left', on=['up_bvdid','closdate_year'], validate='m:1')

    # get last year + 1
    df['last_yr'] = df.groupby('bvdid_r3')['closdate_year'].transform('max')
    # for ever-listed companies, set ipo_last_yr to last_yr + 1
    temp_idx = df['ipo_date_year'].notna()
    df.loc[temp_idx, 'ipo_last_yr'] = df.loc[temp_idx, 'last_yr'] + 1

    # update listed status by unifiying bvdid_r3 level
    df['ipo_yr_ori'] = df['ipo_date_year'].copy()
    df['delisted_yr_ori'] = df['delisted_date_year'].copy()
    df.loc[(df['exch_final']=='otc') | (df['ipo_date_year']>=df['delisted_date_year']), ['ipo_date_year', 'delisted_date_year']] = (np.nan, np.nan)
    # clean up missing ipo_date_year and delisted_date_year
    df.loc[(df['delisted_date_year'].notna()) & (df['ipo_date_year'].isna()), 'delisted_date_year'] = np.nan
    df.loc[(df['listed']=='Delisted') & (df['delisted_date_year'].isna()), 'ipo_date_year'] = np.nan
    # get companies that are listed presuccessors of another restructuring company
    temp_idx = (df['rst_pr'].notna()) & (df['listed_status']==1) & (df['bvdid_r3']!=df['rst_pr'])
    test = df.loc[temp_idx]    
    # propagate their listed status to the restructuring company
    mod_list = []
    for idx,row in test.iterrows():
        pr_idx = (df['bvdid']==row['rst_pr']) & (df['closdate_year']==row['closdate_year'])
        li_status = df.loc[pr_idx, 'listed_status']
        if len(li_status)>0:
            if li_status.iloc[0]==1:
                mod_list.append(row['bvdid'])
                df.loc[pr_idx, ['ipo_date_year', 'delisted_date_year', 'listed', 'exch_final']] = (row['ipo_date_year'], row['delisted_date_year'], row['listed'], row['exch_final'])

    # update delisted_date_year
    df['delisted_date_year'] = df[['delisted_date_year', 'ipo_last_yr']].min(axis=1)
    # get unified ipo and delisted year
    df['ipo_date_year'] = df.groupby('bvdid_r3')['ipo_date_year'].transform('min')
    df['delisted_date_year'] = df.groupby('bvdid_r3')['delisted_date_year'].transform('max')

    # sperately handle rto
    temp_idx = (df['bvdid_r3'].str.contains('r')) & (df['ipo_yr_ori'].isna())
    df.loc[temp_idx, ['ipo_date_year', 'delisted_date_year']] = (np.nan, np.nan)
    # handle rto with two listed firms
    df.loc[df['bvdid_r3']=='GB00438328r', ['ipo_date_year', 'delisted_date_year']] = (2001, 2021)
    df.loc[df['bvdid_r3']=='GB00552331r', ['ipo_date_year', 'delisted_date_year']] = (1987, 2003)
    df.loc[df['bvdid_r3']=='GB01659715r', ['ipo_date_year', 'delisted_date_year']] = (1988, 2009)
    df.loc[df['bvdid_r3']=='GB01760458r', ['ipo_date_year', 'delisted_date_year']] = (1999, 2003)
    df.loc[df['bvdid_r3']=='GB01938746r', ['ipo_date_year', 'delisted_date_year']] = (1989, 2021)
    df.loc[df['bvdid_r3']=='GB02501949r', ['ipo_date_year', 'delisted_date_year']] = (1995, 2009)
    df.loc[df['bvdid_r3']=='GB03142678r', ['ipo_date_year', 'delisted_date_year']] = (1998, 2007)
    df.loc[df['bvdid_r3']=='GB03175632r', ['ipo_date_year', 'delisted_date_year']] = (1996, 2021) # two listed periods
    df.loc[df['bvdid_r3']=='GB03232863', ['ipo_date_year', 'delisted_date_year']] = (2002, 2021) # incorrect rto? looks like flipped
    df.loc[df['bvdid_r3']=='GB03232863r', ['ipo_date_year', 'delisted_date_year']] = (1996, 2004)
    df.loc[df['bvdid_r3']=='GB04108629', ['ipo_date_year', 'delisted_date_year']] = (np.nan, np.nan)
    df.loc[df['bvdid_r3']=='GB04108629r', ['ipo_date_year', 'delisted_date_year']] = (2000, 2020)
    df.loc[df['bvdid_r3']=='GB04978917r', ['ipo_date_year', 'delisted_date_year']] = (2004, 2007)    

    # update listed status
    df['listed_status'] = np.nan
    df.loc[(df['ipo_date_year'] <= df['closdate_year']) & ((df['closdate_year'] < df['delisted_date_year']) | (df['delisted_date_year'].isna())), 'listed_status'] = 1
    # update listed status for the up
    # renmae the listed status of the up to listed_status_up_ori
    df.rename(columns={'listed_status_up':'listed_status_up_ori'}, inplace=True)
    li_status_up = df.groupby(['bvdid', 'closdate_year'])['listed_status'].nth(-1).reset_index()
    li_status_up.rename(columns={'bvdid':'up_bvdid', 'listed_status':'listed_status_up'}, inplace=True)
    df = df.merge(li_status_up, how='left', on=['up_bvdid','closdate_year'], validate='m:1')
    # remove duplicate history of pre-successors
    df = df[~df['bvdid_r3'].isin(mod_list)]

    # assgin the current state
    df.loc[(df['dpt_flag']!=1) & (df['listed_status']!=1) & (df['pv_fd_backed']==1), 'final_state'] = 1
    df.loc[(df['dpt_flag']!=1) & (df['listed_status']!=1) & (df['pv_fd_backed']!=1), 'final_state'] = 2
    df.loc[(df['dpt_flag']!=1) & (df['listed_status']==1), 'final_state'] = 3
    df.loc[(df['dpt_flag']==1) & (df['listed_status_up']==1), 'final_state'] = 4
    df.loc[(df['dpt_flag']==1) & (df['listed_status_up']!=1) & (df['listed_status_guo']!=1) & (df['pv_fd_backed']==1), 'final_state'] = 7
    df.loc[(df['dpt_flag']==1) & (df['listed_status_up']!=1) & (df['listed_status_guo']!=1) & (df['pv_fd_backed']!=1) & (df['up_pv_fd_backed']==1), 'final_state'] = 8
    df.loc[(df['dpt_flag']==1) & (df['listed_status_up']!=1) & (df['final_state'].isna()), 'final_state'] = 5

    # get first and last year
    df['first_yr'] = df.groupby('bvdid_r3')['closdate_year'].transform('min')
    df['last_yr'] = df.groupby('bvdid_r3')['closdate_year'].transform('max')


    df = df[df.closdate_year<=2019]

    # get the carve out firms
    temp_idx_1 = (df.rst_up.notna()) & (df.bvdid!=df.rst_up) & (df.closdate_year<df.rst_pr_first_yr)
    temp_idx_2 = (df.rst_up_2.notna()) & (df.bvdid!=df.rst_up_2) & (df.closdate_year<df.rst_pr_first_yr_2)
    temp_idx = (temp_idx_1 | temp_idx_2) & (df['idpt_flag']!=1)
    carve_out_set = set(df[temp_idx].bvdid_r3.unique())
    df.loc[:, 'carve_out_flag'] = np.nan
    # temp_idx = ((df['rst_ct_flag']==1) | (df['rst_ct_flag_2']==1)) & (df['idpt_flag']!=1) 
    df.loc[df.bvdid_r3.isin(carve_out_set), 'carve_out_flag'] = 1

    # df.to_csv(fin_hist_fd +  ctry + '/' + ctry + '_filter1_merge_cons_yr_panel_ind_link_zp_rst_listed_label_filter_state.csv', index=False)

    # label the last valid report year as failure year
    df.loc[df.toas>=100000, 'valid_report_flag'] = 1
    first_yr = df[df.valid_report_flag==1].groupby('bvdid_r3')['closdate_year'].min()
    last_yr = df[df.valid_report_flag==1].groupby('bvdid_r3')['closdate_year'].max()
    first_yr.rename('first_valid_report_yr', inplace=True)
    last_yr.rename('last_valid_report_yr', inplace=True)
    df = df.merge(first_yr, how='left', left_on='bvdid_r3', right_index=True)
    df = df.merge(last_yr, how='left', left_on='bvdid_r3', right_index=True)
    # temp_idx = (df.closdate_year==(df.last_valid_report_yr+1)) & (~df.final_state.isin([4,5,7,8])) & (~(df['closdate_year']>=2018))
    temp_idx = (df.closdate_year==(df.last_valid_report_yr+1)) & (~df.final_state.isin([4,5,7,8])) & ((df['closdate_year']<2018) | ((df['last_yr']>=2020) & (df['closdate_year']<2019)))
    df.loc[temp_idx, 'final_state'] = 6

    # label the last valid report year for 2018
    df['toas_last'] = df.groupby('bvdid_r3')['toas'].shift(1)
    df['toas_diff'] = df['toas_last'] - df['toas']
    # adjust toas_diff with nr_months
    temp_idx = df['nr_months']!=12
    df.loc[temp_idx, 'toas_diff'] = df.loc[temp_idx, 'toas_diff'].mul(12 / df.loc[temp_idx, 'nr_months'], axis=0)
    df['toas_drop'] = df['toas_diff'] / df['toas_last']
    df['leverage'] = df['ncli'] / df['toas']

    # for 2018, label those toas drop>=30% and leverage>=50%. Label final_state in 2019.
    temp_idx = (df.closdate_year==(df.last_valid_report_yr+1)) & (~df.final_state.isin([4,5,7,8])) & (df['closdate_year']==2019) & (df['last_yr']==2020)  & ((df.groupby('bvdid_r3')['toas_drop'].shift(1)>=0.3) | (df.groupby('bvdid_r3')['leverage'].shift(1)>=0.5))
    df.loc[temp_idx, 'final_state'] = 6

    # add extra rows for the last year of the firm if the last year is not 2019, this makes labeling next state easier
    # in short, we create a new row as a copy for the last year of the firm, and label it as failure
    temp_idx = (df.closdate_year==df.last_yr) & (~df.final_state.isin([4,5,7,8])) & (df.last_yr<2017) #(df.last_yr<2018) 
    extra_rows = df.loc[temp_idx].copy()
    extra_rows['closdate_year'] = extra_rows['closdate_year'] + 1
    extra_rows['final_state'] = 6
    extra_rows['listed_status'] = 0
    
    # handle 2017 and 2018
    ia_idx = (df.historic_status_str.str.contains('Dissolved')) | (df.historic_status_str.str.contains('Bankruptcy')) | (df.historic_status_str.str.contains('Inactive')) | (df.historic_status_str.str.contains('liquidation')) | (df.historic_status_str.str.contains('insolvency'))
    # temp_idx = (df.closdate_year==df.last_yr) & (~df.final_state.isin([4,5,7,8])) & (df.last_yr==2018) & (((df['toas_drop']>=0.3) & (df['toas_last']>=100000)) | (df['toas'].isna()))
    temp_idx = (df.closdate_year==df.last_yr) & (~df.final_state.isin([4,5,7,8])) & (df.last_yr==2018) & (~ia_idx) & ((df['toas'].isna()) | (df['opre'].isna()))
    temp_idx_2 = (df.closdate_year==df.last_yr) & (~df.final_state.isin([4,5,7,8])) & (df.last_yr==2017) & (~ia_idx)
    temp_idx_2 = temp_idx_2 & ((df['toas']<=1000000) | (df['toas_drop']>=0.4) | (df['opre']<=1.0e+06) | (df['toas'].isna()) | (df['opre'].isna()) | (df['leverage']>=0.5))
    temp_idx_3 = (df.closdate_year==df.last_yr) & (df.last_yr.isin([2017,2018,2019])) & ia_idx & (~df.final_state.isin([4,5,7,8]))
    temp_idx = temp_idx | temp_idx_2 | temp_idx_3

    extra_rows_2 = df.loc[temp_idx].copy()
    extra_rows_2['closdate_year'] = extra_rows_2['closdate_year'] + 1
    extra_rows_2['final_state'] = 6
    extra_rows_2['listed_status'] = 0
    extra_rows = pd.concat([extra_rows, extra_rows_2], axis=0)

    # combine extra rows with the original df
    df = pd.concat([df, extra_rows], axis=0)
    # sort by bvdid and year
    df.sort_values(by=['bvdid_r3', 'closdate_year'], inplace=True)
    # label next state
    df['next_state'] = df.groupby('bvdid_r3')['final_state'].shift(-1)

    # postprocessing for states
    df['prev_state'] = df.groupby('bvdid_r3')['final_state'].shift(1)
    temp_idx = (df['prev_state'].isin([1,3,4,5,7,8])) & (df['final_state']==2) & (df['next_state'].isin([1,3,4,5,7,8]))
    df.loc[temp_idx, 'final_state'] = df.loc[temp_idx, 'next_state']

    # some draft
    def replace_consecutive_2s(x):
        # initialize a list to store the output
        output = []
        # initialize a flag to indicate if we have seen a 1 before
        seen_1 = False
        # initialize 2s count
        count_2s = 0
        # loop through the values in x
        for v in x.to_numpy():
            # if the value is 1, set both flags to True and append it to the output
            if (v == 1) and (not seen_1):
                seen_1 = True
                output.append(v)
            # if the value is 2 and we have seen a 1 before, replace it with 1 and append it to the output
            elif v == 2 and seen_1:
                count_2s += 1
            elif (v==1) and (seen_1):
                if count_2s > 0:
                    output.extend([1] * count_2s)
                    count_2s = 0
                output.append(v)
            # otherwise, set the between_1s flag to False and append the original value to the output
            elif (seen_1) and (v in [3,4,5,7,8]) and (count_2s <= 2):
                if count_2s > 0:
                    output.extend([1] * count_2s)
                    count_2s = 0
                output.append(v)
            else:
                seen_1 = False
                if count_2s > 0:
                    output.extend([2] * count_2s)
                    count_2s = 0
                output.append(v)
        # if 2 is the last state
        if count_2s > 0:
            output.extend([2] * count_2s)
        # return the output as a series
        return pd.Series(output, index=x.index)

    # apply the custom function to the state column grouped by id
    df['final_state'] = df.groupby('bvdid_r3')['final_state'].transform(replace_consecutive_2s)

    # update next_state using the updated final_state
    df['next_state'] = df.groupby('bvdid_r3')['final_state'].shift(-1)
    df['prev_state'] = df.groupby('bvdid_r3')['final_state'].shift(1)

    df = df[df.closdate_year<=2019]
    
    # save
    if filter_choice=='first_filter':
        df.to_csv(f'{fin_hist_fd}{ctry}/{ctry}_first_filter_stats{age_test}.csv', index=False)
    elif filter_choice=='before_opre':
        df.to_csv(f'{fin_hist_fd}{ctry}/{ctry}_before_opre_filter{age_test}.csv', index=False)
    elif filter_choice=='select_sample':
        df.to_csv(f'{fin_hist_fd}{ctry}/{ctry}_select_sample{age_test}.csv', index=False)
    else:
        df.to_csv(f'{fin_hist_fd}{ctry}/{ctry}_filter1_merge_cons_yr_panel_ind_link_zp_rst_listed_label_filter_next_state{age_test}.csv', index=False)


