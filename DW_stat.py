import numpy as np
import pandas as pd
import re 
from datetime import date, time, datetime, timedelta
from operator import itemgetter 
import sys 
from pandas.core import strings
import pandas.io.sql as psql
import psycopg2 as pg
from collections import Counter
import os
# from functools import reduce
from psmpy import PsmPy
from psmpy.functions import cohenD
from psmpy.plotting import *
import seaborn as sns
import statsmodels.formula.api as smf
import matplotlib.pyplot as plt
import math
from scipy import stats
import DW_function as ff
import DW_class as cc 
import glob
##rpy2
import rpy2
from rpy2.robjects.packages import importr
import rpy2.robjects as r
import rpy2.robjects.pandas2ri as pandas2ri
from rpy2.robjects import Formula
pandas2ri.activate()
# import rpy2's package module
import rpy2.robjects.packages as rpackages
from rpy2.robjects.conversion import localconverter
# rpy2
base = importr('base')
utils = importr('utils')
utils=rpackages.importr('utils')
utils.install_packages('MatchIt')
utils.install_packages('stats')
statss= importr('stats')
matchit=importr('MatchIt')


if __name__=='__main__' : 
    ### 데이터 불러오기 
# try:
#   path= '/content/drive/MyDrive/DW_metformin/test/data/'
#   files = glob.glob(path + "*.csv")
#   csv = pd.DataFrame()
#   for filename in files:
#     df = pd.read_csv(filename)
#     csv =csv.append(df, ignore_index=True)
#     print(csv)
# except Exception as ex:
#   print("error" + str(ex))
    m0 = pd.read_csv("/home/syk/data2.csv")
    m0.rename(columns={'ID':'subject_id', 'RD.1': 'Renal', 'H/P': 'HP'}, inplace=True)
    print(m0.head())
    ### n수 확인 (전 처리 하기 전 , total)
    n0 = ff.count_measurement(m0, '0: before delete zero baseline, f/u (except crp, esr)')
    print(n0.head())
    ### before, after 모두 0인거 제거 
    condition = (m0['value_as_number_before'] == 0) | (m0['value_as_number_after'] == 0)
    m1 = m0.loc[~condition, :]
    del m0
    ### n수 확인 (ps매칭 전, total)
    n1 = ff.count_measurement(m1, '1: after delete zero baseline, f/u ')
    print(n1.head())
    st= cc.Stats
    ### ps매칭할 데이터만 분리
    ps = m1[['subject_id','cohort_type', 'age', 'gender', 'SU', 'alpha', 'dpp4i', 'gnd', 'metformin', 'sglt2', 'tzd', 'BUN', 'Creatinine','MI', 'HF', 'PV', 'CV',  'CPD', 'RD', 'PUD', 'MLD', 'DCC', 'HP','Renal',  'MSLD', 'AIDS', 'HT', 'HL', 'Sepsis', 'HTT']]
    ps.drop_duplicates(inplace=True)   
    
    ### ps matching
    m_data= st.psmatch(ps, False, 1) # 2nd , 3rd arguments = replacement, ps matching ratio
    m2 =pd.merge(m_data[['subject_id', 'cohort_type', 'age', 'gender']], m1, on =['subject_id', 'cohort_type', 'age', 'gender'], how='left')
    file_size = sys.getsizeof(m2)
    print("after psmatch file size: ", ff.convert_size(file_size), "bytes")
    print(m2.head())
    print(m2.columns)
    
    ## n수 확인 (ps매칭 후)
    n2 = ff.count_measurement(m2, '2: after ps matching')
    print(n2.head())
    
    ## earliest, latest 골라내기 
    condition = m2['row']==1
    m3_1 = m2.loc[condition, :]
    condition= m2['rrow']==1
    m3_2 = m2.loc[condition,:]
    del m2
    ### 통계에 필요한 컬럼만 추출 --> na 제거(자동으로 simplify N )
    m3_1 = m3_1[['subject_id', 'cohort_type', 'measurement_type', 'value_as_number_before', 'value_as_number_after', 'dose_type', 'drug_group']]
    m3_2 = m3_2[['subject_id', 'cohort_type', 'measurement_type', 'value_as_number_before', 'value_as_number_after', 'dose_type', 'drug_group']]
    m3_1.drop_duplicates(inplace=True)
    m3_2.drop_duplicates(inplace=True)
    m3_1.dropna(inplace =True)
    m3_2.dropna(inplace =True)  
    #m3['value_as_number_before'] = m3['value_as_number_before'].replace(0, 0.00000000000001) 
    m3_1['rate']= (m3_1['value_as_number_after'] - m3_1['value_as_number_before']) /m3_1['value_as_number_before'] *100
    m3_1['diff'] = m3_1['value_as_number_after'] - m3_1['value_as_number_before']
    m3_2['rate']= (m3_2['value_as_number_after'] - m3_2['value_as_number_before']) /m3_2['value_as_number_before'] *100
    m3_2['diff'] = m3_2['value_as_number_after'] - m3_2['value_as_number_before']
    
    ### n수 확인 ( simplify 이후 )
    n3_1 = ff.count_measurement(m3_1, '3: after Simplify N')
    print(n3_1.head())
    n3_2 = ff.count_measurement(m3_2, '3: after Simplify N')
    print(n3_2.head())
    #####################################################################################################################################################
    ##earliest 
    ### ttest type별로 등분산성, 정규성, t-test
    test = st.test(m3_1) #rate
    test2 = st.test2(m3_1) #diff
    test.to_csv("/home/syk/ttest_1.csv")
    test2.to_csv("/home/syk/ttest2_1.csv")
    ### type별로 describe()
    condition = (m3_1['cohort_type']!= 1)
    control = m3_1.loc[~condition,:]
    target = m3_1.loc[condition,:]
    print(control.head())
    print(target.head())
    target_describe = st.describe(target)
    control_describe = st.describe(control)
    total_describe = pd.concat([target_describe, control_describe], axis=0)
    total_describe.to_csv("/home/syk/total_describe_1.csv")
    
     ### 용량별 t-test
     ##용량별 psmtching
    condition = (m1['cohort_type']== 0) & (m1['row']==1)
    m4 = m1.loc[condition,:]
    m4['dose_type']= m4['dose_type'].replace('high',1)
    m4['dose_type']= m4['dose_type'].replace('low',0)
    ps = m4[['subject_id','age', 'gender', 'SU', 'alpha', 'dpp4i', 'gnd', 'metformin', 'sglt2', 'tzd', 'BUN', 'Creatinine',
             'MI', 'HF', 'PV', 'CV',  'CPD', 'RD', 'PUD', 'MLD', 'DCC', 'HP','Renal',  'MSLD', 'AIDS', 'HT', 'HL', 'Sepsis', 'HTT','dose_type']]
    ps.drop_duplicates(inplace=True)
    ps.dropna(inplace = True)
    m_data= st.psmatch2(ps, False, 1) # 2nd , 3rd arguments = replacement, ps matching ratio
    m5 =pd.merge(m_data, m4, on =['subject_id','age', 'gender', 'SU', 'alpha', 'dpp4i', 'gnd', 'metformin', 'sglt2', 'tzd', 'BUN', 'Creatinine',
             'MI', 'HF', 'PV', 'CV',  'CPD', 'RD', 'PUD', 'MLD', 'DCC', 'HP','Renal',  'MSLD', 'AIDS', 'HT', 'HL', 'Sepsis', 'HTT','dose_type'], how='left')
    m5.drop_duplicates(inplace=True)
    file_size = sys.getsizeof(m5)
    m5.to_csv("/home/syk/m5_1.csv")

    ## ttest rate & diff
    dose_ttest_rate = st.dose_test(m5)
    dose_ttest_diff = st.dose_test2(m5)
    dose_ttest_rate.to_csv("/home/syk/dose_ttest_rate_1.csv")
    dose_ttest_diff.to_csv("/home/syk/dose_ttest_diff_1.csv")
    ## dose paired test 
    condition = m5['dose_type']==1
    high= m5.loc[condition,:]
    low= m5.loc[~condition,:]
    names = ['high', 'low']  
    dose_list = [high, low]
    paired_results=[]
    dose_describes=[]
    for result, name in zip( dose_list, names):
        if len(result)==0:
            empty = pd.DataFrame({'type': [0], 'shapiro_pvalue_post':[0], 'shapiro_pvalue_pre':[0], 'var_pvalue':[0], 'ttest_F_stat':[0], 'ttest_P_value':[0], 'wilcox_F_stat':[0], 'wilcox_P_value': [0], 'drug_type': [name]})
            paired_results.append(empty)
            empty2 = pd.DataFrame({'measurement_type':[0], 'count':[0], 'mean':[0], '25%':[0], '50%':[0], '75%':[0], 'max':[0]})
            dose_describes.append(empty2)
        else: 
            paired_result = st.pairedtest(result)
            paired_result['dose_type']= name
            paired_results.append(paired_result) 
            dose_describe = st.describe(result)
            dose_describe['dose_type']= name
            dose_describes.append(dose_describe)
    dose_paired_results = pd.concat(paired_results, axis =0 )
    dose_describes_ = pd.concat(dose_describes, axis=0)
    dose_paired_results.to_csv("/home/syk/dose_p_results_1.csv")  
    dose_describes_.to_csv("/home/syk/dose_p_describes_1.csv")  
### paired ttest
    results = st.drug_preprocess(m3_1) # ['metformin', 'SU', 'alpha', 'dpp4i', 'gnd', 'sglt2', 'tzd']
    names = ['metformin', 'SU', 'alpha', 'dpp4i', 'gnd', 'sglt2', 'tzd']
    paired_results=[]
    describe_results=[]
    for result, name in zip(results, names):
        if len(result)==0:
            empty = pd.DataFrame({'type': [0], 'shapiro_pvalue_post':[0], 'shapiro_pvalue_pre':[0], 'var_pvalue':[0], 'ttest_F_stat':[0], 'ttest_P_value':[0], 'wilcox_F_stat':[0], 'wilcox_P_value': [0], 'drug_type': [name]})
            paired_results.append(empty)
            empty2 = pd.DataFrame({'measurement_type':[0], 'count':[0], 'mean':[0], '25%':[0], '50%':[0], '75%':[0], 'max':[0]})
            describe_results.append(empty2)
        else: 
            paired_result = st.pairedtest(result)
            paired_result['drug_type']= name
            paired_results.append(paired_result) 
            describe_result = st.describe(result)
            describe_result['drug_type']= name
            describe_results.append(describe_result)
    p_results = pd.concat(paired_results, axis=0)
    paired_describe = pd.concat(describe_results, axis=0)
#    p_results = pd.concat([paired_results[0], paired_results[1], paired_results[2], paired_results[3], paired_results[4], paired_results[5], paired_results[6]], axis =0 )
    p_results.to_csv("/home/syk/p_results_1.csv")  
    paired_describe.to_csv("/home/syk/p_describe_1.csv")          
# ## python stat 차이나는지 이후 검정 https://techbrad.tistory.com/6..안되면 python으로? 
    count_N = pd.concat([n0, n1, n2, n3_1, n3_2], axis =0)
    count_N.to_csv("/home/syk/count_N.csv")
    
    
    ##############################################################################################################################################
    
    ## latest 
    ### ttest type별로 등분산성, 정규성, t-test
    test = st.test(m3_2) #rate
    test2 = st.test2(m3_2) #diff
    test.to_csv("/home/syk/ttest_2.csv")
    test2.to_csv("/home/syk/ttest2_2.csv")
    ### type별로 describe()
    condition = (m3_2['cohort_type']!= 1)
    control = m3_2.loc[~condition,:]
    target = m3_2.loc[condition,:]
    print(control.head())
    print(target.head())
    target_describe = st.describe(target)
    control_describe = st.describe(control)
    total_describe = pd.concat([target_describe, control_describe], axis=0)
    total_describe.to_csv("/home/syk/total_describe_2.csv")
    
     ### 용량별 t-test
     ##용량별 psmtching
    condition = (m1['cohort_type']== 0) & (m1['rrow']==1)
    m4 = m1.loc[condition,:]
    m4['dose_type']= m4['dose_type'].replace('high',1)
    m4['dose_type']= m4['dose_type'].replace('low',0)
    ps = m4[['subject_id','age', 'gender', 'SU', 'alpha', 'dpp4i', 'gnd', 'metformin', 'sglt2', 'tzd', 'BUN', 'Creatinine',
             'MI', 'HF', 'PV', 'CV',  'CPD', 'RD', 'PUD', 'MLD', 'DCC', 'HP','Renal',  'MSLD', 'AIDS', 'HT', 'HL', 'Sepsis', 'HTT','dose_type']]
    ps.drop_duplicates(inplace=True)
    ps.dropna(inplace = True)
    ps.to_csv("/home/syk/dose_ps_2.csv")
    m_data= st.psmatch2(ps, False, 1) # 2nd , 3rd arguments = replacement, ps matching ratio
    m5 =pd.merge(m_data, m4, on =['subject_id','age', 'gender', 'SU', 'alpha', 'dpp4i', 'gnd', 'metformin', 'sglt2', 'tzd', 'BUN', 'Creatinine',
             'MI', 'HF', 'PV', 'CV',  'CPD', 'RD', 'PUD', 'MLD', 'DCC', 'HP','Renal',  'MSLD', 'AIDS', 'HT', 'HL', 'Sepsis', 'HTT','dose_type'], how='left')
    file_size = sys.getsizeof(m5)
    m5.to_csv("/home/syk/m5_2.csv")

    ## ttest rate & diff
    dose_ttest_rate = st.dose_test(m5)
    dose_ttest_diff = st.dose_test2(m5)
    dose_ttest_rate.to_csv("/home/syk/dose_ttest_rate_2.csv")
    dose_ttest_diff.to_csv("/home/syk/dose_ttest_diff_2.csv")
    ## dose paired test 
    condition = m5['dose_type']==1
    high= m5.loc[condition,:]
    low= m5.loc[~condition,:]
    names = ['high', 'low']  
    dose_list = [high, low]
    paired_results=[]
    dose_describes=[]
    for result, name in zip( dose_list, names):
        if len(result)==0:
            empty = pd.DataFrame({'type': [0], 'shapiro_pvalue_post':[0], 'shapiro_pvalue_pre':[0], 'var_pvalue':[0], 'ttest_F_stat':[0], 'ttest_P_value':[0], 'wilcox_F_stat':[0], 'wilcox_P_value': [0], 'drug_type': [name]})
            paired_results.append(empty)
            empty2 = pd.DataFrame({'measurement_type':[0], 'count':[0], 'mean':[0], '25%':[0], '50%':[0], '75%':[0], 'max':[0]})
            dose_describes.append(empty2)
        else: 
            paired_result = st.pairedtest(result)
            paired_result['dose_type']= name
            paired_results.append(paired_result) 
            dose_describe = st.describe(result)
            dose_describe['dose_type']= name
            dose_describes.append(dose_describe)
    dose_paired_results = pd.concat(paired_results, axis =0 )
    dose_describes_ = pd.concat(dose_describes, axis=0)
    dose_paired_results.to_csv("/home/syk/dose_p_results_2.csv")  
    dose_describes_.to_csv("/home/syk/dose_p_describes_2.csv")  
### paired ttest
    results = st.drug_preprocess(m3_1) # ['metformin', 'SU', 'alpha', 'dpp4i', 'gnd', 'sglt2', 'tzd']
    names = ['metformin', 'SU', 'alpha', 'dpp4i', 'gnd', 'sglt2', 'tzd']
    paired_results=[]
    describe_results=[]
    for result, name in zip(results, names):
        if len(result)==0:
            empty = pd.DataFrame({'type': [0], 'shapiro_pvalue_post':[0], 'shapiro_pvalue_pre':[0], 'var_pvalue':[0], 'ttest_F_stat':[0], 'ttest_P_value':[0], 'wilcox_F_stat':[0], 'wilcox_P_value': [0], 'drug_type': [name]})
            paired_results.append(empty)
            empty2 = pd.DataFrame({'measurement_type':[0], 'count':[0], 'mean':[0], '25%':[0], '50%':[0], '75%':[0], 'max':[0]})
            describe_results.append(empty2)
        else: 
            paired_result = st.pairedtest(result)
            paired_result['drug_type']= name
            paired_results.append(paired_result) 
            describe_result = st.describe(result)
            describe_result['drug_type']= name
            describe_results.append(describe_result)
    p_results = pd.concat(paired_results, axis=0)
    paired_describe = pd.concat(describe_results, axis=0)
#    p_results = pd.concat([paired_results[0], paired_results[1], paired_results[2], paired_results[3], paired_results[4], paired_results[5], paired_results[6]], axis =0 )
    p_results.to_csv("/home/syk/p_results_2.csv")  
    paired_describe.to_csv("/home/syk/p_describe_2.csv")  