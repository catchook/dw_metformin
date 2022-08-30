#DW_STAT2: ps 매칭 변수: egfr추가, bun제거 단순히 신기능과 병용약물군,  version 
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
    m1 = pd.read_csv("/home/syk/data2.csv")
    m1.rename(columns={'ID':'subject_id', 'RD.1': 'Renal', 'H/P': 'HP'}, inplace=True)
### 전처리################################################################################################################################################################### 
    ### n수 확인 (zero delete)
    n1 = ff.count_measurement(m1, '1: Total')
    print(n1.head())
    st= cc.Stats
    # ## ps매칭에 필요한 eGFR 계산  (MDRD 공식 사용)
    EGFR =  m1[['subject_id', 'age', 'gender', 'Creatinine','cohort_type']]
    EGFR.drop_duplicates(inplace =True)
    EGFR.dropna(inplace =True)
    EGFR['gender'] = EGFR['gender'].replace({1.0: 0.742,  0.0: 1.0})
    EGFR['Creatinine'] = EGFR['Creatinine'].astype(int)
    EGFR['age'] = EGFR['age'].astype(int)
    EGFR['egfr'] = 175* (EGFR['Creatinine']**-1.154) * (EGFR['age']**-0.203) * EGFR['gender'] 
    print("EGFR")
    print(EGFR.head())   
    ##### trim하기 전: egfr 
    sns.histplot(data = EGFR, x ='egfr', hue='cohort_type')
    plt.savefig("/home/syk/egfr_1_before_trim.png", dpi= 300)
    sns.histplot(data = EGFR, x ='Creatinine', hue='cohort_type')
    plt.savefig("/home/syk/Cr_1_before_trim.png", dpi= 300)
    ################### egfr describe_1##############################################
    condition = EGFR['cohort_type']== 0
    target = EGFR.loc[condition,:]
    control = EGFR.loc[~condition,:]
    t_ = target['egfr'].describe().to_frame().transpose()
    t_['measurement_type']= 'egfr'
    t_['group']= 'target'
    c_ = control['egfr'].describe().to_frame().transpose()
    c_['measurement_type']= 'egfr'
    c_['group']= 'control'
    egfr_describe_1 = pd.concat([t_, c_], axis=0)
    egfr_describe_1['step'] = 'before_trim'
    ####################################################################################
    ## 2.5% trim Cr  
    high_limit= EGFR['Creatinine'].quantile(.975)
    low_limit = EGFR['Creatinine'].quantile(0.025)
    condition = ( low_limit < EGFR['Creatinine'] ) &  (EGFR['Creatinine']< high_limit)
    EGFR2= EGFR.loc[condition, :]
    EGFR2.drop_duplicates(inplace = True)
    del EGFR
    ###  trim 한 후  fig 
    sns.histplot(data = EGFR2, x ='egfr', hue='cohort_type')
    plt.savefig("egfr_2_trim.png", dpi= 300)
    sns.histplot(data = EGFR2, x ='Creatinine', hue='cohort_type')
    plt.savefig("Cr_2_trim.png", dpi= 300)
# ################### egfr describe_2 ###############################################
    condition = EGFR2['cohort_type']== 0
    target = EGFR2.loc[condition,:]
    control = EGFR2.loc[~condition,:]
    t_ = target['egfr'].describe().to_frame().transpose()
    t_['measurement_type']= 'egfr'
    t_['group']= 'target'
    c_ = control['egfr'].describe().to_frame().transpose()
    c_['measurement_type']= 'egfr'
    c_['group']= 'control'
    egfr_describe_2 = pd.concat([t_, c_], axis=0)
    egfr_describe_2['step'] = '2.5% trim'
    #egfr_describe.to_csv("/home/syk/egfr_describe_1.csv")
    ####################################################################################    
    m2 = pd.merge(EGFR2[['subject_id', 'egfr']], m1, on='subject_id', how='left')
    m2.drop_duplicates(inplace=True) 
    n2 = ff.count_measurement(m2, '2: trim 2.5% Cr')
    print(n2.head()) 
   #######################################################################################PS MATCHING################################################################# 
#   ### ps매칭할 데이터만 분리
    ps = m2[['subject_id','cohort_type', 'age', 'gender', 'egfr','SU', 'alpha', 'dpp4i', 'gnd', 'sglt2', 'tzd',  'Creatinine']]
    ps.drop_duplicates(inplace=True)   
    ps.fillna(1.0, inplace =True)
    
    ##ps매칭 전 smd 
    variable = ['age', 'gender', 'egfr', 'SU', 'alpha', 'dpp4i', 'gnd', 'sglt2', 'tzd',  'Creatinine']
    smd1=ff.smd (ps, variable, 'before ps') 
    ### ps matching 통합 버전  
    m_data = st.psmatch(ps, False, 1) # 2nd , 3rd arguments = replacement, ps matching ratio
    m_data.drop_duplicates(inplace=True)
    ##ps 매칭 후 smd
    smd2 = ff.smd(m_data, variable, 'after ps')

    m3 =pd.merge(m_data[['subject_id', 'cohort_type', 'age', 'gender']], m2, on =['subject_id', 'cohort_type', 'age', 'gender'], how='left')
    file_size = sys.getsizeof(m3)
    print("after psmatch file size: ", ff.convert_size(file_size), "bytes")
    print(m3.head())
    print(m3.columns)
    del m2
    ## n수 확인 (ps매칭 후)
    n3 = ff.count_measurement(m2, '3: after ps matching')
    print(n3.head())
    # ################### egfr describe_3 ###############################################
    condition = m3['cohort_type']== 0
    target = m3.loc[condition,:]
    control = m3.loc[~condition,:]
    t_ = target['egfr'].describe().to_frame().transpose()
    t_['measurement_type']= 'egfr'
    t_['group']= 'target'
    c_ = control['egfr'].describe().to_frame().transpose()
    c_['measurement_type']= 'egfr'
    c_['group']= 'control'
    egfr_describe_3 = pd.concat([t_, c_], axis=0)
    egfr_describe_3['step'] = 'after ps matching'
    egfr_describe = pd.concat([egfr_describe_1, egfr_describe_2, egfr_describe_3], axis =0)
    egfr_describe.to_csv("/home/syk/egfr_describe.csv")
    # ####################################################################################
    # ###  ps매칭 한 후  fig 
    sns.histplot(data = m3, x ='egfr', hue='cohort_type')
    plt.savefig("egfr_3_ps.png", dpi= 300)
    sns.histplot(data = m3, x ='Creatinine', hue='cohort_type')
    plt.savefig("Cr_3_ps.png", dpi= 300)
    
    ##  latest 만 사용
    condition = m3['rrow']==1
    m4 = m3.loc[condition, :]
    del m3
    # ### 통계에 필요한 컬럼만 추출 --> na 제거(자동으로 simplify N )
    m4 = m4[['subject_id', 'cohort_type', 'measurement_type', 'value_as_number_before', 'value_as_number_after', 'dose_type', 'drug_group', 'egfr']]
    m4.drop_duplicate(inplace =True)
    m4.dropna(inplace =True)
    # m4['value_as_number_before'] = m3['value_as_number_before'].replace(0, 0.00000000000001) 
    m4['rate']= (m4['value_as_number_after'] - m4['value_as_number_before']) /m4['value_as_number_before'] *100
    m4['diff'] = m4['value_as_number_after'] - m4['value_as_number_before']

    # ### n수 확인 ( simplify 이후 )
    n4 = ff.count_measurement(m4, '4: after Simplify N')
    print(n4.head())

#  ## python stat 차이나는지 이후 검정 https://techbrad.tistory.com/6..안되면 python으로? 
    count_N = pd.concat([n1, n2, n3, n4], axis =0)
    count_N.to_csv("/home/syk/count_N.csv")
#      ## latest 
     ### ttest type별로 등분산성, 정규성, t-test
    test = st.test(m4) #rate
    test2 = st.test2(m4) #diff
    test.to_csv("/home/syk/ttest.csv")
    test2.to_csv("/home/syk/ttest2.csv")
    ### type별로 describe()
    condition = (m4['cohort_type']!= 1)
    control = m4.loc[~condition,:]
    target = m4.loc[condition,:]
    print(control.head())
    print(target.head())
    target_describe = st.describe(target,'latest_target')
    control_describe = st.describe(control, 'latest_control')
    total_describe = pd.concat([target_describe, control_describe], axis=0)
    total_describe.to_csv("/home/syk/total_describe.csv")
    ### 용량별 t-test
    del m4
    ##용량별 psmtching  
    condition = (m2['cohort_type']== 0) & (m2['rrow']==1)
    m5 = m2.loc[condition,:]
    m5['dose_type']= m5['dose_type'].replace('high',1)
    m5['dose_type']= m5['dose_type'].replace('low',0)
    ps = m5[['subject_id','dose_type', 'age', 'gender', 'egfr','SU', 'alpha', 'dpp4i', 'gnd', 'sglt2', 'tzd',  'Creatinine', 'MI',
             'HF', 'PV', 'CV',  'CPD', 'RD', 'PUD', 'MLD', 'DCC', 'HP','Renal',  'MSLD', 'AIDS', 'HT', 'HL', 'Sepsis', 'HTT']]
    ps.drop_duplicates(inplace=True)
    ps.dropna(inplace = True)
    m_data= st.psmatch2(ps, False, 1) # 2nd , 3rd arguments = replacement, ps matching ratio
    m_data.drop_duplicates(inplace =True)
    m6 =pd.merge(m_data[['subject_id', 'dose_type']], m5, on =['subject_id','dose_type'], how='left')
    m6.drop_duplicates(inplace=True)
    file_size = sys.getsizeof(m5)
    print("after dose psmatch  file size: ", ff.convert_size(file_size), "bytes")
    print(m6.head())
    print(m6.columns)
     ## ttest rate & diff
    dose_ttest_rate = st.dose_test(m6)
    dose_ttest_diff = st.dose_test2(m6)
    dose_ttest_rate.to_csv("/home/syk/dose_ttest_rate.csv")
    dose_ttest_diff.to_csv("/home/syk/dose_ttest_diff.csv")
    ## dose paired test 
    condition = m6['dose_type']==1
    high= m6.loc[condition,:]
    low= m6.loc[~condition,:]
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
            dose_describe = st.describe(result,'latest_dose_target')
            dose_describe['dose_type']= name
            dose_describes.append(dose_describe)
    dose_paired_results = pd.concat(paired_results, axis =0 )
    dose_describes_ = pd.concat(dose_describes, axis=0)
    dose_paired_results.to_csv("/home/syk/dose_p_results.csv")  
    dose_describes_.to_csv("/home/syk/dose_p_describes.csv")  
    
### paired ttest
    results = st.drug_preprocess(m5) # ['metformin', 'SU', 'alpha', 'dpp4i', 'gnd', 'sglt2', 'tzd']
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
            describe_result = st.describe(result,'latest_target_paired')
            describe_result['drug_type']= name
            describe_results.append(describe_result)
    p_results = pd.concat(paired_results, axis=0)
    paired_describe = pd.concat(describe_results, axis=0)
    p_results.to_csv("/home/syk/p_results.csv")  
    paired_describe.to_csv("/home/syk/p_describe.csv") 
    