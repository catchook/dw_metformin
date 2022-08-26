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
from functools import reduce
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
    m1 = pd.read_csv("/home/syk/data.csv")
    m1.rename(columns={'ID2':'subject_id'}, inplace=True)
    print(m1.head())
    ### n수 확인 (ps매칭 전, total)
    n1 = ff.count_measurement(m1, '1: total')
    st= cc.Stats
    ### ps매칭할 데이터만 분리
    ps = m1[['subject_id','cohort_type', 'age', 'gender', 'SU', 'alpha', 'dpp4i',
       'gnd', 'metformin', 'sglt2', 'tzd', 'BUN', 'Creatinine']]
    ps.drop_duplicates(inplace=True)   
    
    ### ps matching
    m_data= st.psmatch(ps, True, 2) # 2nd , 3rd arguments = replacement, ps matching ratio
    m2 =pd.merge(m_data, m1, on =['subject_id', 'cohort_type', 'age', 'gender'], how='left')
    file_size = sys.getsizeof(m2)
    print("after psmatch file size: ", ff.convert_size(file_size), "bytes")
    print(m2.head())
    print(m2.columns)
    ## n수 확인 (ps매칭 후)
    n2 = ff.count_measurement(m2, '2: after ps matching')
    ### 통계에 필요한 컬럼만 추출 --> na 제거(자동으로 simplify N )
    m3 = m2[['subject_id', 'cohort_type', 'measurement_type', 'value_as_number_before', 'value_as_number_after', 'dose_type', 'drug_group']]
    m3.drop_duplicates(inplace=True)
    m3.dropna(inplace =True)
    m3['value_as_number_before'] = m3['value_as_number_before'].replace(0, 0.00000000000001) 
    m3['rate']= (m3['value_as_number_after'] - m3['value_as_number_before']) /m3['value_as_number_before'] *100
    m3['diff'] = m3['value_as_number_after'] - m3['value_as_number_before']
    ### n수 확인 ( simplify 이후 )
    n3 = ff.count_measurement(m3, '3: after Simplify N')
    m3.to_csv("/home/syk/m3.csv")
    ### ttest type별로 등분산성, 정규성, t-test
    test = st.test(m3) #rate
    test2 = st.test2(m3) #diff
    test.to_csv("/home/syk/ttest.csv")
    test2.to_csv("/home/syk/ttest2.csv")
    ### type별로 describe()
    target_final, control_final = st.describe(m3)
    target_final.to_csv("/home/syk/target_describe.csv")
    control_final.to_csv("/home/syk/control_describe.csv")
    
     ### 용량별 t-test
    # sub1, sub2 = st.dose_preprocess(m3)
    # high = st.test(sub1)
    # low = st.test(sub2)
    # high['dose']='high'
    # low['dose']='low'
        
    # high2 = st.test2(sub1)
    # low2 = st.test2(sub2)
    # high2['dose']='high'
    # low2['dose']='low'
    
    # t_results = pd.concat([high, low], axis =0)   
    # t_results.to_csv("/home/syk/dose_ttest.csv")
    
    # t_results2 = pd.concat([high2, low2], axis =0)   
    # t_results2.to_csv("/home/syk/dose_ttest2.csv")
### paired ttest
    results = st.drug_preprocess(m3) # ['metformin', 'SU', 'alpha', 'dpp4i', 'gnd', 'sglt2', 'tzd']
    names = ['metformin', 'SU', 'alpha', 'dpp4i', 'gnd', 'sglt2', 'tzd']
    paired_results=[]
    for result, name in zip(results, names):
        if len(result)==0:
            empty = pd.DataFrame({'type': [0], 'shapiro_pvalue_post':[0], 'shapiro_pvalue_pre':[0], 'var_pvalue':[0], 'ttest_F_stat':[0], 'ttest_P_value':[0], 'wilcox_F_stat':[0], 'wilcox_P_value': [0], 'drug_type': [name]})
            paired_results.append(empty)
        else: 
            paired_result = st.pairedtest(result)
            paired_result['drug_type']= name
            paired_results.append(paired_result) 
    p_results = pd.concat([paired_results[0], paired_results[1], paired_results[2], paired_results[3], paired_results[4], paired_results[5], paired_results[6]], axis =0 )
    p_results.to_csv("/home/syk/p_results.csv")            
# ## python stat 차이나는지 이후 검정 https://techbrad.tistory.com/6..안되면 python으로? 
    count_N = pd.concat([n1, n2, n3], axis =0)
    count_N.to_csv("/home/syk/count_N.csv")