# SQL 
# # check N 
# PS matching 
# # Check N 
# Simplify N 
# # Check N 
# T-Test 
# Paired T-test
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

# input_file reading 
input_file = sys.argv[1]
cohort_inform= sys.argv[2]
t1 = pd.read_csv(input_file)
t1= t1[['Id', 'Name','type1','type2','ingredient_count']]
t1.rename(columns={'Id':'drug_concept_id'}, inplace=True)
cohort_info= pd.read_csv(cohort_inform)
# connect 
if __name__=='__main__' : 
    SCHEMA, HOST, c  = ff.connect()
    condition = cohort_info['IP'] == HOST
    print("schema:", SCHEMA, "host: ", HOST)
    cohort_info =cohort_info.loc[condition,:]
    cohort_hospital= cohort_info['HOSPITAL'].iloc[0]
    cohort_target= cohort_info['T'].iloc[0].astype(str)
    cohort_control= cohort_info['C2'].iloc[0].astype(str)
    print("hospital: ", cohort_hospital)
# sql 
    sql = """ select distinct (case when cohort_definition_id = target then '0'  
    when cohort_definition_id = control then '1' else '' end) as cohort_type
      ,a.subject_id 
      ,a.cohort_start_date
      ,(a.cohort_start_date + 455) as cohort_end_date 
      ,b.drug_concept_id
      ,b.drug_exposure_start_date
      ,(b.drug_exposure_end_date + 30) as drug_exposure_end_date
      ,b.quantity
      ,b.days_supply
      ,(case        when c.measurement_concept_id in (3020460, 3010156) then 'CRP'
                    when c.measurement_concept_id in (3015183, 3013707) then 'ESR'
                    when c.measurement_concept_id = 3013682 then 'BUN' 
                    when c.measurement_concept_id = 3022192 then 'Triglyceride'
                    when c.measurement_concept_id = 3004249 then 'SBP'
                    when c.measurement_concept_id = 3027484 then 'Hb'
                    when c.measurement_concept_id in (3037110, 3040820) then 'Glucose_Fasting'
                    when c.measurement_concept_id = 3016723 then 'Creatinine'
                    when c.measurement_concept_id = 3007070 then 'HDL'
                    when c.measurement_concept_id = 3013721 then 'AST'
                    when c.measurement_concept_id = 3024561 then 'Albumin'
                    when c.measurement_concept_id in (3015089, 3012064, 3016244) then 'insulin'
                    when c.measurement_concept_id = 3038553 then 'BMI'
                    when c.measurement_concept_id = 3004410 then 'HbA1c'
                    when c.measurement_concept_id = 3012888 then 'DBP'
                    when c.measurement_concept_id = 3027114 then 'Total cholesterol'
                    when c.measurement_concept_id = 3028437 then 'LDL'
                    when c.measurement_concept_id in (42529224, 3029187, 42870364) then 'NT-proBNP'
                    else '' 
                    END) as measurement_type
      ,c.measurement_date
      ,c.value_as_number
      , (case when d.gender_concept_id = 8507 then '0' 
              when d.gender_concept_id = 8532 then '1' 
              else ''
              END) as gender   
      , (EXTRACT(YEAR FROM a.cohort_start_date)-d.year_of_birth) as age
      from cdm_hira_2017_results_fnet_v276.cohort as a

      join
      (select * from cdm_hira_2017.measurement where measurement_concept_id in (3020460, 3010156, 3015183, 3013707, 3013682, 3022192, 3004249,
                                                3027484, 3037110, 3040820,3016723, 3007070,3013721, 3024561,3015089, 3012064, 3016244,3038553,
                                                3004410, 3012888, 3027114 ,3028437 , 42529224, 3029187, 42870364))  as c
      on a.subject_id = c.person_id
      
      join
      (select * from cdm_hira_2017.drug_exposure where drug_concept_id in (select distinct descendant_concept_id
                                from cdm_hira_2017.concept_ancestor
                                where ancestor_concept_id in (1503297,43009032,19122137,35198118,1502855,1502809,43009070,1580747,
                                        793143,40166035,1547504,1516766,1525215,35197921,1502826,43009094,1510202,
                                        43009055,44506754,40170911,40239216,43009020,1559684,19097821,1560171,
                                        1597756,19059796,19001409,43009089,1583722,43009051, 793293,45774751,45774435,
                                        44785829,1594973,19033498,43526465,43008991,43013884,44816332,1530014,1529331)))  as b 
      on a.subject_id = b.person_id
     
      join 
      (select * from cdm_hira_2017.person) as d 
      on a.subject_id = d.person_id

      where a.cohort_definition_id in (target, control)
      and b.drug_exposure_start_date between a.cohort_start_date  and (a.cohort_start_date + 455)
"""
# male =0, female =1
# target =0, control =1
    m1= ff.save_query(SCHEMA, cohort_target, cohort_control, sql, c)
        # check file size
    m1.columns= ['cohort_type','subject_id','cohort_start_date','cohort_end_date','drug_concept_id','drug_exposure_start_date',
                                'drug_exposure_end_date', 'quantity', 'days_supply','measurement_type','measurement_date', 'value_as_number','gender'
                                ,'age']
    file_size = sys.getsizeof(m1)
    print("m1 file size: ", ff.convert_size(file_size), "bytes")
    #check N 
    n1 = ff.count_measurement(m1)
    n1.to_csv('/data/results/'+cohort_hospital+'_n1.csv')

    #  1. PS matching data
# [1/3] ADD DATA: drug history (adm drug group) 
    d=cc.Drug
    PS_1st = d.drug_history(m1, t1)
# [2/3] ADD DATA: BUN, Creatinine
    buncr=d.buncr(m1)
    # PS matching 
    ps = pd.merge(m1[['subject_id', 'cohort_type', 'age', 'gender']], PS_1st, on='subject_id', how= 'left')
    ps = pd.merge(ps, buncr,on='subject_id', how='left')
    ps.drop_duplicates(inplace=True)
    ps.fillna(0) # BUN, CREATININE 수치가 없는 경우?
# [3/3] PS matching 
    st= cc.Stats
    m_data= st.psmatch(ps)
    m2 =pd.merge(m_data, m1, on =['subject_id', 'cohort_type', 'age', 'gender'], how='left')
    file_size = sys.getsizeof(m2)
    print("m1 file size: ", ff.convert_size(file_size), "bytes")
    m2.to_csv('/data/results/'+cohort_hospital+'_n1.csv')
    #check N 
    n2 = ff.count_measurement(m2)
    n2.to_csv('/data/results/'+cohort_hospital+'_n2.csv')
# 2. Simpliyfy N 
# [1/3] Simplify N: only who got ESR, CRP
    s = cc.Simplify
    lists = list(m2['measurement_type'].drop_duplicates())
    pair= s.Pair(m2, lists)
# [2/3] simplify N: measurement in drug_Exposure
    exposure= s.Exposure(m2, lists)
# [3/3] simplify N: ALL: 3 ingredient out, target: not metformin out
    final= s.Ingredient(m2, t1, lists)
    print("final")
    print(final.head())
### count N 
    n1 = ff.count_measurement(pair )
    n2 = ff.count_measurement(exposure)
    n3 = ff.count_measurement(final )
    n= [n1, n2, n3]
    count_N = reduce(lambda left, right: pd.merge(left, right, on='cohort_type', how='inner'), n)
    count_N.to_csv("/data/results/"+cohort_hospital+'_count_N.csv')
# 3. Stat
# [1/ ] Tagging Dose type 
# # dose_type 
# 1) quantity, days_supply 컬럼 값 붙이기. 
    final2 = pd.merge(final, m1[['subject_id', 'measurement_type', 'measurement_date', 'drug_concept_id','quantity', 'days_supply']], how='left',
                        left_on= ['subject_id','measurement_type','measurement_date_after','drug_concept_id'], right_on = ['subject_id','measurement_type','measurement_date','drug_concept_id'])
    file_size = sys.getsizeof(final2)
    print("final2 file size: ", ff.convert_size(file_size), "bytes")
    final2.to_csv('/data/results/'+cohort_hospital+'_final2.csv')
# 2) 계산해서 high, low 구분하기. 
#     dose = d.dose(final2, t1)
#     del final2
#     final1= pd.merge(final, dose[['subject_id', 'drug_concept_id','dose_type']], on=['subject_id','drug_concept_id'], how= 'left')
#     final1.drop_duplicates(inplace=True)
# # # [2/] calculate rate
#     final1=st.preprocess(final1)
# # 통계 계산에 필요한 컬럼은? 
# ## subject_id, measurement_type, value_as_number_before, value_as_number_after, rate, dose_type, drug_group 
#     final2 = final1[['subject_id', 'measurement_type', 'value_as_number_before', 'value_as_number_after', 'rate', 'dose_type', 'drug_group']]
#     final2.drop_duplicates(inplace=True)
# ## 수가 동일할까?
#     n4 = ff.count_measurement(final2)
#     n4.to_csv("/data/results/"+cohort_hospital+'_n4.csv')

# # #[2/] type별로 등분산성, 정규성 검정 이후.
#     normality = st.normality(final2) # rate 정규성 검정 
#     normality.to_csv("/data/results/"+cohort_hospital+'_normality.csv')
#     vartest = st.vartest(final2)
#     vartest.to_csv("/data/results/"+cohort_hospital+'_vartest.csv')
# # #[3/] type별로 t-test
#     test = st.test(final2)
#     test.to_csv("/data/results/"+cohort_hospital+'_test.csv')
# ## [4/] 용량별 t-test
#     sub1, sub2 = st.dose_preprocess(final2)
#     high_normality = st.normality(sub1)
#     high_normality['dose'] ='high'
#     low_normality = st.normality(sub2)
#     low_normality['dose'] ='low'
#     high_vartest = st.vartest(sub1)
#     high_vartest['dose'] ='high'
#     low_vartest = st.vartest(sub2)
#     low_vartest['dose'] ='low'
#     high = st.ttest(sub1)
#     high['dose']='high'
#     low= st.ttest(sub2)
#     low['dose']='low'
#     dose_nomality = pd.concat([high_normality,low_normality])
#     dose_vartest= pd.concat([high_vartest,low_vartest ])
#     dose_test = pd.concat([high, low])
#     dose_nomality.to_csv("/data/results/"+cohort_hospital+'_dose_nomality.csv')
#     dose_vartest.to_csv("/data/results/"+cohort_hospital+'_dose_vartest.csv')
#     dose_test.to_csv("/data/results/"+cohort_hospital+'_dose_test.csv')
# ## [5/] paired t-test
#     results = st.drug_preprocess(final2) # ['metformin', 'SU', 'alpha', 'dpp4i', 'gnd', 'sglt2', 'tzd']
#     paired_results=[]
#     for i in results:
#         if len(i)==0:
#             paired_results.append(0)
#         else: 
#             paired_result = st.pairedtest(i)
#             paired_results.append(paired_result)
            
#     metformin = paried_results[0]
#     metformin.to_csv("/data/results/"+cohort_hospital+'_metformin.csv')
#     SU= paried_results[1]
#     SU.to_csv("/data/results/"+cohort_hospital+'_SU.csv')
#     alpha = paried_results[2]
#     alpha.to_csv("/data/results/"+cohort_hospital+'_alpha.csv')
#     dpp4i = paried_results[3]
#     dpp4i.to_csv("/data/results/"+cohort_hospital+'_dpp4i.csv')
#     gnd= paried_results[4]
#     gnd.to_csv("/data/results/"+cohort_hospital+'_gnd.csv')
#     sglt2 = paried_results[5]
#     sglt2.to_csv("/data/results/"+cohort_hospital+'_sglt2.csv')
#     tzd =paried_results[6]
#     tzd.to_csv("/data/results/"+cohort_hospital+'_tzd.csv')
            
            
## python stat 차이나는지 이후 검정 https://techbrad.tistory.com/6..안되면 python으로?

    


