## ps동반질환 변수 + egfr 추가. 
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
import random 
# from psmpy import PsmPy
# from psmpy.functions import cohenD
# from psmpy.plotting import *
import seaborn as sns
# import statsmodels.formula.api as smf
import matplotlib.pyplot as plt
import math
from scipy import stats
import DW_function as ff
import DW_class as cc 
##rpy2
# import rpy2
# from rpy2.robjects.packages import importr
# import rpy2.robjects as r
# import rpy2.robjects.pandas2ri as pandas2ri
# from rpy2.robjects import Formula
# pandas2ri.activate()
# #import rpy2's package module
# import rpy2.robjects.packages as rpackages
# from rpy2.robjects.conversion import localconverter
# # rpy2
# base = importr('base')
# utils = importr('utils')
# utils=rpackages.importr('utils')
# utils.install_packages('MatchIt')
# utils.install_packages('stats')
# statss= importr('stats')
# matchit=importr('MatchIt')

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
    cohort_control= cohort_info['C1'].iloc[0].astype(str)
    print("hospital: ", cohort_hospital)
# sql 
    sql = """ select distinct (case when cohort_definition_id = target then 'T'  
    when cohort_definition_id = control then 'C' else '' end) as cohort_type
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
                    when c.measurement_concept_id in (3013682, 3004295) then 'BUN' 
                    when c.measurement_concept_id = 3022192 then 'Triglyceride'
                    when c.measurement_concept_id in (3004249, 21490853) then 'SBP'
                    when c.measurement_concept_id = 3027484 then 'Hb'
                    when c.measurement_concept_id in (3037110, 3040820) then 'Glucose_Fasting'
                    when c.measurement_concept_id = 3016723 then 'Creatinine'
                    when c.measurement_concept_id = 3007070 then 'HDL'
                    when c.measurement_concept_id = 3013721 then 'AST'
                    when c.measurement_concept_id = 3024561 then 'Albumin'
                    when c.measurement_concept_id in (3015089, 3012064, 3016244) then 'insulin'
                    when c.measurement_concept_id = 3038553 then 'BMI'
                    when c.measurement_concept_id = 3004410 then 'HbA1c'
                    when c.measurement_concept_id in (3012888, 21490851) then 'DBP'
                    when c.measurement_concept_id = 3027114 then 'Total cholesterol'
                    when c.measurement_concept_id = 3028437 then 'LDL'
                    when c.measurement_concept_id in (42529224, 3029187, 42870364) then 'NT-proBNP'
                    else '' 
                    END) as measurement_type
      ,c.measurement_date
      ,c.value_as_number
      , (case when d.gender_concept_id = 8507 then 'M' 
              when d.gender_concept_id = 8532 then 'F' 
              else ''
              END) as gender   
      , (EXTRACT(YEAR FROM a.cohort_start_date)-d.year_of_birth) as age
      ,(case        when e.ancestor_concept_id = 4329847 then 'MI'
                    when e.ancestor_concept_id = 316139 then 'HF'
                    when e.ancestor_concept_id = 321052 then 'PV'
                    when e.ancestor_concept_id in (381591, 434056 ) then 'CV'
                    when e.ancestor_concept_id = 4182210 then 'Dementia'
                    when e.ancestor_concept_id = 4063381 then 'CPD'
                    when e.ancestor_concept_id in ( 257628, 134442, 80800, 80809, 256197, 255348)  then 'RD'
                    when e.ancestor_concept_id = 4247120 then 'PUD'
                    when e.ancestor_concept_id in (4064161, 4212540)  then 'MLD'
                    when e.ancestor_concept_id = 201820 then 'D'
                    when e.ancestor_concept_id in (443767,442793 ) then 'DCC'
                    when e.ancestor_concept_id in (192606, 374022) then 'H/P'
                    when e.ancestor_concept_id in (4030518,	4239233, 4245042 ) then 'RD'
                    when e.ancestor_concept_id = 443392 then 'M'
                    when e.ancestor_concept_id in (4245975, 4029488, 192680, 24966) then 'MSLD'
                    when e.ancestor_concept_id = 432851 then 'MST'
                    when e.ancestor_concept_id = 439727 then 'AIDS'
                    when e.ancestor_concept_id = 316866 then 'HT'
                    when e.ancestor_concept_id = 432867 then 'HL'
                    when e.ancestor_concept_id = 132797 then 'Sepsis'
                    when e.ancestor_concept_id = 4254542 then 'HTT'
                    else '' 
                    END) as condition_type
      from cdm_hira_2017_results_fnet_v276.cohort as a

      join
      (select * from cdm_hira_2017.measurement where measurement_concept_id in (3020460, 3010156, 3015183, 3013707, 3013682, 3004295, 3022192, 3004249,21490853,
                                                3027484, 3037110, 3040820,3016723, 3007070,3013721, 3024561,3015089, 3012064, 3016244,3038553,
                                                3004410, 3012888, 21490851 ,3027114 ,3028437 , 42529224, 3029187, 42870364, 40771922, 3005770, 3025315,3036277 ))  as c
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
      
      join 
      ( select a.person_id as person_id, a.condition_concept_id, a.condition_start_date as condition_start_date, b.ancestor_concept_id as ancestor_concept_id 
      from cdm_hira_2017.condition_occurrence as a
        inner join cdm_hira_2017.concept_ancestor as b
        on a.condition_concept_id = b.descendant_concept_id
        where b.ancestor_concept_id in( 4329847,316139, 321052 ,381591,434056,4182210, 4063381, 257628, 134442, 80800, 80809, 256197
                                        ,255348, 4247120 ,4064161, 4212540, 201820, 443767, 442793, 192606, 374022, 4030518, 443392, 4245975, 4029488, 192680,
                                        24966, 432851, 439727, 316866, 432867, 132797, 4254542, 4239233, 4245042) ) as e
      on a.subject_id = e.person_id
      where a.cohort_definition_id in (target, control)
      and b.drug_exposure_start_date between (a.cohort_start_date + 90)  and (a.cohort_start_date + 455)
      and e.condition_start_date between (a.cohort_start_date - 365) and a.cohort_start_date 
"""
# male =0, female =1
# target =0, control =1
    m1= ff.save_query(SCHEMA, cohort_target, cohort_control, sql, c)
        # check file size
    m1.columns= ['cohort_type','subject_id','cohort_start_date','cohort_end_date','drug_concept_id','drug_exposure_start_date',
                                'drug_exposure_end_date', 'quantity', 'days_supply','measurement_type','measurement_date', 'value_as_number','gender'
                                ,'age', 'condition_type']
    ff.change_str_date(m1)
    st= cc.Stats
    m1= st.preprocess(m1)
    file_size = sys.getsizeof(m1)
    print("m1 file size: ", ff.convert_size(file_size), "bytes")
    print(m1.head())
    # 전처리 VALUE =0 제거 
    condition = m1['value_as_number'] != 0
    m1 = m1.loc[condition, :]
    #check N 
    n1 = ff.count_measurement(m1, '1: Total')
#      1. PS matching data
# [1/3] ADD DATA: drug  and comordity history (adm drug group) 
    d = cc.Drug
    PS_1st = d.drug_history(m1, t1)
    History = d.history(m1)
# [2/3] ADD DATA: CCR, Creatinine
    buncr=d.buncr(m1)
    # PS matching 
    ps = pd.merge(m1[['subject_id', 'cohort_type', 'age', 'gender']], PS_1st, on='subject_id', how= 'left')
    ps.drop_duplicates(inplace =True)
    ps2 = pd.merge(ps, buncr,on='subject_id', how='left')
    ps2.drop_duplicates(inplace =True)
    ps3 = pd.merge(ps2, History,on='subject_id', how='left' )
    ps3.drop_duplicates(inplace =True)
    print("before ps matching")
    print(ps3.head())
    del ps
    del ps2
  # 2. Simpliyfy N 
 # [1/4 ] Tagging Dose type 
 # dose_type 
    dose = d.dose(m1, t1)
    m2= pd.merge(m1, dose[['subject_id', 'drug_concept_id','dose_type']], on=['subject_id', 'drug_concept_id'], how= 'left')
    m2.drop_duplicates(inplace=True)
    print("add high, low data: m2")
    print(m2.columns)
    print(m2.head())
    del m1
# [2/4] Simplify N: only who got ESR, CRP
    s = cc.Simplify
    lists = list(m2['measurement_type'].drop_duplicates())
    pair= s.Pair(m2)
# [3/4] simplify N: measurement in drug_Exposure
    exposure= s.Exposure(m2)
# [4/4] simplify N: ALL: 3 ingredient out, target: not metformin out
    final= s.Ingredient(m2, t1)
    print("final")
    print(final.head())
    print(final.columns)
## count N 
    n2 = ff.count_measurement(pair, '2: pair')
    n3 = ff.count_measurement(exposure, '3: exposure')
    n4 = ff.count_measurement(final,'4: rule out')
# 3. Stat
# 통계 계산에 필요한 컬럼은? 
# subject_id, measurement_type, value_as_number_before, value_as_number_after, rate, dose_type, drug_group 
    final.drop_duplicates(inplace=True)
    final = ff.delete_none(final)
#   final['value_as_number_before'] = final['value_as_number_before'].replace(0, 0.00000000000001) 
    final['rate']= (final['value_as_number_after'] - final['value_as_number_before']) /final['value_as_number_before'] *100
    final['diff'] = final['value_as_number_after'] - final['value_as_number_before']
    final['rate'] = final['rate'].round(2)
    final['diff'] = final['diff'].round(2)
    final2 = final[['subject_id', 'cohort_type', 'measurement_type', 'value_as_number_before', 'value_as_number_after', 'rate', 'diff','dose_type', 'drug_group', 'row', 'rrow' ]]
    final2.drop_duplicates(inplace=True)
   # final2.fillna(999, inplace=True)
    file_size = sys.getsizeof(final2)
    print("add rate, diff file size: ", ff.convert_size(file_size), "bytes")
    print("final2 : add rate, diff  variable")
    print("final2")
    print(final2.columns)
    print(final2.head())
    del final
#######################################################################################################################################
# 16 개 병원 데이터를 합치기 위한 코드 
## 1사람당 여러줄로 데이터 가져나오기 
#### 1) before, after, rate, diff, ps, drug_group, dose_type
    data= pd.merge(ps3, final2, on=['subject_id', 'cohort_type'], how='left')
    data.drop_duplicates(inplace= True)
    n5 = ff.count_measurement(data, '5: before merge') 
        ## change names
#### 병원 + subject_id
    original_ids = data['subject_id'].unique()
    while True:
        new_ids = {id_: random.randint(10_000_000, 99_999_999) for id_ in original_ids}
        if len(set(new_ids.values())) == len(original_ids):
            # all the generated id's were unique
            break
        # otherwise this will repeat until they are
    data['ID'] = data['subject_id'].map(new_ids)
    data['ID2'] = str(cohort_hospital) + data['ID'].astype(str)
    data.drop(columns=['subject_id','ID'], inplace= True)
    data.rename(columns={'ID2':'ID'}, inplace=True)
    file_size = sys.getsizeof(data)
    print("data: to merge 16 hospital file size: ", ff.convert_size(file_size), "bytes")
    print("data: to merge 16 hospitals")
    print(data.head())
    print(data.columns)
    sample =data[:500]
    sample.to_csv('/data/results/'+cohort_hospital+'_to_merge_data_sample.csv')
    #check N 
    count_N = pd.concat([n1, n2, n3, n4, n5], axis =0)
    count_N.to_csv("/data/results/"+cohort_hospital+'_count_N.csv')
    data.to_csv('/data/results/'+cohort_hospital+'_to_merge_data.csv')

