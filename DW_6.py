# SQL 
# # check N 
# PS matching 
# # Check N 
# Simplify N 
# # Check N 
# T-Test 
# Paired T-test
### ps matching, simplify 병렬로 독립적으로 진행. 
## ps matching all과 simplify한 n수를 그냥 합쳐서 내보내기.
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
       ,(case       when e.condition_concept_id in (select distinct descendant_concept_id from cdm_hira_2017.concept_ancestor where ancestor_concept_id = 4329847) then 'acute_myocardial_infarction'
                    when e.condition_concept_id in (select distinct descendant_concept_id from cdm_hira_2017.concept_ancestor where ancestor_concept_id = 316139)  then 'Congestive_heart_failure'
                    when e.condition_concept_id in (select distinct descendant_concept_id from cdm_hira_2017.concept_ancestor where ancestor_concept_id = 321052) then 'Peripheral_vascular_disease'
                    when e.condition_concept_id in (select distinct descendant_concept_id from cdm_hira_2017.concept_ancestor where ancestor_concept_id in (381591, 434056)) then 'Cerebrovascular_disease'
                    when e.condition_concept_id in (select distinct descendant_concept_id from cdm_hira_2017.concept_ancestor where ancestor_concept_id = 4182210) then 'Dementia'
                    when e.condition_concept_id in (select distinct descendant_concept_id from cdm_hira_2017.concept_ancestor where ancestor_concept_id = 4063381) then 'Chronic_Pulmonary_disease'
                    when e.condition_concept_id in (select distinct descendant_concept_id from cdm_hira_2017.concept_ancestor where ancestor_concept_id in (257628, 134442, 80800, 80809, 256197, 255348)) then 'Rheumatologic_disease'
                    when e.condition_concept_id in (select distinct descendant_concept_id from cdm_hira_2017.concept_ancestor where ancestor_concept_id = 4247120) then 'Peptic_ulcer_disease'
                    when e.condition_concept_id in (select distinct descendant_concept_id from cdm_hira_2017.concept_ancestor where ancestor_concept_id in (4064161, 4212540)) then 'Mild_liver_disease'
                    when e.condition_concept_id in (select distinct descendant_concept_id from cdm_hira_2017.concept_ancestor where ancestor_concept_id = 201820) then 'Diabetes'
                    when e.condition_concept_id in (select distinct descendant_concept_id from cdm_hira_2017.concept_ancestor where ancestor_concept_id in (443767, 442793)) then 'Diabetes_with_chronic_complications'
                    when e.condition_concept_id in (select distinct descendant_concept_id from cdm_hira_2017.concept_ancestor where ancestor_concept_id in (192606, 374022)) then 'Hemoplegia_or_paralegia'
                    when e.condition_concept_id in (select distinct descendant_concept_id from cdm_hira_2017.concept_ancestor where ancestor_concept_id in  (4030518, 4239233, 4245042)) then 'Renal_disease'
                    when e.condition_concept_id in (select distinct descendant_concept_id from cdm_hira_2017.concept_ancestor where ancestor_concept_id  = 443392) then 'Any_malignancy'
                    when e.condition_concept_id in (select distinct descendant_concept_id from cdm_hira_2017.concept_ancestor where ancestor_concept_id in (4245975, 4029488, 192680, 24966) ) then 'Moderate_to_severe_liver_disease'
                    when e.condition_concept_id in (select distinct descendant_concept_id from cdm_hira_2017.concept_ancestor where ancestor_concept_id  = 432851 ) then 'Metastatic_solid_tumor'
                    when e.condition_concept_id in (select distinct descendant_concept_id from cdm_hira_2017.concept_ancestor where ancestor_concept_id  = 439727 ) then 'AIDS'
                    when e.condition_concept_id in (select distinct descendant_concept_id from cdm_hira_2017.concept_ancestor where ancestor_concept_id  = 316866 ) then 'Hypertension'
                    when e.condition_concept_id in (select distinct descendant_concept_id from cdm_hira_2017.concept_ancestor where ancestor_concept_id  = 432867 ) then 'Hyperlipidemia'
                    when e.condition_concept_id in (select distinct descendant_concept_id from cdm_hira_2017.concept_ancestor where ancestor_concept_id  = 132797 ) then 'Sepsis'
                    when e.condition_concept_id in (select distinct descendant_concept_id from cdm_hira_2017.concept_ancestor where ancestor_concept_id  = 4254542 ) then 'Hypopituitarism'
                    else '' 
                    END) as condition_type
      from cdm_hira_2017_results_fnet_v276.cohort as a

      join
      (select * from cdm_hira_2017.measurement where measurement_concept_id in (3020460, 3010156, 3015183, 3013707, 3013682, 3004295, 3022192, 3004249,21490853,
                                                3027484, 3037110, 3040820,3016723, 3007070,3013721, 3024561,3015089, 3012064, 3016244,3038553,
                                                3004410, 3012888, 21490851 ,3027114 ,3028437 , 42529224, 3029187, 42870364))  as c
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
      (select person_id, condition_concept_id, condition_start_date from cdm_hira_2017.condition_occurrence 
      where condition_concept_id in (select distinct descendant_concept_id from cdm_hira_2017.concept_ancestor
                                where ancestor_concept_id in( 4329847,316139, 321052 ,381591,434056,4182210, 4063381, 257628, 134442, 80800, 80809, 256197
                                ,255348, 4247120 ,4064161, 4212540, 201820, 443767, 442793, 192606, 374022, 4030518, 443392, 4245975, 4029488, 192680,
                                24966, 432851, 439727, 316866, 432867, 132797, 4254542, 4239233, 4245042))) as  e
      on a.subject_id = e.person_id
      where a.cohort_definition_id in (target, control)
      and b.drug_exposure_start_date between a.cohort_start_date  and (a.cohort_start_date + 455)
      and d.condition_start_date < a.cohort_start_date
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
    sample= m1[:500]
    sample.to_csv('/data/results/'+cohort_hospital+'_sample.csv')
    #check N 
    n1 = ff.count_measurement(m1, '1: sql')
    #  1. PS matching data
# [1/3] ADD DATA: drug history (adm drug group) 
#     d = cc.Drug
#     PS_1st = d.drug_history(m1, t1)
#     PS_1st.to_csv('/data/results/'+cohort_hospital+'_PS_1st.csv')
# # [2/3] ADD DATA: BUN, Creatinine
#     buncr=d.buncr(m1)
#     buncr.to_csv('/data/results/'+cohort_hospital+'_buncr.csv')
#     # PS matching 
#     ps = pd.merge(m1[['subject_id', 'cohort_type', 'age', 'gender']], PS_1st, on='subject_id', how= 'left')
#     ps2 = pd.merge(ps, buncr,on='subject_id', how='left')
#     ps2.drop_duplicates(inplace=True)
#     ps2.fillna(999.0, inplace =True) # BUN, CREATININE 수치가 없는 경우?
#     print("before ps matching")
#     print(ps2.head())
# # [3/3] PS matching 
#     m_data= st.psmatch(ps2,True, 2) # 2nd , 3rd arguments = replacement, ps matching ratio
#     m2 =pd.merge(m_data, m1, on =['subject_id', 'cohort_type', 'age', 'gender'], how='left')
#     file_size = sys.getsizeof(m2)
#     print("after psmatch file size: ", ff.convert_size(file_size), "bytes")
#     sample = m2[:100]
#     sample.to_csv('/data/results/'+cohort_hospital+'_after_psmatch.csv')
#     #check N 
#     n2 = ff.count_measurement(m2, '2: after psmatch')
# # # # 2. Simpliyfy N 
# # # [1/4 ] Tagging Dose type 
# # # # dose_type 
#     dose = d.dose(m2, t1)
#     m3= pd.merge(m2, dose[['subject_id', 'drug_concept_id','dose_type']], on=['subject_id','drug_concept_id'], how= 'left')
#     m3.drop_duplicates(inplace=True)
#     print("add high, low data: m3")
#     print(m3.columns)
#     print(m3.head())
#     del m2
#     del m1
#     sample = m3[:100]
#     sample.to_csv('/data/results/'+cohort_hospital+'_add_highlow.csv')
# # [2/4] Simplify N: only who got ESR, CRP
#     s = cc.Simplify
#     lists = list(m3['measurement_type'].drop_duplicates())
# # lists =['CRP','ESR','BUN','Triglyceride','SBP', 'Hb', 'Glucose_Fasting', 'Creatinine', 'HDL','AST', 'Albumin', 'insulin', 'BMI', 'HbA1c', 'DBP', 'Total cholesterol', 'LDL', 'NT-proBNP' ]
#     pair= s.Pair(m3, lists)
# # [3/4] simplify N: measurement in drug_Exposure
#     exposure= s.Exposure(m3, lists)
# # [4/4] simplify N: ALL: 3 ingredient out, target: not metformin out
#     final= s.Ingredient(m3, t1, lists)
#     print("final")
#     print(final.head())
#     print(final.columns)
# # ### count N 
#     n3 = ff.count_measurement(pair, '3: pair')
#     n4 = ff.count_measurement(exposure, '4 exposure')
#     n5 = ff.count_measurement(final,'5 rule out')
# # # 3. Stat
# # # 통계 계산에 필요한 컬럼은? 
# # ## subject_id, measurement_type, value_as_number_before, value_as_number_after, rate, dose_type, drug_group 
#     final['rate']= (final['value_as_number_after'] - final['value_as_number_before']) /final['value_as_number_before'] *100
#     final['diff'] = final['value_as_number_after'] - final['value_as_number_before']
#     final['rate'] = final['rate'].round(2)
#     final['diff'] = final['diff'].round(2)
#     final.drop_duplicates(inplace=True)
#     final.fillna(999.0, inplace=True)
#     file_size = sys.getsizeof(final)
#     print("add rate, diff file size: ", ff.convert_size(file_size), "bytes")
#     print("final : add rate, diff  variable")
#     print("final")
#     print(final.columns)
#     print(final.head())
# # ## 수가 동일할까?
#     n6 = ff.count_measurement(final, '6: calculate dose type')
# # # #[2/] type별로 등분산성, 정규성, t-test
#     test = st.test(final)
#     test.to_csv("/data/results/"+cohort_hospital+'_ttest.csv')
# # # #[3/] type별로 describe()
#     target_final, control_final = st.describe(final)
#     target_final.to_csv("/data/results/"+cohort_hospital+'_target_describe.csv')
#     control_final.to_csv("/data/results/"+cohort_hospital+'_control_describe.csv')
# # ## [4/] 용량별 t-test
#     sub1, sub2 = st.dose_preprocess(final)
#     high = st.test(sub1)
#     low = st.test(sub2)
#     high['dose']='high'
#     low['dose']='low'
#     t_results = pd.concat([high, low], axis =0)   
#     t_results.to_csv("/data/results/"+cohort_hospital+'_dose_ttest.csv')
# # ## [5/] paired t-test
#     results = st.drug_preprocess(final) # ['metformin', 'SU', 'alpha', 'dpp4i', 'gnd', 'sglt2', 'tzd']
#     names = ['metformin', 'SU', 'alpha', 'dpp4i', 'gnd', 'sglt2', 'tzd']
#     paired_results=[]
#     for result, name in zip(results, names):
#         if len(result)==0:
#             empty = pd.DataFrame({'type': [0], 'shapiro_pvalue_post':[0], 'shapiro_pvalue_pre':[0], 'var_pvalue':[0], 'ttest_F_stat':[0], 'ttest_P_value':[0], 'wilcox_F_stat':[0], 'wilcox_P_value': [0], 'drug_type': [name]})
#             paired_results.append(empty)
#         else: 
#             paired_result = st.pairedtest(result)
#             paired_result['drug_type']= name
#             paired_results.append(paired_result) 
#     p_results = pd.concat([paired_results[0], paired_results[1], paired_results[2], paired_results[3], paired_results[4], paired_results[5], paired_results[6]], axis =0 )
#     p_results.to_csv("/data/results/"+cohort_hospital+'_p_results.csv')            
# ## python stat 차이나는지 이후 검정 https://techbrad.tistory.com/6..안되면 python으로? 
#     count_N = pd.concat([n1, n2, n3, n4, n5, n6], axis =0)
#     count_N.to_csv("/data/results/"+cohort_hospital+'_count_N.csv')


