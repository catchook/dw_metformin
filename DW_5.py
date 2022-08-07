#DW_5 : 합의된 쿼리문으로 변경 
# 1. Simpliyfy N 
# [1/3] Simplify N: only who got ESR, CRP
# [2/3] simplify N: measurement in drug_Exposure
# [3/3] simplify N: ALL: 3 ingredient out, target: not metformin out 
# 2. Add PS matching data
# [1/3] 1st PS matching: drug history (adm drug group) --최종 대상자 말고도, 코호트 id잇으면 모두..?
# [2/3] 1st PS matching: BUN, Creatinine
# [3/3] 2nd PS matching: disease history --중간보고 이후 
# 4. Primary analysis (Add Subgroup analysis)
# [1/] select earliest measurement data 
# [2/] by dose group 
# [3/] ps 1st matching
# [4/] t-test (t vs c )
# [5/] paired t-test (in t )
# 5. Add HealthScore data:
# [1/3] healthscore variabels: before, after 
# [2/3] calculate healthscore 
##########################################################################################################################################################################
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
import DW_function as ff
import DW_class as cc 

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
                    when c.measurement_concept_id = 3013682 then 'BUN' 
                    when c.measurement_concept_id = 3022192 then 'Triglyceride'
                    when c.measurement_concept_id = 3004249 then 'SBP'
                    when c.measurement_concept_id = 3027484 then 'Hb'
                    when c.measurement_concept_id in (3037110, 3040820) then 'Glucose_Fasting'
                    when c.measurement_concept_id = 3016723 then 'Creatinine'
                    when c.measurement_concept_id = 3007070 then 'HDL'
                    when c.measurement_concept_id = 3013721 then 'AST'
                    when c.measurement_concept_id = 3024561 then 'Albumin'
                    else '' 
                    END) as measurement_type
      ,c.measurement_date
      ,c.value_as_number
      , (case when d.gender_concept_id = 8507 then 'M'
              when d.gender_concept_id = 8532 then 'F' 
              else ''
              END) as gender
      ,d.year_of_birth
      , EXTRACT(YEAR FROM a.cohort_start_date) as start_year     
      , (EXTRACT(YEAR FROM a.cohort_start_date)-d.year_of_birth) as age
      from cdm_hira_2017_results_fnet_v276.cohort as a

      join
      (select * from cdm_hira_2017.measurement where measurement_concept_id in (3020460, 3010156, 3015183, 3013707, 3013682, 3022192, 3004249,
                                                3027484, 3037110, 3040820,3016723, 3007070,3013721, 3024561 ))  as c
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
      and b.drug_exposure_start_date between (a.cohort_start_date + 90) and (a.cohort_start_date + 455)
"""
    m1= ff.save_query(SCHEMA, cohort_target, cohort_control, sql, c)
        # check file size
    m1.columns= ['cohort_type','subject_id','cohort_start_date','cohort_end_date','drug_concept_id','drug_exposure_start_date',
                                'drug_exposure_end_date', 'quantity', 'days_supply','measurement_type','measurement_date', 'value_as_number','gender'
                                ,'year_of_birth','start_year','age']
    file_size = sys.getsizeof(m1)
    print("m1 file size: ", ff.convert_size(file_size), "bytes")
    # m1= pd.read_csv("test_sejong_icn.csv")
   
# 1. Simpliyfy N 
# [1/3] Simplify N: only who got ESR, CRP
    s = cc.Simplify
    lists= ['CRP','ESR']
    pair= s.Pair(m1, lists)
# [2/3] simplify N: measurement in drug_Exposure
    exposure= s.Exposure(m1, lists)
# [3/3] simplify N: ALL: 3 ingredient out, target: not metformin out
    final= s.Ingredient(m1, t1, lists)
### count N 
    n1 = ff.count_measurement(pair, lists)
    n2 = ff.count_measurement(exposure, lists)
    n3 = ff.count_measurement(final, lists)
    n= [n1, n2, n3]
    count_N = reduce(lambda left, right: pd.merge(left, right, on='cohort_type', how='inner'), n)
    count_N.to_csv("/data/results/"+cohort_hospital+'_count_N.csv')

# 2. Add PS matching data
# [1/3] 1st PS matching: drug history (adm drug group) --최종 대상자 말고도, 코호트 id잇으면 모두..?
    d=cc.Drug
    PS_1st = d.psmatch1(m1, t1)
# [2/3] 1st PS matching: BUN, Creatinine
    buncr=d.buncr(m1)
# # [3/3] 2nd PS matching: disease history

# 3. Primary analysis (Add Subgroup analysis)
# [1/] select earliest measurement data 
    final2= ff.make_row(final)
# [2/] by dose group  
    dose=d.dose(m1, t1)
    final2= pd.merge(final2, dose[['subject_id', 'drug_concept_id','dose_type']], on=['subject_id','drug_concept_id'], how= 'left')
## merge
    final2= pd.merge(final2, PS_1st, on='subject_id', how='left')
    final2= pd.merge(final2,buncr, on='subject_id', how='left')
    final2= pd.merge(final2, m1[['subject_id', 'gender','age']], on='subject_id', how='left')
    final2.drop_duplicates(inplace=True)
    file_size = sys.getsizeof(final2)
    print("final2 file size: ", ff.convert_size(file_size), "bytes")
    final2.to_csv("/data/results/"+cohort_hospital+'_final.csv')
# [3/] ps 1st matching
# [4/] t-test (t vs c )
# [5/] paired t-test (in t )
    

# 4. Add HealthScore data:
# [1/] healthscore variabels: before, after 
    s = cc.Simplify
    lists= ['BUN','Triglyceride','SBP','Hb','Glucose_Fasting','Creatinine','HDL','AST','Albumin']
    pair= s.Pair(m1, lists)
# [2/] healthscore:  measurement in drug_Exposure
    exposure= s.Exposure(m1, lists)
# [3/] healthscore: ALL: 3 ingredient out, target: not metformin out
    final= s.Ingredient(m1, t1, lists)
    final.to_csv("/data/results/"+cohort_hospital+'_healthsocre.csv')
### count N 
    n1 = ff.count_measurement(pair, lists)
    n2 = ff.count_measurement(exposure, lists)
    n3 = ff.count_measurement(final, lists)
    n= [n1, n2, n3]
    count_N_hs = reduce(lambda left, right: pd.merge(left, right, on='cohort_type', how='inner'), n)
    count_N_hs.to_csv("/data/results/"+cohort_hospital+'_count_N_hs.csv')
# [4/] calculate healthscore 
