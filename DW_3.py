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

# get environment variables 
DATABASE= os.getenv('MEDICAL_RECORDS_DATABASE')
print(DATABASE)
USER =os.getenv('MEDICAL_RECORDS_USER')
print(USER)
PW= os.getenv('MEDICAL_RECORDS_PW')
print(PW)
URL=os.getenv('MEDICAL_RECORDS_URL')
print(URL)
HOST = URL.split(':')[0]
PORT = URL.split(':')[1]
SCHEMA= os.getenv('MEDICAL_RECORDS_SCHEMA')
print('conncet sucess')
print('')

con = pg.connect(database =DATABASE, user=USER, password=PW, host=HOST, port=PORT)
c= con.cursor()

# input_file reading
input_file = sys.argv[1]
cohort_inform= sys.argv[2]

t1 = pd.read_csv(input_file)
t1= t1[['Id', 'Name','type1','type2','ingredient_count']]

cohort_info= pd.read_csv(cohort_inform)
condition = cohort_info['host'] == HOST
cohort_info =cohort_info.loc[condition,:]
cohort_hospital= cohort_info['hospital'].iloc[0]
cohort_target= cohort_info['T'].iloc[0].astype(str)
cohort_control= cohort_info['C1'].iloc[0].astype(str)

print('//////////////////////////////////////////////////////////////////////////////////')
print(cohort_hospital)
print("input_file reading done")
print('//////////////////////////////////////////////////////////////////////////////////')

del input_file
del cohort_inform

##########################################################################################################################################
#create Measurement table 
#output: M table ; [before latest 1 result] union [after results (index date+ 90days ~ +455days)]
##########################################################################################################################################
sql=""" with before as ( select distinct a.cohort_definition_id,(case when a.cohort_definition_id = target then 'T'
                         when a.cohort_definition_id = control then 'C' else '' end) as cohort_type
                  ,a.subject_id
                  ,a.cohort_start_date
                  ,a.cohort_end_date
                  ,b.measurement_concept_id
                  ,(case 
                  when b.measurement_concept_id in (3020460, 3010156) then 'CRP'
                  when b.measurement_concept_id in (3015183, 3013707) then 'ESR'
                  when b.measurement_concept_id = 3013682 then 'BUN' 
                  when b.measurement_concept_id = 3022192 then 'TG'
                  when b.measurement_concept_id = 3004249 then 'SBP'
                  when b.measurement_concept_id = 3027484 then 'Hb'
                  when b.measurement_concept_id = 3040820 then 'Glucose'
                  when b.measurement_concept_id = 3012888 then 'DBP'
                  when b.measurement_concept_id = 3016723 then 'Cr'
                  when b.measurement_concept_id = 3028437 then 'LDL'
                  when b.measurement_concept_id = 3007070 then 'HDL'
                  when b.measurement_concept_id = 3027114 then 'Cholesterol'
                  when b.measurement_concept_id = 3038553 then 'BMI'
                  when b.measurement_concept_id = 3013721 then 'AST'
                  when b.measurement_concept_id = 3024561 then 'Albumin'
                  when b.measurement_concept_id = 3006923 then 'ALT'
                  else '' 
                  END) as measurement_type
                  ,b.measurement_date
                  ,b.value_as_number
    from cdm_hira_2017_results_fnet_v276.cohort a
        left join  cdm_hira_2017.measurement b
        on a.subject_id = b.person_id
    where a.cohort_definition_id in (target, control)
        and b.measurement_date <= a.cohort_start_date /*measurement  before cohort start*/
        and b.measurement_concept_id in (3020460, 3015183,3010156,3013707,3013682,3022192,3004249,3027484,3040820,3012888,3016723,3028437,
        3007070,3027114,3038553,3013721,3024561,3006923)
        ) select *, ROW_NUMBER() OVER (PARTITION BY subject_id, measurement_type ORDER BY measurement_date DESC) AS ROW from before 
        UNION 
select distinct a.cohort_definition_id,(case when a.cohort_definition_id = target then 'T'
                         when a.cohort_definition_id = control then 'C' else '' end) as cohort_type
                  ,a.subject_id
                  ,a.cohort_start_date
                  ,a.cohort_end_date
                  ,b.measurement_concept_id
                  ,(case 
                  when b.measurement_concept_id in (3020460, 3010156) then 'CRP'
                  when b.measurement_concept_id in (3015183, 3013707) then 'ESR'
                  when b.measurement_concept_id = 3013682 then 'BUN' 
                  when b.measurement_concept_id = 3022192 then 'TG'
                  when b.measurement_concept_id = 3004249 then 'SBP'
                  when b.measurement_concept_id = 3027484 then 'Hb'
                  when b.measurement_concept_id = 3040820 then 'Glucose'
                  when b.measurement_concept_id = 3012888 then 'DBP'
                  when b.measurement_concept_id = 3016723 then 'Cr'
                  when b.measurement_concept_id = 3028437 then 'LDL'
                  when b.measurement_concept_id = 3007070 then 'HDL'
                  when b.measurement_concept_id = 3027114 then 'Cholesterol'
                  when b.measurement_concept_id = 3038553 then 'BMI'
                  when b.measurement_concept_id = 3013721 then 'AST'
                  when b.measurement_concept_id = 3024561 then 'Albumin'
                  when b.measurement_concept_id = 3006923 then 'ALT'
                  else '' 
                  END) as measurement_type
                  ,b.measurement_date
                  ,b.value_as_number
                  ,null as ROW
    from cdm_hira_2017_results_fnet_v276.cohort a
        left join  cdm_hira_2017.measurement b
        on a.subject_id = b.person_id
    where a.cohort_definition_id in (target, control)
        and b.measurement_date BETWEEN a.cohort_start_date + 90 AND  a.cohort_start_date + 455 /*measurement  AFTER cohort start 90~455DAYS*/
        and b.measurement_concept_id in (3020460, 3015183,3010156,3013707,3013682,3022192,3004249,3027484,3040820,3012888,3016723,3028437,
        3007070,3027114,3038553,3013721,3024561,3006923)
"""
#change data schema 
sql=re.sub('cdm_hira_2017_results_fnet_v276'+SCHEMA+'_results_dq_v276', sql)
sql= re.sub('cdm_hira_2017', SCHEMA, sql)
sql= re.sub('target', cohort_target, sql)
sql = re.sub('control',cohort_control,sql)
c.execute(sql)
rows=c.fetchall()
m1=[]
for row in rows:   
    row_lists_output=[] 
    for column_index in range(len(row)):
        row_lists_output.append(str(row[column_index]))
    m1.append(row_lists_output)
m1= pd.DataFrame.from_records(m1, columns=['cohort_definition_id,','cohort_type','subject_id','cohort_start_date',
'cohort_end_date','measurement_concept_id','measurement_type','measurement_date','value_as_number', 'ROW'])




##########################################################################################################################################
#create Drug table 
#output 1: 1st Ps matching table (index date~ +455days), ingredient binary variabels 
#output 2: Define sub group; only target; join M table --> m in durg_exposure period--> select earliest --> exclude 3 ingredients  
#output 3: Define Dose group; only target; count Metformin --> calculate x (quality/days supply) 
#output 4: 2nd Ps matching table (index date before) 
##########################################################################################################################################

##########################################################################################################################################
#create Disease table  
#output 1: 2nd PS matching table; select covariate disease, first diagonosis dates of TDM
##########################################################################################################################################

##########################################################################################################################################
#create Demographic table  
#output 1: 1st PS matching table; sex, age
##########################################################################################################################################