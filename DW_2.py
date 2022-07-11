import numpy as np
import pandas as pd
import re #정규표현식
from datetime import date, time, datetime, timedelta
from operator import itemgetter #operator(정렬 기능), itemgetter(각 리스트 다양한 위치에 따라 리스트 정렬)
import sys #sys모듈이 제공하는 모든 기능 이용 
#import glob 
####
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
cohort_hospital= cohort_info['hospital']
cohort_target= cohort_info['T'].iloc[0].astype(str)
cohort_control= cohort_info['C1'].iloc[0].astype(str)

print("input_file reading done")
print('')

del input_file
del cohort_inform
#####################################################################################################################
# CREATE measurement TABLE : CRP, ESR (before, after) 
#####################################################################################################################
 ## m1 BEFORE: FIND LASTEST CRP, ESR  
sql="""   select distinct a.cohort_definition_id,(case when a.cohort_definition_id = target then 'T'
                         when a.cohort_definition_id = control then 'C' else '' end) as cohort_type
                  ,a.subject_id
                  ,a.cohort_start_date
                  ,a.cohort_end_date
                  ,b.measurement_concept_id
                  ,(case when b.measurement_concept_id =3020460 then 'CRP'
                  when b.measurement_concept_id =3015183 then 'ESR' else '' end) as measurement_type
                  ,b.measurement_date
                  ,b.value_as_number
                  ,ROW_NUMBER() OVER (PARTITION BY subject_id, measurement_concept_id 
                                      ORDER BY measurement_date DESC) AS ROW
    from cdm_hira_2017_results_fnet_v276.cohort a
        left join  cdm_hira_2017.measurement b
        on a.subject_id = b.person_id
    where a.cohort_definition_id in (target, control)
        and b.measurement_date <= a.cohort_start_date /*measurement  before cohort start*/
"""
#change data schema 
sql=re.sub('cdm_hira_2017_results_fnet_v276',SCHEMA+'_results_dq_v276', sql)
#change data schema 2
sql= re.sub('cdm_hira_2017', SCHEMA, sql)
#chage target, control 
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

d=m1['subject_id'].nunique()

print('sql measurement/ before success')
print(" measurement table of index date before , total N is {}".format(d))
# m1: select only crp, esr of latest
condition =(m1['measurement_concept_id']=='3020460')|(m1['measurement_concept_id']=='3015183')
m1= m1.loc[condition,:]
condition = (m1['ROW'] == '1')
filter_m1 = m1.loc[condition, :]
e=filter_m1['subject_id'].nunique()
print("latest before measurement table , total N is {}".format(e))
print(" ")

#pivot m2: before_crp, before_esr
p=filter_m1.pivot(index='subject_id', columns='measurement_type', values='value_as_number')


# What else measurement?--for 2nd ps matching 
d=m1['measurement_concept_id'].value_counts(ascending=False)
d= pd.DataFrame(d)
d.to_csv("/data/results/"+cohort_hospital+"_what_else_measurement.csv")
print('what else measruement? make file')
print('')

del d

del rows
del m1

 ## AFTER: FIND LASTEST CRP, ESR  
sql ="""   select distinct a.cohort_definition_id,(case when a.cohort_definition_id = target then 'T'
                         when a.cohort_definition_id = control then 'C' else '' end) as cohort_type
                  ,a.subject_id
                  ,a.cohort_start_date
                  ,a.cohort_end_date
                  ,b.measurement_concept_id
                  ,(case when b.measurement_concept_id =3020460 then 'CRP'
                  when b.measurement_concept_id =3015183 then 'ESR' else '' end) as measurement_type
                  ,b.measurement_date
                  ,b.value_as_number
                  ,ROW_NUMBER() OVER (PARTITION BY subject_id, measurement_concept_id 
                                      ORDER BY measurement_date DESC) AS ROW
    from cdm_hira_2017_results_fnet_v276.cohort a
        left join  cdm_hira_2017.measurement b
        on a.subject_id = b.person_id
    where a.cohort_definition_id in (target, control)
        and a.cohort_start_date  <= b.measurement_date /*measurement  AFTER cohort start*/
        AND b.measurement_concept_id in (3020460, 3015183)
"""
 #change data schema 
sql=re.sub('cdm_hira_2017_results_fnet_v276',SCHEMA+'_results_dq_v276', sql)
#change data schema 2
sql= re.sub('cdm_hira_2017', SCHEMA, sql)
#chage target, control 
sql= re.sub('target', cohort_target, sql)
sql = re.sub('control',cohort_control,sql)
c= con.cursor()
c.execute(sql)
rows=c.fetchall()
m2=[]
for row in rows:   
    row_lists_output=[] 
    for column_index in range(len(row)):
        row_lists_output.append(str(row[column_index]))
    m2.append(row_lists_output)
m2= pd.DataFrame.from_records(m2, columns=['cohort_definition_id,','cohort_type','subject_id','cohort_start_date',
'cohort_end_date','measurement_concept_id','measurement_type','measurement_date','value_as_number', 'ROW'])

## select latest lab test
condition = (m2['ROW'] == '1')
filter_m2 = m2.loc[condition, :]
print('sql measurement/ after success ')
print("latest before measurement table , total N is {}".format(e))
e=filter_m2['subject_id'].nunique()
print("latest after measurement table , total N is {}".format(e))
## join before, after measurement 
m3= filter_m1.merge(filter_m2, on='subject_id', how='inner')
e= m3['subject_id'].nunique()
print('total of measurement table, total N is {}'.format(e))
## pivot 




print('sql measurement after success')



d=m2['subject_id'].nunique()
print("After measurement(before) table  join , total N is {}".format(e))
print("After measurement(after) table  join , total N is {}".format(d))
print('')
 