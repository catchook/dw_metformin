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

print(DATABASE, USER, PW, URL, HOST, PORT, SCHEMA)

# con = pg.connect(database='omop',
#                 user='dbadmin',
#                 password = 'INIT@1234',
#                 host='203.245.2.202',
#                 port=5432)
#target: 2479, control = 2480
# connect to database
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
#cohort_hospital= cohort_info['hospital']
cohort_target= cohort_info['T'].iloc[0].astype(str)
cohort_control= cohort_info['C1'].iloc[0].astype(str)

print(t1.head())
print("input_file reading done")
print('')

del input_file
del cohort_inform
# create DRUG basic table (ID/DRUG/DATE/TYPE (after index date )) 
sql=""" 
    select distinct a.cohort_definition_id,(case when a.cohort_definition_id = target then 'T'
                         when a.cohort_definition_id = control then 'C' else '' end) as cohort_type
                  ,a.subject_id
                  ,a.cohort_start_date
                  ,a.cohort_end_date
                  ,b.drug_concept_id
                  ,b.drug_exposure_start_date
                  ,b.drug_exposure_end_date
                  ,b.quantity
                  ,b.days_supply
    from cdm_hira_2017_results_fnet_v276.cohort a
        left join  cdm_hira_2017.drug_exposure b
        on a.subject_id = b.person_id
    where a.cohort_definition_id in (target, control)
        and a.cohort_start_date <= b.drug_exposure_start_date /*drug exp after cohort start*/
        and b.drug_concept_id in 
        (select distinct descendant_concept_id from cdm_hira_2017.concept_ancestor
        where ancestor_concept_id in( 1503297, 43009032,19122137,35198118,1502855,1502809,43009070,1580747,
                                    793143,40166035,1547504,1516766,1525215,35197921,1502826,43009094,1510202,
                                    43009055,44506754,40170911,40239216,43009020,1559684,19097821,1560171,
                                    1597756,19059796,19001409,43009089,1583722,43009051, 793293,45774751,45774435,
                                    44785829,1594973,19033498,43526465,43008991,43013884,44816332,1530014,1529331) )
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
t2=[]
for row in rows:   
    row_lists_output=[] 
    for column_index in range(len(row)):
        row_lists_output.append(str(row[column_index]))
    t2.append(row_lists_output)
t3= pd.DataFrame.from_records(t2, columns=['cohort_definition_id,','cohort_type','subject_id','cohort_start_date',
'cohort_end_date','Id','drug_exposure_start_date','drug_exposure_end_date', 'quantity', 'days_supply'])
print(t3.tail())
print('sql success')
t3['Id']=t3['Id'].astype(int)
a=t3['subject_id'].nunique()
b=t3.loc[t3['cohort_type']=='T', 'subject_id']
b=b.nunique()
d=t3.loc[t3['cohort_type']=='C', 'subject_id']
d=d.nunique()
print("First total N is {}".format(a))
print("First T is {},First C is {}".format(b,c))
print('')
del t2
del b
del d
# 1st PS matching: join id,type1,2, ingredient_count  
t4 = pd.merge(left= t3, right= t1, how='left', on='Id')
t4.rename(columns={'Id':'drug_concept_id'}, inplace=True)
print(t4.head(10))
print('join success')
print('')

t51= t4[['cohort_type','subject_id','drug_concept_id','type1','ingredient_count']]
t52= t4[['cohort_type','subject_id','drug_concept_id','type2','ingredient_count']]
t51.drop_duplicates(inplace=True)
t52.drop_duplicates(inplace=True)
# print(' type1')
# print(t51.head(10))
# print(" ")
# print("type2")
# print(t52.head(10))
# print('drop duplicates')
# print(" ")
del t1
del t3
# ##1st PS matching: check error and nan 
# nan1 = t51[t51['type1'].isnull()]
# print(double_nan.head())
# print(" check double Nan of type")
# print("")

# ## extract drug conept_id list of nan 
# drug_list =double_nan['drug_concept_id'].drop_duplicates()
# for i in drug_list:
#     print(i)

# print("extract drug conept_id list of nan")

# long to wide
t51 =t51.pivot_table(index=['subject_id'], columns ='type1', aggfunc= 'size', fill_value =0)
t52 =t52.pivot_table(index=['subject_id'], columns ='type2', aggfunc= 'size', fill_value =0)
t51['subject_id']=t51.index
t51=t51.reset_index(drop=True)
t52['subject_id']=t52.index
t52=t52.reset_index(drop=True)
print("t51 column are {}".format(t51.columns))
print(t51.head())
print("t52 column are {}".format(t52.columns))
print(t52.head())
t51= t51.rename(columns ={'type1':'type'}) #nan값은 꼭 채우기, 합치게되면 nan이 됨. 
t52= t52.rename(columns={'type2':'type'})

t522 = pd.DataFrame(columns=['subject_id','SU', 'alpha', 'dpp4i', 'error', 'gnd', 'metformin', 'sglt2', 'tzd'])
#column fix, 불러온 column이 없으면 0으로 채워 넣기. 
columns =['SU', 'alpha', 'dpp4i', 'error', 'gnd', 'metformin', 'sglt2', 'tzd']
for column_index in t52.columns:
    if column_index in columns:
        t522[column_index]=t52[column_index]
    else: 
        t522[column_index]=0
t522['subject_id']=t52['subject_id']
t522=t522.fillna(0)
# print('t522 df')
# print(t522.head())
# print("total t5")
t5= pd.concat([t51, t522], axis=0)


# print(t5.head())

print("start group by ")

t5=t5.groupby(['subject_id']).agg({"alpha": sum, "dpp4i": sum,"error": sum,"gnd": sum,"metformin": sum,"tzd": sum,"SU": sum,"sglt2": sum})
del t51
del t52
del t522
t5['subject_id']= t5.index
t5=t5.reset_index(drop=True)
d=t5['subject_id'].nunique()
print("First total N is {}".format(a))
print("After drug type join , total N is {}".format(d))
#convert to binary variabels(o or 1)

# print(t5.head(10))
print(
     " T5: FOR 1st PS matching: co-drug classification done"
 )
print("")
 # CREATE measurement TABLE : CRP, ESR (before, after) 
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

print(m1.tail())
d=m1['subject_id'].nunique()
print("First total N is {}".format(a))
print("After measurement table of index date before , total N is {}".format(d))
print('sql measurement before success')
print('')
# What else measurement?--for 2nd ps matching 
d=m1['measurement_concept_id'].value_counts()
print('what else measruement?')
print(d)
print('')
# m2: select only crp, esr of before
condition =(m1['measurement_concept_id']=='3020460')|(m1['measurement_concept_id']=='3015183')
m2= m1.loc[condition,:]
print("select only crp or esr test, before idex date")
# print(m2.head())
# print('check any null')
# d= m2.isnull().sum()
# print(d)
d= m2['subject_id'].nunique()
print("m2 table: select only crp, esr of before has {}".format(d))
print(d)
condition = (m2['ROW'] == '1')
filter_m2 = m2.loc[condition, :]
print("filter_m2: only crp, esr latest")
print(filter_m2.head())
e=filter_m2['subject_id'].nunique()
print("First total N is {}".format(a))
print("m2 table: select only crp, esr of before has {}".format(d))
print("After measurement(before) table  join , total N is {}".format(e))

print(" ")
del d
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
m3=[]
for row in rows:   
    row_lists_output=[] 
    for column_index in range(len(row)):
        row_lists_output.append(str(row[column_index]))
    m3.append(row_lists_output)
m3= pd.DataFrame.from_records(m3, columns=['cohort_definition_id,','cohort_type','subject_id','cohort_start_date',
'cohort_end_date','measurement_concept_id','measurement_type','measurement_date','value_as_number', 'ROW'])

# print(m3.tail())
print('sql measurement after success')
d=m3['subject_id'].nunique()
print("First total N is {}".format(a))
print("After measurement(before) table  join , total N is {}".format(e))
print("After measurement(after) table  join , total N is {}".format(d))
print('')

# print('check any null')
# d= m3.isnull().sum()
# print(d)

condition = (m3['ROW'] == '1')
filter_m3 = m3.loc[condition, :]
print("filter_m3 ")
print(filter_m3.head())
b=filter_m3['subject_id'].nunique()
print("First total N is {}".format(a))
print("filter_m3: latest crp, esr data of after index date, has {} id ".format(b))
print(" ")

del b 
del d
# M : create measurement table
M= pd.concat([filter_m2, filter_m3], axis =0)
print(M.columns)
M=M.sort_values(by=['subject_id','measurement_date'], ascending=[False, False]) 
print("M: create measurement table ")
print(M.head())
d=M['subject_id'].nunique()
print("First total N is {}".format(a))
print("After create final measurement table , total N is {}".format(d))
print('')
del m2
del m3
del filter_m2
del d
 # Define group,--CRP, ESR CLOSE MEDICATION (after index) 
filter_m3 = filter_m3[['subject_id','measurement_type','measurement_date']]
t4= t4[['subject_id', 'drug_exposure_start_date','drug_exposure_end_date',"drug_concept_id", 'type1', "type2", 'quantity','days_supply']]
print("before change date of drug_exposure_Date")
print(t4.head())
print("after change date of drug_Exposure_Date: 1 wk before/after")
t4['drug_exposure_start_date'] = t4['drug_exposure_start_date'].map(lambda x:datetime.strptime(x, '%Y-%m-%d')- timedelta(days=7))
t4['drug_exposure_end_date'] = t4['drug_exposure_end_date'].map(lambda x:datetime.strptime(x, '%Y-%m-%d')+ timedelta(days=7))
print(t4.head())
print('')
d=filter_m3['subject_id'].nunique()
print("First total N is {}".format(a))
print("filter_m3: measurement of after index date, total N is {}".format(d))
d=t4['subject_id'].nunique()
print("t4: drug data of after index date, total N is {}".format(d))
print('')

g1= filter_m3.merge(t4, on='subject_id', how='inner')
d=g1['subject_id'].nunique()
print("First total N is {}".format(a))
print("g1: crp, esr data exist  how many? :total N is {}".format(d))
g2=g1[(g1.drug_exposure_start_date <= g1.measurement_date) & (g1.measurement_date <= g1.drug_exposure_end_date)]
d=g2['subject_id'].nunique()
print("g2(measurement in drug exposure): total N is {}".format(d))
g3=g1[(g1.drug_exposure_start_date > g1.measurement_date) | (g1.measurement_date > g1.drug_exposure_end_date)]
d=g3['subject_id'].nunique()
print("g3(measurement in drug no-exposure): total N is {}".format(d))
print("measurement in no-exposure period")
print(g3.head(20))
## if measurement in no-exposure period: drug_Type?

del filter_m3
del t4
del g1
del d
 # Define dose group--forsubgroup analysis 
 # what was medication before index?--for 2nd ps matching 
 # what was co-disease? before index-- for 2nd ps matching 

# if __name__ == "__main__":
# 다 끝난 후 module화 
