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
# CREATE measurement TABLE : CRP, ESR (before, after) id/cohort_type/CRP_b/CRP_a/ESR_b/ESR_a/measurement_date(ONLY_AFTER)
# output : m, d ('what else measruement? make file')
#       print("who got all CRP, ESR  DATE ?, N is {}".format(all_crp_esr_n))
#       print("who got every before, after esr data? N is {}".format(m_esr_n))
#       print("who got every before, after CRP data?, N is {}".format(m_crp_n))
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
# What else measurement?--for 2nd ps matching 
d=m1['measurement_concept_id'].value_counts(ascending=False)
d= pd.DataFrame(d)
# file_dir= "/data/results/"+cohort_hospital+"_what_else_measurement.csv"
# d.to_csv(file_dir)
print('what else measruement? make file')
print('')
# m1: select only crp, esr of latest
condition =(m1['measurement_concept_id']=='3020460')|(m1['measurement_concept_id']=='3015183')
m1= m1.loc[condition,:]
condition = (m1['ROW'] == '1')
filter_m1 = m1.loc[condition, :]
e=filter_m1['subject_id'].nunique()
print("latest before measurement table , total N is {}".format(e))

#pivot m1: before_crp, before_esr --> function 
p=filter_m1.pivot(index='subject_id', columns='measurement_type', values='value_as_number')
p['subject_id']=p.index
p=p.reset_index(drop= True)
print('pivot')
print(p.head(3))
e=p['subject_id'].nunique()
print('after pivot of before measurement table, total N is still {}?'.format(e))
print(" ")
del d
del rows
del m1
del filter_m1
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
del m2
print('sql measurement/ after success ')
e=p['subject_id'].nunique()
print("latest before measurement table , total N is {}".format(e))
e=filter_m2['subject_id'].nunique()
print("latest after measurement table , total N is {}".format(e))

#pivot m2: after_crp, after_esr --> function 
p2=filter_m2.pivot(index='subject_id', columns='measurement_type', values='value_as_number')
p2['subject_id']=p2.index
p2=p2.reset_index(drop= True)
print('pivot')
print(p2.head(3))
e=p2['subject_id'].nunique()
print('after pivot of after measurement table, total N is still {}?'.format(e))
print(" ")


#before/after crp 자료가 있는 사람 
before_crp= p.loc[p['CRP'].notnull(),['subject_id', 'CRP'] ]
after_crp= p2.loc[p2['CRP'].notnull(),['subject_id', 'CRP'] ]
m2_crp = filter_m2.loc[filter_m2['measurement_type']=='CRP',['cohort_type','subject_id','measurement_date']]
after_crp = after_crp.merge(m2_crp, on='subject_id') #id, type, date, crp
crp= before_crp.merge(after_crp, on='subject_id', suffixes=('_before', '_after')) #crp, before, after, id, type, date
m_crp_n=crp['subject_id'].nunique()
print("who got every before, after CRP data?, N is {}".format(m_crp_n))
print(crp.head(3))

del m2_crp
del before_crp
del after_crp
##before/after esr 자료가 있는 사람 
before_esr= p.loc[p['ESR'].notnull(),['subject_id', 'ESR'] ]
after_esr= p2.loc[p2['ESR'].notnull(),['subject_id', 'ESR'] ]
m2_esr = filter_m2.loc[filter_m2['measurement_type']=='ESR',['subject_id','measurement_date']]
after_esr = after_esr.merge(m2_esr, on='subject_id')
esr= before_esr.merge(after_esr, on='subject_id', suffixes=('_before', '_after'))
m_esr_n=esr['subject_id'].nunique()

print("//////////////////////////////////////////////////////////////////")
print("show me crp.columns = {}".format(crp.columns))
print("show me esr.columns = {}".format(esr.columns))
print("/////////////////////////////////////////////////////////////")
print("who got every before, after esr data? N is {}".format(m_esr_n))
print(esr.head(3))
del m2_esr
del before_esr
del after_esr
del p
del p2
# who got every before, after data?
m  = pd.merge(left=crp, right=esr, how='outer', on='subject_id', suffixes=('_crp','_esr'))

## measurement_date, change dtype (Str-> date), if nan, replace 9999-12-31 
m_all = pd.merge(left=crp, right=esr, how='inner', on='subject_id', suffixes=('_crp','_esr'))
all_crp_esr_n = m_all['subject_id'].nunique()

m['measurement_date_crp'] = m['measurement_date_crp'].replace(np.nan, '9999-12-31')
m['measurement_date_esr'] = m['measurement_date_esr'].replace(np.nan, '9999-12-31')
m['measurement_date_crp'] = m['measurement_date_crp'].map(lambda x:datetime.strptime(x, '%Y-%m-%d'))
m['measurement_date_esr'] = m['measurement_date_esr'].map(lambda x:datetime.strptime(x, '%Y-%m-%d'))
del m_all
print("measurement_table, check mesurement_Date suffixes, replace 9999-12-31")
print(m.head(5))
print("who got all CRP, ESR  DATE ?, N is {}".format(all_crp_esr_n))
print("who got every before, after esr data? N is {}".format(m_esr_n))
print("who got every before, after CRP data?, N is {}".format(m_crp_n))
del crp
del esr
#####################################################################################################################
# CREATE DRUG TABLE (after index) : Define drug group, dose group, 1st PS matching ; id/cohort_type/Drug_group/Dose_group/1st PS matching(adm 7 type) 
# dm_table
# print("First total N is {}".format(total_all_subject_n))
# print("First T is {},First C is {}".format(total_T_subject_n,total_C_subject_n))
# print("measure_in_drug_exposure_n , N is {}".format(measure_in_drug_exposure_n))
# print("measure_in_non_drug_exposure_n , N is {}".format(measure_in_non_drug_exposure_n))
#print("how many who got measure data in no error drug exposrue, N is {}".format(measurement_in_no_error_drug_exposure_n))
#print("how many who got measure data in error drug exposrue, N is {}".format(measurement_in_error_drug_exposure_n))
#####################################################################################################################
# 1. SQL : COUNT N
# 2. SELECT ONLY WHO GOT MESURE_DATA IN DRUG_EXPOSURE 
# 3. merge drug_type file 
# 4. 1st PS matching 
# 5. DEFINE DRUG_GROUP 
# 6. drug grouping: long to wide 
# 7. DEFINE DOSE_GROUP


# 1. SQL : COUNT N
sql=""" 
    select distinct (case when a.cohort_definition_id = target then 'T'
                         when a.cohort_definition_id = control then 'C' else '' end) as cohort_type
                  ,a.subject_id
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
d1=[]
for row in rows:   
    row_lists_output=[] 
    for column_index in range(len(row)):
        row_lists_output.append(str(row[column_index]))
    d1.append(row_lists_output)
d1= pd.DataFrame.from_records(d1, columns=['cohort_type','subject_id','Id','drug_exposure_start_date',
                            'drug_exposure_end_date', 'quantity', 'days_supply'])

d1['Id']=d1['Id'].astype(int)
# count N  --> Function
total_all_subject_n=d1['subject_id'].nunique()
total_T_subject_n=d1.loc[d1['cohort_type']=='T', 'subject_id']
total_T_subject_n=total_T_subject_n.nunique()
total_C_subject_n= d1.loc[d1['cohort_type']=='C', 'subject_id']
total_C_subject_n=total_C_subject_n.nunique()
print("First total N is {}".format(total_all_subject_n))
print("First T is {},First C is {}".format(total_T_subject_n,total_C_subject_n))
print('')

# change drug_exposure_end_date and dtype (str -> date)
d1['drug_exposure_end_date']=d1['drug_exposure_end_date'].map(lambda x:datetime.strptime(x, '%Y-%m-%d')+ timedelta(days=30))
d1['drug_exposure_start_date'] = d1['drug_exposure_start_date'].map(lambda x:datetime.strptime(x, '%Y-%m-%d'))
dm_table = pd.merge(left= m, right= d1, on=['subject_id', 'cohort_type'])
dm_table['measure_in_drug_exposure'] = dm_table.apply(lambda x:  1 if ((x['drug_exposure_start_date'] <= x['measurement_date_esr']) & (x['measurement_date_esr'] <= x['drug_exposure_end_date'])) |
((x['drug_exposure_start_date'] <= x['measurement_date_crp']) & (x['measurement_date_crp'] <= x['drug_exposure_end_date'])) else 0, axis=1 ) #axis=1 column, compare columns
print('drug_measurement_Table, check measure_in_drug_exposure column;  1 or 0')
print(dm_table.head(3))
# 2. SELECT ONLY WHO GOT MESURE_DATA IN DRUG_EXPOSURE : measure_in_drug_exposure
measure_in_drug_exposure= dm_table.loc[dm_table['measure_in_drug_exposure']==1,:]
if measure_in_drug_exposure is None:
    measure_in_drug_exposure = pd.DataFrame()
measure_in_drug_exposure_n =measure_in_drug_exposure['subject_id'].drop_duplicates()
measure_in_drug_exposure_n = measure_in_drug_exposure_n.count()
print("measure_in_drug_exposure_n , N is {}".format(measure_in_drug_exposure_n))
print("measure_in_drug_exposure , total columns are {}".format(measure_in_drug_exposure.columns))
print(measure_in_drug_exposure.head(3))
measure_in_non_drug_exposure = dm_table.loc[dm_table['measure_in_drug_exposure']==0, 'subject_id']
if measure_in_non_drug_exposure is None:
    measure_in_non_drug_exposure = pd.DataFrame()
measure_in_non_drug_exposure= measure_in_non_drug_exposure.drop_duplicates()
measure_in_non_drug_exposure_n = measure_in_non_drug_exposure.count()
print("measure_in_non_drug_exposure_n , N is {}".format(measure_in_non_drug_exposure_n))
print('')
 
del d1
del m
del measure_in_non_drug_exposure
del dm_table

# 3. merge drug_type file 
measure_in_drug_exposure = pd.merge(measure_in_drug_exposure, t1[['Id','Name','type1','type2']], how = 'left', on="Id")
measure_in_drug_exposure.rename(columns={'Id':'drug_concept_id'}, inplace=True)
measure_in_drug_exposure = measure_in_drug_exposure.drop(columns=['drug_exposure_start_date', 'drug_exposure_end_date','measurement_date'], axis=1)
measure_in_drug_exposure.drop_duplicates(inplace=True)
del t1
# 4. 1st PS matching table; (all_drug_Type after index date)

drug_set = measure_in_drug_exposure[['subject_id', 'type1','type2']]
drug_set = pd.melt(drug_set, id_vars=['subject_id'], value_vars=['type1', 'type2'], value_name='type') 
drug_set = drug_set.drop_duplicates()
drug_set = drug_set.pivot_table(index=['subject_id'], columns ='type', aggfunc= 'size', fill_value =0)
drug_set['subject_id']=drug_set.index
drug_set=drug_set.reset_index(drop=True)

## making drug_Type table 
drug_type_table = pd.DataFrame(columns=['subject_id','SU', 'alpha', 'dpp4i', 'gnd', 'metformin', 'sglt2', 'tzd','error'])
columns =['SU', 'alpha', 'dpp4i', 'gnd', 'metformin', 'sglt2', 'tzd','error']
for column_index in drug_set.columns:
    if column_index in columns:
        drug_type_table[column_index]= drug_set[column_index]
    else: 
        drug_type_table[column_index]=0
drug_type_table['subject_id']=drug_set['subject_id']
##convert to 1 or 0 
drug_type_table[['SU', 'alpha', 'dpp4i', 'gnd', 'metformin', 'sglt2', 'tzd','error']] =drug_type_table[['SU', 'alpha', 'dpp4i', 'gnd', 'metformin', 'sglt2', 'tzd','error']].where(drug_type_table[['SU', 'alpha', 'dpp4i', 'gnd', 'metformin', 'sglt2', 'tzd','error']] !=0, 1,0)
print("making drug_Type table for 1st matching")
print(drug_type_table.head(3))
del drug_set

# 5. DEFINE DRUG_GROUP 


# ## join only no error_type
# dm_table = dm_table.drop(['type1','type2'], axis=1)
# dm_table = pd.merge(dm_table, drug_type_table, on ='subject_id', how='inner' )
# no_error_type_measuremnt_n = dm_table['subject_id'].count()
# print("join only no error_type on dm_table,  column are {}, total N  is {}".format(dm_table.columns, no_error_type_measuremnt_n))
# print(dm_table.head(3))
# print("final dm_table columns are {}".format(dm_table.columns))

# del drug_type_table


# 6. DEFINE DOSE_GROUP





print ("////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////")
print("First total N is {}".format(total_all_subject_n))
print("First T is {},First C is {}".format(total_T_subject_n,total_C_subject_n))
print("who got all CRP, ESR  DATE ?, N is {}".format(all_crp_esr_n))
print("who got every before, after esr data? N is {}".format(m_esr_n))
print("who got every before, after CRP data?, N is {}".format(m_crp_n))
print("measure_in_drug_exposure_n , N is {}".format(measure_in_drug_exposure_n))
print("measure_in_non_drug_exposure_n , N is {}".format(measure_in_non_drug_exposure_n))
# print("how many who got measure data in no error drug exposrue, N is {}".format(measurement_in_no_error_drug_exposure_n))
# print("how many who got measure data in error drug exposrue, N is {}".format(measurement_in_error_drug_exposure_n))
print ("////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////")

# if error exist, remove row, insulin 투여하거나, 3제 요법..
### type1에서 error가 없는 거, 그 이후 type2에서도 error가 없는걸로. 
# measurement_in_no_error_drug_exposure = measure_in_drug_exposure[~measure_in_drug_exposure['type1'].str.contains('error', na=False, case=False)]
# measurement_in_no_error_drug_exposure = measurement_in_no_error_drug_exposure[~measurement_in_no_error_drug_exposure['type2'].str.contains('error', na=False, case=False)]
# measurement_in_no_error_drug_exposure_n= measurement_in_no_error_drug_exposure['subject_id'].drop_duplicates()
# measurement_in_no_error_drug_exposure_n= measurement_in_no_error_drug_exposure_n.count()
# print("measure_in_drug_exposure_n , N is {}".format(measure_in_drug_exposure_n))
# print("how many who got measure data in no error drug exposrue, N is {}".format(measurement_in_no_error_drug_exposure_n))
# print('measurement_in_no_error_drug_exposure')
# print(measurement_in_no_error_drug_exposure.head())
# measurement_in_error_drug_exposure_n= measure_in_drug_exposure_n - measurement_in_no_error_drug_exposure_n
# print("how many who got measure data in error drug exposrue, N is {}".format(measurement_in_error_drug_exposure_n))