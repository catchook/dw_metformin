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
condition = cohort_info['IP'] == HOST
cohort_info =cohort_info.loc[condition,:]
cohort_hospital= cohort_info['HOSPITAL'].iloc[0]
cohort_target= cohort_info['T'].iloc[0].astype(str)
cohort_control= cohort_info['C1'].iloc[0].astype(str)

print('//////////////////////////////////////////////////////////////////////////////////')
print(cohort_hospital)
print(os.getcwd())
print("input_file reading done")
print('//////////////////////////////////////////////////////////////////////////////////')

del input_file
del cohort_inform

##########################################################################################################################################
#create Measurement table 
#output: M table ; [before latest 1 result] union [after results (index date+ 90days ~ +455days)]
##########################################################################################################################################
## Filter only latest before data(ROW =1 ) & after data (ROW=Null)
## Seperate before, after table; divide _before, _After
## Simplify N : simplify N only who had ESR, CRP Data (before / after)
## Extract  paired data   
## Count N ;WHO GOT PAIRED DATA? - BY T/C
    ## [M_after_simple]: SEPERATE AFTER DATA-- subject_id, meausuremnet_type, measurement_date  
        ## But M table need to be join finally
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
                  ,CAST('0' AS INTEGER) as ROW
    from cdm_hira_2017_results_fnet_v276.cohort a
        left join  cdm_hira_2017.measurement b
        on a.subject_id = b.person_id
    where a.cohort_definition_id in (target, control)
        and b.measurement_date BETWEEN (a.cohort_start_date + 90) AND  (a.cohort_start_date + 455) /*measurement  AFTER cohort start 90~455DAYS*/
        and b.measurement_concept_id in (3020460, 3015183,3010156,3013707,3013682,3022192,3004249,3027484,3040820,3012888,3016723,3028437,
        3007070,3027114,3038553,3013721,3024561,3006923)
"""
#change data schema 
sql=re.sub('cdm_hira_2017_results_fnet_v276',SCHEMA+'_results_dq_v276', sql)
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
print('check m data, columns are {}'.format(m1.columns))
# drop NULL on'value_as_number'
m1.dropna(subset=['value_as_number'], inplace=True)
print('drop NULL on value_as_number')
## Filter only latest before data(ROW =1 ) & after data (ROW=Null)
before = m1.loc[m1['ROW']=='1',['cohort_type', 'subject_id','measurement_date','measurement_type', 'value_as_number']]
after = m1.loc[m1['ROW']=='0',['cohort_type', 'subject_id','measurement_date','measurement_type','value_as_number']]
after.dropna(inplace=True)
print('Filter only latest before data(ROW =1 ) & after data (ROW=0, and not null)')
print('before')
print(before.head(2))
print('after')
print(after.head(2))
## extract  paired data : all measurement type
condition = before['measurement_type'].isin(['CRP','ESR','BUN','Cr','TG','SBP','Hb','Glucose','DBP','LDL','HDL', 'Cholesterol','BMI','AST','ALT','Albumin'])
before_ESR_CRP = before.loc[condition, ['cohort_type',"subject_id",'measurement_type','value_as_number','measurement_date']]
condition = after['measurement_type'].isin(['CRP','ESR','BUN','Cr','TG','SBP','Hb','Glucose','DBP','LDL','HDL', 'Cholesterol','BMI','AST','ALT','Albumin'])
after_ESR_CRP = after.loc[condition,['cohort_type',"subject_id",'measurement_type','value_as_number','measurement_date']]
ESR_CRP = pd.merge(before_ESR_CRP,after_ESR_CRP, on=['subject_id', 'cohort_type','measurement_type'], how='inner')
ESR_CRP_n= ESR_CRP.nunique()
condition = before['measurement_type'].isin(['BUN','Cr'])
Before_BUN_Cr = before.loc[condition, ["subject_id",'measurement_type','value_as_number']]
Before_BUN_Cr = Before_BUN_Cr.pivot_table(index=['subject_id'], columns =['measurement_type'], values= 'value_as_number', fill_value =0).reset_index()
ESR_CRP =pd.merge(left= ESR_CRP, right= Before_BUN_Cr, on= "subject_id", how='left' )
print('ESR_CRP; paired outcomes data, and add BUN, Cr before data for 1st ps matching')
print(ESR_CRP.head(10))
ESR_CRP.to_csv("/data/results/"+cohort_hospital+'_paired_measurement_data.csv')
 
print('extract  paired data ')
## Count N ;WHO GOT PAIRED DATA? - BY T/C
print("who got paired data of ALL OUTCOMES include ESR, CRP , N is {}".format(ESR_CRP_n))
condition= (ESR_CRP['cohort_type']=='T') & (ESR_CRP['measurement_type']=='CRP')
CRP_t_n= ESR_CRP.loc[ condition,['subject_id']].nunique()
condition= (ESR_CRP['cohort_type']=='T') & (ESR_CRP['measurement_type']=='ESR')
ESR_t_n= ESR_CRP.loc[ condition,['subject_id']].nunique()
condition= (ESR_CRP['cohort_type']=='C') & (ESR_CRP['measurement_type']=='CRP')
CRP_c_n= ESR_CRP.loc[ condition,['subject_id']].nunique()
condition= (ESR_CRP['cohort_type']=='C') & (ESR_CRP['measurement_type']=='ESR')
ESR_c_n= ESR_CRP.loc[ condition,['subject_id']].nunique()
all_n = ESR_CRP['subject_id'].nunique()
print("who got all data before, after, N is {}".format(all_n))
condition= ESR_CRP['cohort_type']=='T'
all_t_n = ESR_CRP.loc[condition, ['subject_id']].nunique()
print("who got all data before, after among Target, N is {}".format(all_t_n))
condition= ESR_CRP['cohort_type']=='C'
all_c_n = ESR_CRP.loc[condition, ['subject_id']].nunique()
print("who got all data before, after among Control, N is {}".format(all_c_n))
print("CRP: Target {} , Control {} subject".format(CRP_t_n,CRP_c_n))
print("ESR: Target {} , Control {} subject".format(ESR_t_n,ESR_c_n))
## simplify N : simplify N only who had ESR, CRP Data (before / after)
M_before = pd.merge(left =ESR_CRP[['subject_id','BUN','Cr']],right= before_ESR_CRP, on = 'subject_id' , how='left')
M_after = pd.merge(left =ESR_CRP[['subject_id','BUN','Cr']] ,right= after_ESR_CRP, on = 'subject_id' , how='left')
print(' simplify N : simplify N  who had all outcomes include ESR, CRP Data (before / after)')
print('M_before')
print(M_after.head(3))
M_before.to_csv("/data/results/"+cohort_hospital+'_M_before.csv')
M_after.to_csv("/data/results/"+cohort_hospital+'_M_after.csv')
del before 
del after 
del m1
del ESR_CRP
del before_ESR_CRP
del after_ESR_CRP
## [M_after]-- cohort_type, subject_id, meausuremnet_type, measurement_date , value 
        ## But M_before table need to be join finally   

##########################################################################################################################################
#create Drug table 
#output 1: 1st Ps matching table (index date~ +455days), ingredient binary variabels 
#output 2: Define sub group; only target; join M table --> m in durg_exposure period--> select earliest --> exclude 3 ingredients  
#output 3: Define Dose group; only target; count Metformin --> calculate x (quality/days supply) 
#output 4: DM TABLE; DRUG + Measurement data 
#output 5: 2nd Ps matching table (index date before) 
##########################################################################################################################################
# Extract Drug table -- index date AFTER (+455day), edit drug_Exposure_End_date
# Simplify N : Join [M_after_simple] table -- simplify only who had ESR, CRP data
# output 1: 1st PS matching table
    # join input file[1] 
    # melt type1, type2
    # classify drug_type  

# New column : measurement_in_drug_exposure
# Simplify N: only who had outcome data where [measurement_in_drug_exposure]=1
# Fix Outcome data(ESR, CRP); Which one is first? --group by subject_id, select which measurement_data is first?
# Count N : WHO GOT PAIRED DATA IN DRUG_EXPOSURE? - BY T/C
# output 2: Define sub group (only target);
    # select only target
    # Join input file[1] 
    # melt type1, type2
    # new column: metformin_count
    # new column: ingredient_count; count type ( after drop_duplicates)
    # new column: subgroup; only targe, control-null 
# output 3: Define Dose group (only target)
    # new column: metformin_dose_lists (using regx); select only metformin dose
    # new column: metformin_dose; add metformin doses; multiply (quality/ days of supply) 
    # new column: dose_group: high(over 1,000 mg/day) / low  
# output 4: DM TABLE
    # Join M table;  subject_id, cohort_type /1st PS matching; 7 group, binary columns/ 
    #                measurement_type; CRP_before, after ..etc/ value_as_number  /
    #                dose_group / subgroup
# Count N : WHO GOT PAIRED DATA IN METFORMIN_EXPOSURE(TARGET) & ADM_EXPOSURE (CONTROL)? 
# output 5: 2nd PS matching ; extract before index drug data

# Extract Drug table -- index date AFTER (+455day)
sql=""" 
    select distinct (case when a.cohort_definition_id = target then 'T'
                         when a.cohort_definition_id = control then 'C' else '' end) as cohort_type
                  ,a.subject_id
                  ,b.drug_concept_id
                  ,b.drug_exposure_start_date 
                  ,(b.drug_exposure_end_date + 30) as drug_exposure_end_date
                  ,b.quantity
                  ,b.days_supply
    from cdm_hira_2017_results_fnet_v276.cohort a
        left join  cdm_hira_2017.drug_exposure b
        on a.subject_id = b.person_id
    where a.cohort_definition_id in (target, control)
        and b.drug_exposure_start_date Between a.cohort_start_date and  (a.cohort_start_date + 455)         /*drug exp after cohort start*/
        and b.drug_concept_id in 
        (select distinct descendant_concept_id from cdm_hira_2017.concept_ancestor
        where ancestor_concept_id in( 1503297, 43009032,19122137,35198118,1502855,1502809,43009070,1580747,
                                    793143,40166035,1547504,1516766,1525215,35197921,1502826,43009094,1510202,
                                    43009055,44506754,40170911,40239216,43009020,1559684,19097821,1560171,
                                    1597756,19059796,19001409,43009089,1583722,43009051, 793293,45774751,45774435,
                                    44785829,1594973,19033498,43526465,43008991,43013884,44816332,1530014,1529331) )
"""
#change data schema 
sql=re.sub('cdm_hira_2017_results_fnet_v276', SCHEMA+'_results_dq_v276', sql)
sql= re.sub('cdm_hira_2017', SCHEMA, sql)
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
print('#change data schema ')
# Simplify N : Join [M_after] table -- simplify only who had ESR, CRP data 
condition= M_after['measurement_type'].isin(['ESR','CRP']) 
M_after_CRPESR= M_after.loc[condition, :] 
dm= pd.merge(left=M_after_CRPESR,right=d1, on= ['subject_id','cohort_type'], how='left')
dm.to_csv("/data/results/"+cohort_hospital+'_dm.csv')
print('dm table 1')
print(dm.head(3))
del d1
print('# Simplify N : Join [M_after] table -- simplify only who had ESR, CRP data')
## [M_after]-- cohort_type, subject_id, meausuremnet_type, measurement_date , value 
##[ d1]--cohort_Type, subject)id, Id, drug_start, end, quantity, days_Supply
# output 1: 1st PS matching table
    # join input file[1] 
drug_classification = pd.merge(dm, t1[['Id','Name','type1','type2']], how = 'left', on="Id")
drug_classification.rename(columns={'Id':'drug_concept_id'}, inplace=True)
drug_classification = drug_classification.drop(columns=['drug_exposure_start_date', 'drug_exposure_end_date',
'quantity', 'days_supply','measurement_type', 'value_as_number','Name','drug_concept_id','measurement_date' ], axis=1)
drug_classification.drop_duplicates(inplace=True)
print('# output 1: 1st PS matching table # join input file[1] ')
    # melt type1, type2
drug_classification = drug_classification.melt(id_vars=['subject_id'], value_vars=['type1','type2'], value_name='type')
#drug_classification = drug_classification.groupby('subject_id')['type'].agg(lambda x: list(set(x))).reset_index()
print('# output 1: 1st PS matching table # melt type1, type2')
    # classify drug_type  
drug_classification = drug_classification.pivot_table(index=['subject_id'], columns ='type', aggfunc= 'size', fill_value =0).reset_index()
print('# output 1: 1st PS matching table   # classify drug_type')
    # convert to 1 or 0 
PS_1st = pd.DataFrame(columns=['subject_id','SU', 'alpha', 'dpp4i', 'gnd', 'metformin', 'sglt2', 'tzd'])
columns =['SU', 'alpha', 'dpp4i', 'gnd', 'metformin', 'sglt2', 'tzd']
for column_index in drug_classification.columns:
    if column_index in columns:
        PS_1st[column_index]= drug_classification[column_index]
        PS_1st[column_index] = np.where(PS_1st[column_index]>1, 1,0)
    else: 
        PS_1st[column_index]=0
PS_1st['subject_id']=drug_classification['subject_id']
del drug_classification
print('# output 1: 1st PS matching table   # convert to 1 or 0 ')
PS_1st.to_csv("/data/results/"+cohort_hospital+'_PS_1st.csv')
print(PS_1st.head(3))

# New column : measurement_in_drug_exposure
## change drug_exposure_end_date and dtype (str -> date)
dm['drug_exposure_end_date']=dm['drug_exposure_end_date'].map(lambda x:datetime.strptime(x, '%Y-%m-%d'))
dm['drug_exposure_start_date'] = dm['drug_exposure_start_date'].map(lambda x:datetime.strptime(x, '%Y-%m-%d'))
dm['measurement_date'] = dm['measurement_date'].map(lambda x:datetime.strptime(x, '%Y-%m-%d'))
dm['measure_in_drug_exposure'] = dm.apply(lambda x:  1 if ( (x['drug_exposure_start_date'] <= x['measurement_date']) and 
(x['measurement_date'] <= x['drug_exposure_end_date'])) else 0, axis=1 ) #axis=1 column, compare columns
print('drug_measurement_Table, check measure_in_drug_exposure column;  1 or 0')
print("dm  columns are {}".format(dm.columns))
print(dm.head(10))
# Simplify N: only who had outcome data where [measurement_in_drug_exposure]=1
condition = dm['measure_in_drug_exposure']== 1
dm=dm.loc[condition, :]
print('only measurement in durg exposure')
print(dm.head(10))
print('# Simplify N: only who had outcome data where [measurement_in_drug_exposure]=1')
# new columns: ROWS
dm['ROW'] = dm.sort_values(by='measurement_date', ascending= True).groupby(['subject_id','measurement_type']).cumcount()+1

# Count N : WHO GOT PAIRED DATA IN DRUG_EXPOSURE? - BY T/C
condition= (dm['cohort_type'] =='T')& (dm['measure_in_drug_exposure']==1) & (dm['measurement_type']=='CRP') 
CRP_in_drugexposure_T_n=dm.loc[condition, ['subject_id']].nunique()
condition= (dm['cohort_type'] =='C')& (dm['measure_in_drug_exposure']==1) &( dm['measurement_type']=='CRP') 
CRP_in_drugexposure_C_n=dm.loc[condition, ['subject_id']].nunique()
condition= (dm['cohort_type'] =='T')& (dm['measure_in_drug_exposure']==1 )& (dm['measurement_type']=='ESR' )
ESR_in_drugexposure_T_n=dm.loc[condition, ['subject_id']].nunique()
condition= (dm['cohort_type'] =='C')& (dm['measure_in_drug_exposure']==1) &( dm['measurement_type']=='ESR' )
ESR_in_drugexposure_C_n=dm.loc[condition, ['subject_id']].nunique()
print("CRP in drug exposure, T: {}, C: {}".format(CRP_in_drugexposure_T_n,CRP_in_drugexposure_C_n))
print("ESR in no exposure, T: {}, C: {}".format(ESR_in_drugexposure_T_n, ESR_in_drugexposure_C_n))

# output 2: Define sub group (only target);
    # select only target
condition = dm['cohort_type']=='T'
    # Join input file[1]
dm_T = dm.loc[ condition, :]
print('dm_T first')
print(dm_T.head())
dm_T = pd.merge(dm_T, t1[['Id','Name','type1','type2']], how = 'left', on='Id')
print('dm_T AND T1 MERGE')
print(dm_T.head())
dm_T.rename(columns={'Id':'drug_concept_id'}, inplace=True)
print('dm_T')
print(dm_T.head())
dm_T.drop_duplicates(inplace=True)
print('# output 2: Define sub group (only target);    # Join input file[1]')
    #create drug set group by subject_id, measurement_type, ROW, get type list 
subgroup=dm_T.melt(id_vars=['subject_id','measurement_type','measurement_date'], value_vars=['type1','type2'], value_name ='type') 
subgroup= subgroup.groupby(['subject_id','measurement_type','measurement_date'])['type'].agg( lambda x:list(x)).reset_index()
print(' #create drug set group by subject_id, measurement_type,  get type list ')
    # new column: metformin_count
subgroup['metformin_count'] = subgroup['type'].apply(lambda x: x.count("metformin"))
print(' # new column: metformin_count')
    # new column: ingredient_count; count type
subgroup['ingredient_count'] =subgroup['type'].apply(lambda x: len(set(x)))
print(' # new column: ingredient_count; count type')
    # new column : subgroup (only target, control -null)
subgroup['drug_group'] =subgroup['type'].apply(lambda x:'/'.join(map(str, x)))
print('# new column : subgroup (only target, control -null)')
 ## subgroup columns = id, row, m-type,  type(LIST) , m-count, i-count, d-group
subgroup.to_csv("/data/results/"+cohort_hospital+'_subgroup.csv')

# output 3: Define Dose group (only target)
dosegroup = pd.merge(dm_T[['subject_id','measurement_type','measurement_date','Name','quantity','days_supply']],
                    subgroup, on=['subject_id','measurement_date','measurement_type'], how='inner')
condition=(dosegroup['ingredient_count'] < 3) & (dosegroup['metformin_count']!=0)
dosegroup= dosegroup.loc[condition, :]
print('# output 3: Define Dose group (only target)')
    # new column: metformin_dose (using regx); select only metformin dose
dosegroup['doselist']= dosegroup['Name'].apply(lambda x: re.findall(r'\d*\.\d+|\d+', str(x)))
metformin_dose=[]
for i in dosegroup['doselist']:
    if len(i)==1:
        x=i[0]
        metformin_dose.append(float(x))
    elif len(i)==2:
        x=i[0]
        y=i[1]
        if float(x) >= float(y):
            metformin_dose.append(float(x))
        else: metformin_dose.append(float(y))
    else: metformin_dose.append(0)
dosegroup['metformin_dose']=metformin_dose
del metformin_dose
del subgroup
print('# new column: metformin_dose (using regx); select only metformin dose')
print(dosegroup.head(10))
dosegroup = dosegroup.astype({'quantity':'float', 'days_supply':'float'})
    # new column: dose_group: high(over 1,000 mg/day) / low  
dosegroup['metformin_dose_type']=dosegroup.apply(lambda x:'high' if (x['metformin_dose']* x['quantity']/x['days_supply']>=1000.0) else 'low', axis=1)
print(dosegroup.head())
print('  # new column: dose_group: high(over 1,000 mg/day) / low ')
    # new column : ROW, METFORMIN EXISTS, AND ROW=1 
    # Fix Outcome data(ESR, CRP); Which one is first? --group by subject_id, measurement_type
dosegroup['ROW']= dosegroup.sort_values(by='measurement_date', ascending= True).groupby(['subject_id','measurement_type'])['measurement_date'].cumcount()+1
print("is it work? row?????????????????????????????????????????????????????????????????")
print(dosegroup.head(5))
print('# Fix Outcome data(ESR, CRP); Which one is first? --group by subject_id, measurement_type')
condition = dosegroup['ROW']==1
dosegroup= dosegroup.loc[condition,:]
dosegroup.to_csv("/data/results/"+cohort_hospital+'_dosegroup.csv')
# 이후, target군에서는 metformin이 있으면서 row =1인 것만 추출, control군에서도 row=1 index date에 가까운 걸 추출  
# output 4: DM TABLE
## split dm table, target, control; control  (row=1), but target..yet  
## join dm_Target_table (except row) on dose group (row =1) how = left
## join M_before
## join PS_1st 
## JOIN DOSE, SUBGROUP (ONLY TARGET)

## split dm table, target, control; control  (row=1), but target..yet  
condition= (dm['cohort_type']=='C') & (dm['ROW']==1)
dm_control = dm.loc[condition,['subject_id', 'cohort_type', 'measurement_type','value_as_number', 'measurement_date','ROW','BUN','Cr'] ]
condition= (dm['cohort_type']=='T') 
dm_target = dm.loc[condition,['subject_id', 'cohort_type', 'measurement_type','value_as_number', 'measurement_date','BUN','Cr'] ]
## join dm_Target_table (except row) on dose group (row =1) how = left
Target_dosegroup = pd.merge(right= dm_target , left =dosegroup[['subject_id','measurement_date','ROW']], 
                            on= ['subject_id', 'measurement_date'], how='left')
Target_dosegroup.drop('measurement_date', inplace=True, axis=1)
#concat dm_control, dm_target
dm_after= pd.concat([dm_control, Target_dosegroup], axis =0 )
dm_after.to_csv("/data/results/"+cohort_hospital+'_dm_after.csv')
del dm_control
del dm_target
dm_after.drop('ROW', inplace=True, axis=1)
#simplify m_before
M_before = pd.merge(left= dm_after[['subject_id','measurement_type']], right= M_before, how='left',
                    on= ['subject_id', 'measurement_type']) 
## concat  M_before
dm_total = pd.concat([dm_after,M_before], axis=0)
dm_total.to_csv("/data/results/"+cohort_hospital+'_dm_total.csv')
del dm_after
del M_before
## join 1st_PS MATCHING 
dm_1stPS = pd.merge(left=dm_total, right= PS_1st, on ='subject_id', how='left')
del dm_total
#  join dose group, subgroup
final = pd.merge(left= dm_1stPS, right= dosegroup[['subject_id','drug_group','metformin_dose_type']], on='subject_id', how='left')
final.to_csv("/data/results/"+cohort_hospital+'_final_C1.csv')

# one line version: one line one id; long to wide 
## long to wide: dm_total 


##join PS_1st, dose group, subgroup  
del final 



# Count N : WHO GOT PAIRED DATA IN METFORMIN_EXPOSURE(TARGET) & ADM_EXPOSURE (CONTROL)? 


# output 5: 2nd PS matching ; extract before index drug data


##########################################################################################################################################
#create Disease table  
#output 1: 2nd PS matching table; select covariate disease, first diagonosis dates of TDM
##########################################################################################################################################

##########################################################################################################################################
#create Demographic table  
#output 1: 1st PS matching table; sex, age
##########################################################################################################################################