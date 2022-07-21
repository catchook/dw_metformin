import os 
import pandas as pd 
import numpy as np 
import re
import psycopg2 as pg
from logging.config import dictConfig
import logging
from datetime import date, time, datetime, timedelta


dictConfig({
    'version': 1, #version에는 고정된 값 1로, 다른 값 입력시 value error  발생
    'formatters':{
        'default':{
            'format':'[%(asctime)s] %(message)s',
        }
    },
     'handler':{
        'file':{
            'level':'DEBUG',
            'class': 'logging.logging.FileHandler',
            'filename':'debug.log',
            'formatter':'default',
        },
     },
     'root': {
        'level':'DEBUG',
        'handlers':['file']
     }
})

# environment variabels, connect 
def connect():
    DATABASE= os.getenv('MEDICAL_RECORDS_DATABASE')
    USER =os.getenv('MEDICAL_RECORDS_USER')
    PW= os.getenv('MEDICAL_RECORDS_PW')
    URL=os.getenv('MEDICAL_RECORDS_URL')
    HOST = URL.split(':')[0]
    PORT = URL.split(':')[1]
    SCHEMA= os.getenv('MEDICAL_RECORDS_SCHEMA')
    print('DATABASE :', DATABASE, USER, PW, URL, HOST, PORT)

    con = pg.connect(database =DATABASE, user=USER, password=PW, host=HOST, port=PORT)
    c= con.cursor()
    print("connect success")
    print("/////////////////////////////////////////////////////////////////////////")
    return SCHEMA, HOST, c 

# sql 불러오고 저장하기 
def save_query( SCHEMA, cohort_target, cohort_control, sql, c):
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
    m1= pd.DataFrame.from_records(m1)
    print('save sql success')
    return m1

#count N--
def count_crp_esr(df):
    all_n = df['subject_id'].nunique()
    condition= (df['cohort_type']=='T') & (df['measurement_type']=='CRP')
    CRP_t_n= df.loc[ condition,['subject_id']].nunique()
    condition= (df['cohort_type']=='T') & (df['measurement_type']=='ESR')
    ESR_t_n= df.loc[ condition,['subject_id']].nunique()
    condition= (df['cohort_type']=='C') & (df['measurement_type']=='CRP')
    CRP_c_n= df.loc[ condition,['subject_id']].nunique()
    condition= (df['cohort_type']=='C') & (df['measurement_type']=='ESR')
    ESR_c_n= df.loc[ condition,['subject_id']].nunique()
    return all_n, CRP_t_n , ESR_t_n, CRP_c_n, ESR_c_n

def change_str_date(dm):
    dm['drug_exposure_end_date']=dm['drug_exposure_end_date'].map(lambda x:datetime.strptime(x, '%Y-%m-%d'))
    dm['drug_exposure_start_date'] = dm['drug_exposure_start_date'].map(lambda x:datetime.strptime(x, '%Y-%m-%d'))
    dm['measurement_date'] = dm['measurement_date'].map(lambda x:datetime.strptime(x, '%Y-%m-%d'))
    print('finish change_str_date')


def extract_number(df):
    dose_list= df['Name'].apply(lambda x: re.findall(r'\d*\.\d+|\d+', str(x)))
    metformin_dose=[]
    for i in dose_list:
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
    return metformin_dose 

      # convert to 1 or 0 
def drug_classification(df):
    PS_1st = pd.DataFrame(columns=['subject_id','SU', 'alpha', 'dpp4i', 'gnd', 'metformin', 'sglt2', 'tzd'])
    columns =['SU', 'alpha', 'dpp4i', 'gnd', 'metformin', 'sglt2', 'tzd']
    for column_index in df.columns:
        if column_index in columns:
            PS_1st[column_index]= df[column_index]
            PS_1st[column_index] = np.where(df[column_index]>1, 1,0)
        else: 
            PS_1st[column_index]=0
    PS_1st['subject_id']=df['subject_id']        
    return PS_1st

