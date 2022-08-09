import os 
import pandas as pd 
import numpy as np 
import re
import psycopg2 as pg
from logging.config import dictConfig
import logging
import seaborn as sns
import math
from scipy import stats
from datetime import date, time, datetime, timedelta


# dictConfig({
#     'version': 1, #version에는 고정된 값 1로, 다른 값 입력시 value error  발생
#     'formatters':{
#         'default':{
#             'format':'[%(asctime)s] %(message)s',
#         }
#     },
#      'handler':{
#         'file':{
#             'level':'DEBUG',
#             'class': 'logging.logging.FileHandler',
#             'filename':'debug.log',
#             'formatter':'default',
#         },
#      },
#      'root': {
#         'level':'DEBUG',
#         'handlers':['file']
#      }
# })
# healthscore 
model_name= 'linear'
results_root = 'model_info'


# environment variabels, connect 
def connect():
    DATABASE= os.getenv('MEDICAL_RECORDS_DATABASE')
    USER =os.getenv('MEDICAL_RECORDS_USER')
    PW= os.getenv('MEDICAL_RECORDS_PW')
    URL=os.getenv('MEDICAL_RECORDS_URL')
    HOST = URL.split(':')[0]
    PORT = URL.split(':')[1]
    SCHEMA= os.getenv('MEDICAL_RECORDS_SCHEMA')
    print('DATABASE :', DATABASE,'USER:', USER, 'PW:', PW, 'URL;', URL,"host:", HOST, "PORT :", PORT)

    con = pg.connect(database =DATABASE, user=USER, password=PW, host=HOST, port=PORT)
    # con = pg.connect(database='omop',
    #             user='dbadmin',
    #             password = 'INIT@1234',
    #             host='203.245.2.202',
    #             port=5432)
    c= con.cursor()
    print("connect success")
    print("/////////////////////////////////////////////////////////////////////////")
    return SCHEMA, HOST, c 

# sql 불러오고 저장하기 
def save_query( SCHEMA, cohort_target, cohort_control, sql, c):
    sql=re.sub('cdm_hira_2017_results_fnet_v276',SCHEMA+'_results_dq_v276', sql)
   # sql=re.sub('cdm_hira_2017_results_fnet_v276',SCHEMA+'_results_fnet_v276', sql)
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

# def count_crp_esr(df):
#     all_n = df['subject_id'].nunique()
#     condition= (df['cohort_type']=='T') & (df['measurement_type']=='CRP')
#     CRP_t_n= df.loc[condition, 'subject_id'].nunique()
#     condition= (df['cohort_type']=='T') & (df['measurement_type']=='ESR')
#     ESR_t_n= df.loc[condition, 'subject_id'].nunique()
#     condition= (df['cohort_type']=='C') & (df['measurement_type']=='CRP')
#     CRP_c_n= df.loc[condition, 'subject_id'].nunique()
#     condition= (df['cohort_type']=='C') & (df['measurement_type']=='ESR')
#     ESR_c_n= df.loc[condition, 'subject_id'].nunique()
#     return all_n, CRP_t_n , ESR_t_n, CRP_c_n, ESR_c_n

def count_measurement(df, lists):
 #   lists = ['BUN','Triglyceride','SBP','Hb','Glucose_Fasting','Creatinine','HDL','AST','Albumin']
    T_numbers=[]
    C_numbers=[]
    for  i in lists: 
        condition= (df['cohort_type']=='T') & (df['measurement_type']== i )
        n= df.loc[condition, 'subject_id'].nunique()
        T_numbers.append(n)
        condition= (df['cohort_type']=='C') & (df['measurement_type']== i )
        n= df.loc[condition, 'subject_id'].nunique()
        C_numbers.append(n)
    T_count_N = pd.DataFrame(np.array([T_numbers]), columns =lists)
    T_count_N['cohort_type'] ='Target'
    C_count_N = pd.DataFrame(np.array([C_numbers]), columns =lists)
    C_count_N['cohort_type'] ='Control'
    count_N = pd.concat([T_count_N,C_count_N ])
    return   count_N

def delete_none(data):
    condition = (data['value_as_number_before'].eq('None') | data['value_as_number_after'].eq('None'))
    data = data.loc[~condition, :]
    return(data)


def change_str_date(dm):
    dm['drug_exposure_end_date']=dm['drug_exposure_end_date'].map(lambda x:datetime.strptime(str(x), '%Y-%m-%d'))
    dm['drug_exposure_start_date'] = dm['drug_exposure_start_date'].map(lambda x:datetime.strptime(str(x), '%Y-%m-%d'))
    dm['measurement_date'] = dm['measurement_date'].map(lambda x:datetime.strptime(str(x), '%Y-%m-%d'))
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

#       # convert to 1 or 0 
# def drug_classification(df):
#     PS_1st = pd.DataFrame(columns=['subject_id','SU', 'alpha', 'dpp4i', 'gnd', 'metformin', 'sglt2', 'tzd'])
#     columns =['SU', 'alpha', 'dpp4i', 'gnd', 'metformin', 'sglt2', 'tzd']
#     for column_index in df.columns:
#         if column_index in columns:
#             PS_1st[column_index]= df[column_index]
#             PS_1st[column_index] = np.where(df[column_index]>1, 1,0)
#         else: 
#             PS_1st[column_index]=0
#     PS_1st['subject_id']=df['subject_id']        
#     return PS_1st

def transformation_abnormal(data, normal_df, default_concept):
    normal_df = pd.read_csv(os.path.join(results_root,'normal_information.csv'))
    normal_df['concept_id'] = [str(x) for x in normal_df['concept_id']]
    score_Data_df = data.copy()
    default_concept = [str(x) for x in default_concept]
    for concept_id in default_concept:
        temp_norm = normal_df[normal_df['concept_id'] == concept_id].reset_index(drop=True)
        if any(temp_norm['gender']=='A'):
            criteria1 = score_Data_df[concept_id] < temp_norm['nr_min'][0]
            criteria2 = score_Data_df[concept_id] > temp_norm['nr_max'][0]
            criteria3 = [not x for x in (criteria1 + criteria2)]
            nr_min = temp_norm['nr_min'][0]
            nr_max = temp_norm['nr_max'][0]
            mr_min = temp_norm['mr_min'][0]
            mr_max = temp_norm['mr_max'][0]

            score_Data_df.loc[criteria1, concept_id] = (nr_min - score_Data_df.loc[criteria1, concept_id]) / (nr_min - mr_min)
            score_Data_df.loc[criteria2, concept_id] = (score_Data_df.loc[criteria2, concept_id] - nr_max) / (mr_max - nr_max)
            score_Data_df.loc[criteria3, concept_id] = 0
        else:
            for gender in ['M','F']:
                g_temp_norm = temp_norm[temp_norm['gender'] == gender].reset_index(drop=True)
                nr_min = g_temp_norm['nr_min'][0]
                nr_max = g_temp_norm['nr_max'][0]
                mr_min = g_temp_norm['mr_min'][0]
                mr_max = g_temp_norm['mr_max'][0]
                criteria1 = (score_Data_df[concept_id] <= nr_min) & (score_Data_df['gender']==gender)
                criteria2 = (score_Data_df[concept_id] >= nr_max) & (score_Data_df['gender']==gender)
                criteria3 = (score_Data_df[concept_id] > nr_min) & (score_Data_df[concept_id] < nr_max) & (score_Data_df['gender']==gender)
                score_Data_df.loc[criteria1, concept_id] = (nr_min - score_Data_df.loc[criteria1, concept_id]) / (nr_min - mr_min)
                score_Data_df.loc[criteria2, concept_id] = (score_Data_df.loc[criteria2, concept_id] - nr_max) / (mr_max - nr_max)
                score_Data_df.loc[criteria3, concept_id] = 0

        score_Data_df[concept_id] = [1 if x>=1 else x for x in score_Data_df[concept_id]]
    return score_Data_df





def generate_age_delta(data,results_root, selection_concept, model_name, gender):
    r = 1
    data = data[data['gender']==gender]
    info_df = pd.read_csv(os.path.join(results_root, 'age_' + model_name +'_'+gender+ '_weight.csv'))
    info_df['name'] = [str(x) for x in info_df['name']]
    selection_concept = [str(x) for x in selection_concept]
    filter = [True if x in selection_concept else False for x in info_df['name'] ]
    info_df = info_df[filter]
    info_df['weight'] = info_df['coef'] / np.sqrt(sum(np.power(info_df['coef'],2)))
    concept_list = info_df['name'].to_list()

    normal_df = pd.read_csv(os.path.join(results_root,'normal_information.csv'))
    normal_df['concept_id'] = [str(x) for x in normal_df['concept_id']]
    ndata = data[data['gender']==gender][['age','gender']+concept_list]
    score_df = transformation_abnormal(ndata, normal_df, concept_list)
    new_df = score_df[['age', 'gender']]
    sData_df = pd.DataFrame()
    rData_df = pd.DataFrame()
    max_df = pd.DataFrame(index=[0])
    min_df = pd.DataFrame(index=[0])
    for concept_id in concept_list:
        temp_norm = normal_df[normal_df['concept_id'] == concept_id].reset_index(drop=True)
        if any(temp_norm['gender']=='M'):
            temp_norm = temp_norm[temp_norm['gender'] == 'M'].reset_index(drop=True)
        elif any(temp_norm['gender']=='F'):
            temp_norm = temp_norm[temp_norm['gender'] == 'F'].reset_index(drop=True)

        temp = info_df[info_df['name'] == concept_id].reset_index(drop=True)

        mean = temp['mean'][0]
        std = np.sqrt(temp['var'][0])
        weight = temp['weight'][0]
        sdX = (ndata[concept_id] - mean) / std
        sData_df['scale_'+concept_id] = sdX
        # 최대값 구하기
        sdX_max = (temp_norm['mr_max'][0] - mean) / std
        sdX_min = (temp_norm['mr_min'][0] - mean) / std
        max_df[concept_id] = 2*max(sdX_max*weight, sdX_min*weight)
        min_df[concept_id] = min(sdX_max*weight, sdX_min*weight)
        if weight >0:
            ratio = 1 + score_df[concept_id]
        else:
            ratio = 1 - score_df[concept_id]
        rData_df['ratio_'+concept_id] = ratio
        new_df[concept_id] = r * ratio * weight * sdX

    data['delta_age'] = new_df[concept_list].sum(axis=1)
    result = pd.concat([data, sData_df], axis=1)
    result = pd.concat([result, rData_df], axis=1)
    return result

def generate_score(data, results_root):
    f_max_min = pd.read_csv(results_root + '/F_max.csv').sum(axis=1)
    f_max = max(f_max_min)
    f_min = min(f_max_min)
    m_max_min = pd.read_csv(results_root + '/M_max.csv').sum(axis=1)
    m_max = max(m_max_min)
    m_min = min(m_max_min)
    max_data = {'min': [f_min, m_min], 'max': [f_max, m_max], 'gender': ['F', 'M']}
    # max_data = {'min':[-5.069,-5.407], 'max':[25.934,25.076],'gender':['F','M']}
    max_data = pd.DataFrame(max_data)

    score_data = pd.merge(data, max_data, on='gender', how='left')
    # max_data[['gender','age','y_pred','min','max']]
    score_data['score'] = 100 * (1 - (score_data['delta_age'] - score_data['min']) / (score_data['max'] - score_data['min']))
    return score_data

def healthage_score(hc_before):
    for gender in ['F','M']:
        if gender == 'M':
                selection_concept = [3024561, 3040820, 3013682, 3004249, 3007070, 3027484]
        elif gender == 'F':
                selection_concept = [3024561, 3040820, 3013682, 3004249, 3016723, 3007070, 3013721, 3022192]
    #  성별에 따라 건강변화량 계산
        delta_age_mb= generate_age_delta(hc_before, selection_concept, 'M')
        delta_age_fb= generate_age_delta(hc_before, selection_concept, 'F')
        # delta_age_ma= generate_age_delta(hc_after, selection_concept, 'M')
        # delta_age_fa= generate_age_delta(hc_after, selection_concept, 'F')
    #건강변화량 이용하여 스코어 계산
        score_data_mb = generate_score(delta_age_mb, results_root)
        score_data_fb = generate_score(delta_age_fb, results_root)
        # score_data_ma = ff.generate_score(delta_age_ma, results_root)
        # score_data_fa = ff.generate_score(delta_age_fa, results_root)

        score_data = pd.concat([score_data_mb,score_data_fb], axis=0)
    #score_data_after = pd.concat([score_data_ma,score_data_fa], axis=0)
        print('healthscore_complete')
        print(score_data.head())

def make_row(df):
        # new columns: ROWS
    df['ROW'] = df.sort_values(['subject_id','measurement_type','measurement_date_after'], ascending= True).groupby(['subject_id','measurement_type']).cumcount()+1
    ## pick only row =1 
    condition = df['ROW']==1
    df= df.loc[condition,:].drop(columns ='ROW')
    return df

def convert_size(size_bytes):
  import math
  if size_bytes==0:
    return "0B"
  size_name = ("B","KB","MB","GB", "TB", "PB","EB","ZB","YB")
  i = int(math.floor(math.log(size_bytes,1024 )))
  p= math.pow(1024, i)
  s= round(size_bytes/ p,2)
  return "%s %s"% (s, size_name[i])

      # ## 코호트 기간 3개월 미만 인 것 추출. 
    # m1['cohort_end_date'] = pd.to_datetime(m1['cohort_end_date'])
    # m1['cohort_start_date'] = pd.to_datetime(m1['cohort_start_date'])
    # m1['cohort_period'] = m1['cohort_end_date'] - m1['cohort_start_date']
    # print(m1['cohort_period'].describe())