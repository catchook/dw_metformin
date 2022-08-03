import os 
import pandas as pd 
import numpy as np 
import re
import psycopg2 as pg
from logging.config import dictConfig
import logging
from datetime import date, time, datetime, timedelta
import DW_function as ff

# 1. Simpliyfy N 
# [1/3] Simplify N: only who got ESR, CRP
# [2/3] simplify N: measurement in drug_Exposure
# [3/3] simplify N: ALL: 3 ingredient out, target: not metformin out 

class Simplify:
    def __init__(self, data, t1):
        self.data = data
        self.t1 = t1
    def Pair(data):
        condition= (data['measurement_date'] < data['cohort_start_date']) & (data['measurement_type'].isin(['CRP','ESR']))
        before  = data.loc[condition, ['subject_id','measurement_type','measurement_date','cohort_type','value_as_number']]
        before.dropna(inplace=True)
        before['row'] = before.sort_values(['measurement_date'], ascending =False).groupby(['subject_id', 'measurement_type']).cumcount()+1
        condition = data['row']== 1
        before = before.loc[condition,['subject_id','measurement_type','measurement_date','cohort_type','value_as_number']]
        condition= (data['measurement_type'].isin(['CRP','ESR'])) & (data['measurement_date'] >= data['cohort_start_date'] ) & (data['measurement_date'] >= data['drug_exposure_start_date']) & (data['measurement_date'] <= data['drug_exposure_end_date'])
        after = data.loc[condition,['subject_id','cohort_type','measurement_type','measurement_date','value_as_number','drug_concept_id','drug_exposure_start_date','drug_exposure_end_date','cohort_start_date']]
        after.dropna(inplace=True)
        pair = pd.merge(before, after, on=['subject_id','measurement_type','cohort_type'], how ='inner', suffixes=('_before','_after'))
        return pair 
    def Exposure(data):
        pair33 = Simplify.Pair(data)
        pair33['drug_exposure_end_date']=pair33['drug_exposure_end_date'].map(lambda x:datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S'))
        pair33['drug_exposure_start_date'] = pair33['drug_exposure_start_date'].map(lambda x:datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S'))
        pair33['measurement_date_after'] = pair33['measurement_date_after'].map(lambda x:datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S'))
        condition = (pair33['drug_exposure_start_date'] <= pair33['measurement_date_after']) & (pair33['measurement_date_after'] <= pair33['drug_exposure_end_date'])
        exposure = pair33.loc[condition,:]
        return exposure 
    def Ingredient(data, t1):
        ## Step3 : 3 ingredient out (control, target 공통) &  target = metformin 포함 한 것만 추출 
        exposure=Simplify.Exposure(data)
        dc = pd.merge(exposure,  t1[['drug_concept_id','Name','type1','type2']], on= 'drug_concept_id', how = 'left')
        dc.drop_duplicates(inplace=True)
        ## new column: ingredient_count; count typ
        dc2= dc.melt(id_vars=['subject_id','measurement_type','measurement_date_after','cohort_type'], value_vars=['type1','type2'], value_name ='type')
        dc2.dropna(inplace=True)
        dc2= dc2.groupby(['subject_id','measurement_type','measurement_date_after','cohort_type'])['type'].agg( lambda x:list(set(x))).reset_index() ##list({k:None for k in x}.keys())
        dc2['ingredient_count'] = dc2['type'].apply(lambda x: len(set(x)))
        dc2['metformin_count'] = dc2['type'].apply(lambda x: x.count("metformin"))
        condition = (dc2['ingredient_count'] >= 3) | ((dc2['cohort_type']=='T') &(dc2['metformin_count']==0))
        dc2= dc2.loc[~condition, :]
        dc2['drug_group'] =dc2['type'].apply(lambda x:'/'.join(map(str, x)))
        dc2['row'] = dc2.sort_values(['measurement_date_after'], ascending =True).groupby(['subject_id', 'measurement_type']).cumcount()+1
        condition = dc2['row']==1
        dc2 = dc2.loc[condition,['subject_id', 'measurement_type', 'measurement_date_after', 'drug_group']]
        final = pd.merge(exposure[['subject_id','measurement_type','cohort_type','measurement_date_before', 'value_as_number_before',
                'measurement_date_after','value_as_number_after']], dc2, on=['subject_id', 'measurement_type','measurement_date_after'])
        final.drop_duplicaes(inplace=True)
        return final 
    
class Drug:
    def __init__(self, data, t1):
        self.data = data
        self.t1 = t1
    def dose(data, t1):
        dc = pd.merge(data[['subject_id', 'drug_concept_id', 'measurement_date', 'measurement_type','quantity', 'days_supply','cohort_type']], 
            t1[['drug_concept_id','Name','type1','type2']], how = 'left', on= "drug_concept_id")
        dc.drop_duplicates(inplace=True)
    # dose group 
        metformin_dose =ff.extract_number(dc)
        dc['metformin_dose']= metformin_dose
        dc = dc.astype({'quantity':'float', 'days_supply':'float'})
    # new column: dose_group: high(over 1,000 mg/day) / low  
        dc['dose_type']=dc.apply(lambda x:'high' if (x['metformin_dose']* x['quantity']/x['days_supply']>=1000.0) else 'low', axis=1)
        return dc
# 2. Add PS matching data
# [1/3] 1st PS matching: drug history (adm drug group) 
# [2/3] 1st PS matching: BUN, Creatinine
# [3/3] 2nd PS matching: disease history
class PSmatch:
    def __init__(self, final):
        self.final = final 
    def classification(final):
     #1) 1st PS matching: drug classification 
        dc3 = final.melt(id_vars=['subject_id'], value_vars=['type1','type2'], value_name='type')
        dc3.dropna(inplace=True)
        dc3 = dc3.groupby('subject_id')['type'].agg(lambda x: ",".join(list(set(x)))).reset_index()
        PS_1st = pd.get_dummies(dc3.set_index('subject_id').type.str.split(',\s*', expand=True).stack()).groupby(level='subject_id').sum().astype(int).reset_index()
        # condition= PS_1st['error']==0
        # PS_1st= PS_1st.loc[condition,:].drop(columns='error')
        print("PS_1st columns are {}".format(PS_1st.columns))
        del dc3 













