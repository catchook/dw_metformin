## 
import os 
import pandas as pd 
import numpy as np 
import re
import psycopg2 as pg
from logging.config import dictConfig
import logging
from datetime import date, time, datetime, timedelta
from psmpy import PsmPy
from psmpy.functions import cohenD
from psmpy.plotting import *
import seaborn as sns
import statsmodels.formula.api as smf
import statsmodels.api as sm 
import matplotlib.pyplot as plt
import math
from scipy import stats
import DW_function as ff
# #rpy2
import rpy2
from rpy2.robjects.packages import importr
import rpy2.robjects as r
import rpy2.robjects.pandas2ri as pandas2ri
pandas2ri.activate()
from rpy2.robjects import Formula

# import rpy2's package module
import rpy2.robjects.packages as rpackages
from rpy2.robjects.conversion import localconverter
# rpy2
base = importr('base')
utils = importr('utils')
utils= rpackages.importr('utils')
utils.install_packages('MatchIt')
utils.install_packages('stats')
statss= importr('stats')
matchit=importr('MatchIt')


# 1. Simpliyfy N 
# [1/3] Simplify N: only who got ESR, CRP
# [2/3] simplify N: measurement in drug_Exposure
# [3/3] simplify N: ALL: 3 ingredient out, target: not metformin out 

class Simplify:
    def __init__(self, data, t1):
        self.data = data
        self.t1 = t1
    def Pair(data): #(data['measurement_type'].isin(lists)) 제거
        condition= (data['measurement_date'] < data['cohort_start_date']) 
        before  = data.loc[condition, ['subject_id','measurement_type','measurement_date','cohort_type','value_as_number']]
        before.dropna(inplace=True)
        before['row'] = before.sort_values(['measurement_date'], ascending =False).groupby(['subject_id', 'measurement_type']).cumcount()+1
        condition = before['row']== 1
        before = before.loc[condition,['subject_id','measurement_type','measurement_date','cohort_type','value_as_number']]
        before.drop_duplicates(inplace= True)
        print("before")
        before.info()
        condition= ( data['measurement_date'] >= data['cohort_start_date'] )
        after = data.loc[condition, :]
        after.dropna(inplace=True)
        after.drop_duplicates(inplace =True)
        print("pair::: join start")
        pair = pd.merge(before, after, on=['subject_id','measurement_type','cohort_type'], how ='inner', suffixes=('_before','_after'))
        pair=ff.delete_none(pair)
        pair.drop_duplicates(inplace =True)
        print("check pair")
        pair.info()
############## delete intermediate file #################
        del before
        del after        
#########################################################        
        return pair
    def Exposure(data):
        condition = (data['drug_exposure_start_date'] <= data['measurement_date_after']) & (data['measurement_date_after'] <= data['drug_exposure_end_date'])
        exposure = data.loc[condition,:]
        exposure=ff.delete_none(exposure)
        exposure.drop_duplicates(inplace =True)
        return exposure 
    def Ruleout(data, t1):
        ## Step3 : 3 ingredient out (control, target 공통) &  target = metformin 포함 한 것만 추출 
        exposure=Simplify.Exposure(data)
        exposure['drug_concept_id'] = exposure['drug_concept_id'].astype(int)
        dc = pd.merge(exposure,  t1[['drug_concept_id','Name','type1','type2']], on= 'drug_concept_id', how = 'left')
        dc.drop_duplicates(inplace=True)
        ## new column: ingredient_count; count typ
        dc2= dc.melt(id_vars=['subject_id','measurement_type','measurement_date_after','cohort_type'], value_vars=['type1','type2'], value_name ='type')
        dc2.dropna(inplace=True)
        dc2= dc2.groupby(['subject_id','measurement_type','measurement_date_after','cohort_type'])['type'].agg( lambda x:list(set(x))).reset_index() ##list({k:None for k in x}.keys())
        dc2['ingredient_count'] = dc2['type'].apply(lambda x: len(set(x)))
        dc2['metformin_count'] = dc2['type'].apply(lambda x: x.count("metformin"))
        
        condition = (dc2['ingredient_count'] >= 3) 
        dc2= dc2.loc[~condition, :]
        condition = ((dc2['cohort_type']=='T')| (dc2['cohort_type']==0)) & (dc2['metformin_count']==0) ##따옴표를 없앰, 에러시 "0"으로 대체 
        dc2= dc2.loc[~condition, :]
        
        dc2['drug_group'] =dc2['type'].apply(lambda x:'/'.join(map(str, x)))
        dc2['row'] = dc2.sort_values(['measurement_date_after'], ascending =True).groupby(['subject_id', 'measurement_type']).cumcount()+1
        dc2['rrow'] = dc2.sort_values(['measurement_date_after'], ascending =False).groupby(['subject_id', 'measurement_type']).cumcount()+1
        print(" before select certain columns , step: ingredient--dc2 ")
        print(dc2.head())
        dc2 = dc2[['subject_id', 'measurement_type', 'measurement_date_after', 'drug_group','row','rrow']]
        dc2.drop_duplicates(inplace=True)
        print(" after drop_duplicates, step: ingredient--dc2 ")
        print(dc2.head())
        final = pd.merge(exposure[['subject_id','measurement_type','cohort_type','measurement_date_before', 'value_as_number_before',
                'measurement_date_after','value_as_number_after', 'dose_type']], dc2, on=['subject_id', 'measurement_type','measurement_date_after'], how ='inner')
        final.drop_duplicates(inplace=True)
        final=ff.delete_none(final)
        return final 
# 2. Add PS matching data
# [1/3] 1st PS matching: drug history (adm drug group) 
# [2/3] 1st PS matching: BUN, Creatinine
# [3/3] 2nd PS matching: disease history 

class Drug:
    def __init__(self, data, t1):
        self.data = data
        self.t1 = t1
    def dose(data, t1):
        data['drug_concept_id'] = data['drug_concept_id'].astype(int)
        data['quantity'] = data['quantity'].astype(float)
        data['days_supply'] = data['days_supply'].astype(float)
        t1['drug_concept_id'] = t1['drug_concept_id'].astype(int)
        dc = pd.merge(data[['subject_id', 'drug_concept_id','quantity', 'days_supply','cohort_type']], 
            t1[['drug_concept_id','Name','type1','type2']], how = 'left', on= "drug_concept_id")
        dc.drop_duplicates(inplace=True)
    # dose group 
        metformin_dose =ff.extract_number(dc)
        dc['metformin_dose']= metformin_dose
        dc['days_supply'] = dc['days_supply'].round(2)
        dc['days_supply'] = dc['days_supply'].replace(0.0, 1.0 )
        dc = dc.astype({'quantity':'float', 'days_supply':'float'})
    # new column: dose_group: high(over 1,000 mg/day) / low  
        dc['dose_type']=dc.apply(lambda x:'high' if (x['metformin_dose']* x['quantity']/x['days_supply']>=1000.0) else 'low', axis=1)
        return dc

    def drug_history(data, t1):
    # [1/3] 1st PS matching: drug history (adm drug group) 
     #1) 1st PS matching: drug classification
        dc = Drug.dose(data, t1)
        dc2 = dc.melt(id_vars=['subject_id'], value_vars=['type1','type2'], value_name='type')
        dc2.dropna(inplace=True)
        dc2 = dc2.groupby('subject_id')['type'].agg(lambda x: ",".join(list(set(x)))).reset_index()
        PS = pd.get_dummies(dc2.set_index('subject_id').type.str.split(',\s*', expand=True).stack()).groupby(level='subject_id').sum().astype(int).reset_index()
        # condition= PS_1st['error']==0
        # PS_1st= PS_1st.loc[condition,:].drop(columns='error')
        PS_1st = pd.DataFrame(columns=['subject_id','SU', 'alpha', 'dpp4i', 'gnd', 'metformin', 'sglt2', 'tzd'])
        columns = ['SU', 'alpha', 'dpp4i', 'gnd', 'metformin', 'sglt2', 'tzd']
        for index in columns:
            if index in PS.columns:
                PS_1st[index] = PS[index]
            else:
                PS_1st[index] = 0
        PS_1st['subject_id']= PS['subject_id']
        print("PS_1st columns are {}".format(PS_1st.columns))
 ####### delete intermediate data
        del dc
        del dc2
        del PS
 ###################################       
        return PS_1st     
    def renal(data):
        # [2/3] 1st PS matching: BUN, Creatinine
        # before cr, bun 추출 
        print("renal:: filter data")
        condition= (data['measurement_date'] < data['cohort_start_date']) & (data['measurement_type'].isin(['Creatinine', 'BUN']))
        before  = data.loc[condition, ['subject_id','measurement_type','measurement_date','value_as_number', 'age','gender']]
        before.dropna(inplace=True)
        # select latest measurement
        print("renal:: new columns row, select row ==1, latest data")
        before['row'] = before.sort_values(['measurement_date'], ascending =False).groupby(['subject_id', 'measurement_type']).cumcount()+1
        condition = before['row']== 1
        before = before.loc[condition,['subject_id','measurement_type','value_as_number', 'age', 'gender']]
        print("renal:: start pivot")
        before_pivot = before.pivot(values ='value_as_number', index='subject_id', columns ='measurement_type')
        before_pivot.columns = before_pivot.columns.values
        before_pivot.reset_index(level=0, inplace =True)  
        #before_pivot.fillna(1, inplace= True)  
        print("this is renal  table ")
        print(before_pivot.head())
        # #null, na 값은 1, 10 로 대체
        before_pivot['BUN'].fillna(10, inplace = True)
        before_pivot['Creatinine'].fillna(1, inplace = True)
        ## eGFR 계산 
        print("renal:: start egfr")
        before2 = pd.merge(before_pivot, before[['subject_id', 'age', 'gender']], on ='subject_id', how='inner')
        before2['gender'] = before2['gender'].replace(['M', 'F'], [0.742, 1])
        before2['egfr'] = round(175 * pow(before2['Creatinine'], -1.154) * pow(before2['age'], -0.203) * before2['gender'], 2 )
        before = before2[['subject_id', 'BUN','Creatinine', 'egfr']]
        print("describe renal::")
        before.info()
 ####### delete intermediate data
        del before_pivot
        del before2
 ###################################    
        return before
    
    def disease_history(data, data22, data33):
        data2 = data[['subject_id', 'condition_type']].drop_duplicates()
        dc = data2.groupby('subject_id')['condition_type'].agg(lambda x: ",".join(list(set(x)))).reset_index()
        dc.set_index('subject_id', inplace = True)
        PS = pd.get_dummies(dc['condition_type'].str.split(',\s*', expand=True).stack()).groupby(level='subject_id').sum().astype(int).reset_index()
        # condition= PS_1st['error']==0
        # PS_1st= PS_1st.loc[condition,:].drop(columns='error')
        PS_1st = pd.DataFrame(columns=['subject_id','MI', 'HF', 'PV', 'CV', 'Dementia', 'CPD', 'Rheuma', 'PUD', 'MLD', 'D', 'DCC', 'HP', 'Renal', 'M', 'MSLD', 'MST', 'AIDS', 'HT', 'HL', 'Sepsis', 'HTT'])
        columns = ['MI', 'HF', 'PV', 'CV', 'Dementia', 'CPD', 'Rheuma', 'PUD', 'MLD', 'D', 'DCC', 'HP', 'Renal', 'M', 'MSLD', 'MST', 'AIDS', 'HT', 'HL', 'Sepsis', 'HTT']
        for index in columns:
            if index in PS.columns:
                PS_1st[index] = PS[index]
            else:
                PS_1st[index] = 0
        PS_1st['subject_id']= PS['subject_id']
        print("History columns are {}".format(PS_1st.columns))
        ## 고혈압 데이터랑 합치기 
        d_hp = PS_1st.merge(data22, on = 'subject_id', how ='left')
        d_hp['hypertension_drug'] = d_hp['hypertension_drug'].fillna(0)
        ## 합친 데이터가 0 이상이면 1로 변경 
        d_hp['HT2'] = d_hp['HT'] + d_hp['hypertension_drug']
        d_hp['HT2'].mask(d_hp['HT2'] > 0, 1, inplace = True)
        ## 고지혈증 데이터랑 합치기 
        d_hphl = d_hp.merge(data33, on ='subject_id', how='left')
        d_hphl['hyperlipidemia_drug'] = d_hphl['hyperlipidemia_drug'].fillna(0)
        ## 합친 데이터가 0 이상이면 1로 변경 
        d_hphl['HL2'] = d_hphl['HL'] + d_hphl['hyperlipidemia_drug']
        d_hphl['HL2'].mask(d_hphl['HL2'] > 0, 1, inplace = True)
        PS_1st = d_hphl[['subject_id','MI', 'HF', 'PV', 'CV', 'Dementia', 'CPD', 'Rheuma', 'PUD', 'MLD', 'D', 'DCC', 'HP', 'Renal', 'M', 'MSLD', 'MST', 'AIDS', 'HT2', 'HL2', 'Sepsis', 'HTT']]
 ############## delete intermediate data
        del data2
        del dc
        del PS
        del d_hp
        del d_hphl
        del data22
        del data33 
 ######################################                   
        return PS_1st     
    def cci(data):
        data['cci'] = data['MI'] + data['HF'] + data['PV'] + data['CV'] + data['Dementia'] + data['CPD'] + data['Rheuma'] + data['PUD'] + data['MLD'] + data['D'] + data['DCC']*2 + data['HP']*2 + data['Renal']*2 + data['M']*2 + data['MSLD']*3 + data['MST']*6 + data['AIDS']*6
        data = data[['subject_id', 'cci']]
        print("describe cci:::")
        data.info()
        return data
    
    
class Stats:
    def __init__(self, data):
        self.data = data
    def preprocess(data):
        condition = data['value_as_number'].eq('None') 
        data = data.loc[~condition, :]
        data.dropna(inplace=True)
        data=data.replace([np.inf, -np.inf], np.nan).dropna()
        data['cohort_type']=data['cohort_type'].replace('T',0)
        data['cohort_type']=data['cohort_type'].replace('C',1)
        data['gender']=data['gender'].replace('F',1)
        data['gender']=data['gender'].replace('M',0)    
        data['value_as_number'] = data['value_as_number'].astype(float)
        return data
    def psmatch(data,condition ,ratio_n):           
        func_seed = r.r['set.seed']
        func_matchit = r.r['matchit']
        func_summary = r.r['summary']
        func_seed(1)
        with localconverter(r.default_converter + pandas2ri.converter):
            r_data = r.conversion.py2rpy(data)
        r_out1=func_matchit(formula = Formula('cohort_type ~ Creatinine+ egfr + BUN +gender + age+ SU + alpha+ dpp4i + gnd + sglt2 +tzd + MI + HF +PV + CV + CPD +RD+ PUD +MLD + DCC + HP + Renal + MSLD + AIDS + HT+ HL + Sepsis+ HTT'), data = r_data, method ='nearest', distance ='logit', replace = condition, ratio = ratio_n)
        func_match_data = r.r['match.data']
        summary = func_summary(r_out1)
        m_data = func_match_data(r_out1, data =r_data, distance ='prop.score')
        with localconverter(r.default_converter + pandas2ri.converter):
            pd_m_data = r.conversion.rpy2py(m_data)
            pd_summary = r.conversion.rpy2py(summary)
        for i in pd_summary:
            a=pd_summary[i]
            print(" ")
            print(i)
            print(" ")
            print(a)
        return pd_m_data
    def psmatch2(data,condition ,ratio_n):           
        func_seed = r.r['set.seed']
        func_matchit = r.r['matchit']
        func_seed(1)
        with localconverter(r.default_converter + pandas2ri.converter):
            r_data = r.conversion.py2rpy(data)
        r_out1=func_matchit(formula = Formula('dose_type ~ Creatinine + egfr + BUN +gender + age+ SU + alpha+ dpp4i + gnd + sglt2 +tzd + MI + HF +PV + CV + CPD +RD+ PUD +MLD + DCC + HP + Renal + MSLD + AIDS + HT+ HL + Sepsis+ HTT'), data = r_data, method ='nearest', distance ='logit', replace = condition, ratio = ratio_n)
        func_match_data = r.r['match.data']
        m_data = func_match_data(r_out1, data =r_data, distance ='prop.score')
        with localconverter(r.default_converter + pandas2ri.converter):
            pd_m_data = r.conversion.rpy2py(m_data)
        return pd_m_data    
    # def psmatch3(data,condition ,ratio_n):           
    #     func_seed = r.r['set.seed']
    #     func_matchit = r.r['matchit']
    #     func_seed(1)
    #     with localconverter(r.default_converter + pandas2ri.converter):
    #         r_data = r.conversion.py2rpy(data)
    #     r_out1=func_matchit(formula = Formula('dose_type ~ BUN + Creatinine'), data = r_data, method ='nearest', distance ='logit', replace = condition, ratio = ratio_n)
    #     func_match_data = r.r['match.data']
    #     m_data = func_match_data(r_out1, data =r_data, distance ='prop.score')
    #     with localconverter(r.default_converter + pandas2ri.converter):
    #         pd_m_data = r.conversion.rpy2py(m_data)
    #     return pd_m_data
    # def psmatch4(data,condition ,ratio_n):           
    #     func_seed = r.r['set.seed']
    #     func_matchit = r.r['matchit']
    #     func_seed(1)
    #     with localconverter(r.default_converter + pandas2ri.converter):
    #         r_data = r.conversion.py2rpy(data)
    #     r_out1=func_matchit(formula = Formula('dose_type ~ gender + age+ SU + alpha+ dpp4i + gnd + sglt2 +tzd + MI + HF +PV + CV + CPD +RD+ PUD +MLD + DCC + HP + Renal + MSLD + AIDS + HT+ HL + Sepsis+ HTT'), data = r_data, method ='nearest', distance ='logit', replace = condition, ratio = ratio_n)
    #     func_match_data = r.r['match.data']
    #     m_data = func_match_data(r_out1, data =r_data, distance ='prop.score')
    #     with localconverter(r.default_converter + pandas2ri.converter):
    #         pd_m_data = r.conversion.rpy2py(m_data)
    #     return pd_m_data   
    def test(data): #rate
        func_shapiro = r.r['shapiro.test']
        func_vartest = r.r['var.test']       
        func_ttest=r.r['t.test']
        func_wilcox=r.r['wilcox.test']
        lists = list(data['measurement_type'].drop_duplicates())
        shapiro_pvalue_target=[]
        shapiro_pvalue_control=[]
        var_pvalue=[]
        ttest_F_stat =[]
        ttest_P_value=[]
        wilcox_F_stat =[]
        wilcox_P_value=[]
        condition = data['rate'].eq('None') 
        data = data.loc[~condition, :]
        data = data.dropna()
        for i in lists:
            condition = ((data['cohort_type']==0)|(data['cohort_type']=='T')) & (data['measurement_type']== i )
            target = data.loc[condition, 'rate']
            condition = ((data['cohort_type']==1)|(data['cohort_type']=='C')) & (data['measurement_type']== i )
            control = data.loc[condition, 'rate']           
            target.drop_duplicates(inplace =True)
            control.drop_duplicates(inplace =True)
            with localconverter(r.default_converter + pandas2ri.converter):
                r_target = r.conversion.py2rpy(target)
                r_control = r.conversion.py2rpy(control)

            if ((len(r_target) < 3) | (len(r_control) <3)):
                shapiro_pvalue_target.append(0)
                shapiro_pvalue_control.append(0)
                var_pvalue.append(0)
                ttest_F_stat.append(0)
                ttest_P_value.append(0)
                wilcox_F_stat.append(0)
                wilcox_P_value.append(0)
            else:               
                t_out = func_shapiro(r_target)
                c_out = func_shapiro(r_control)           
                m_out1= func_vartest(r_control, r_target)     
                m_out2= func_ttest(r_control, r_target)
                m_out3= func_wilcox(r_control, r_target)
                shapiro_pvalue_target.append(t_out[1][0])
                shapiro_pvalue_control.append(c_out[1][0])
                var_pvalue.append(m_out1[2][0])
                ttest_F_stat.append(m_out2[0][0])
                ttest_P_value.append(m_out2[2][0])
                wilcox_F_stat.append(m_out3[0][0])
                wilcox_P_value.append(m_out3[2][0])
        df = pd.DataFrame({'type': lists, 'shapiro_pvalue_target' : shapiro_pvalue_target, 'shapiro_pvalue_control': shapiro_pvalue_control, 'var_pvalue' : var_pvalue, 
                           'ttest_F_stat': ttest_F_stat, 'ttest_P_value':ttest_P_value, 'wilcox_F_stat':wilcox_F_stat, 'wilcox_P_value':wilcox_P_value })
        return df 
    def test2(data): #diff
        func_shapiro = r.r['shapiro.test']
        func_vartest = r.r['var.test']       
        func_ttest=r.r['t.test']
        func_wilcox=r.r['wilcox.test']
        lists = list(data['measurement_type'].drop_duplicates())
        shapiro_pvalue_target=[]
        shapiro_pvalue_control=[]
        var_pvalue=[]
        ttest_F_stat =[]
        ttest_P_value=[]
        wilcox_F_stat =[]
        wilcox_P_value=[]
        condition = data['diff'].eq('None') 
        data = data.loc[~condition, :]
        data = data.dropna()
        for i in lists:
            condition = ((data['cohort_type']==0)|(data['cohort_type']=='T')) & (data['measurement_type']== i )
            target = data.loc[condition, 'diff']
            condition = ((data['cohort_type']==1)|(data['cohort_type']=='C')) & (data['measurement_type']== i )
            control = data.loc[condition, 'diff']           
            target.drop_duplicates(inplace =True)
            control.drop_duplicates(inplace =True)
            with localconverter(r.default_converter + pandas2ri.converter):
                r_target = r.conversion.py2rpy(target)
                r_control = r.conversion.py2rpy(control)

            if ((len(r_target) < 3) | (len(r_control) <3)):
                shapiro_pvalue_target.append(0)
                shapiro_pvalue_control.append(0)
                var_pvalue.append(0)
                ttest_F_stat.append(0)
                ttest_P_value.append(0)
                wilcox_F_stat.append(0)
                wilcox_P_value.append(0)
            else:               
                t_out = func_shapiro(r_target)
                c_out = func_shapiro(r_control)           
                m_out1= func_vartest(r_control, r_target)     
                m_out2= func_ttest(r_control, r_target)
                m_out3= func_wilcox(r_control, r_target)
                shapiro_pvalue_target.append(t_out[1][0])
                shapiro_pvalue_control.append(c_out[1][0])
                var_pvalue.append(m_out1[2][0])
                ttest_F_stat.append(m_out2[0][0])
                ttest_P_value.append(m_out2[2][0])
                wilcox_F_stat.append(m_out3[0][0])
                wilcox_P_value.append(m_out3[2][0])
        df = pd.DataFrame({'type': lists, 'shapiro_pvalue_target' : shapiro_pvalue_target, 'shapiro_pvalue_control': shapiro_pvalue_control, 'var_pvalue' : var_pvalue, 
                           'ttest_F_stat': ttest_F_stat, 'ttest_P_value':ttest_P_value, 'wilcox_F_stat':wilcox_F_stat, 'wilcox_P_value':wilcox_P_value })
        return df 
    def dose_test(data): #rate
        lists = list(data['measurement_type'].drop_duplicates())
        # func_shapiro = r.r['shapiro.test']
        func_vartest = r.r['var.test']       
        func_ttest=r.r['t.test']
        func_wilcox=r.r['wilcox.test']
        kolmogorov_pvalue_high=[]
        kolmogorov_pvalue_low=[]
        var_pvalue=[]
        ttest_F_stat =[]
        ttest_P_value=[]
        wilcox_F_stat =[]
        wilcox_P_value=[]
        #condition = data['rate'].eq('None') 
        #data2 = data.loc[~condition, :]
        #data2.dropna(inplace =True)
        for i in lists:
            condition = (data['measurement_type']== i ) & (data['dose_type']== 1)
            high = data.loc[condition, 'rate']
            condition = (data['measurement_type']== i ) & (data['dose_type']== 0)
            low = data.loc[condition, 'rate']           
            #high.drop_duplicates(inplace =True)
            #low.drop_duplicates(inplace =True)
            with localconverter(r.default_converter + pandas2ri.converter):
                r_high = r.conversion.py2rpy(high)
                r_low = r.conversion.py2rpy(low)

            if ((len(high) <= 3) | (len(low)<=3)):
                kolmogorov_pvalue_high.append(0)
                kolmogorov_pvalue_low.append(0)
                var_pvalue.append(0)
                ttest_F_stat.append(0)
                ttest_P_value.append(0)
                wilcox_F_stat.append(0)
                wilcox_P_value.append(0)
            else:               
                h_out = stats.kstest(r_high,'norm')
                l_out = stats.kstest(r_low, 'norm')           
                m_out1= func_vartest(r_high, r_low)     
                m_out2= func_ttest(r_high, r_low)
                m_out3= func_wilcox(r_high, r_low)
                kolmogorov_pvalue_high.append(h_out[1])
                kolmogorov_pvalue_low.append(l_out[1])
                var_pvalue.append(m_out1[2][0])
                ttest_F_stat.append(m_out2[0][0])
                ttest_P_value.append(m_out2[2][0])
                wilcox_F_stat.append(m_out3[0][0])
                wilcox_P_value.append(m_out3[2][0])
        df = pd.DataFrame({'type': lists, 'kolmogorov_pvalue_high' : kolmogorov_pvalue_high, 'kolmogorov_pvalue_low': kolmogorov_pvalue_low, 'var_pvalue' : var_pvalue, 
                           'ttest_F_stat': ttest_F_stat, 'ttest_P_value':ttest_P_value, 'wilcox_F_stat':wilcox_F_stat, 'wilcox_P_value':wilcox_P_value })
        return df
     
    def dose_test2(data): #diff
        lists = list(data['measurement_type'].drop_duplicates())
        # func_shapiro = r.r['shapiro.test']
        func_vartest = r.r['var.test']       
        func_ttest=r.r['t.test']
        func_wilcox=r.r['wilcox.test']
        kolmogorov_pvalue_high=[]
        kolmogorov_pvalue_low=[]
        var_pvalue=[]
        ttest_F_stat =[]
        ttest_P_value=[]
        wilcox_F_stat =[]
        wilcox_P_value=[]
        # condition = data['diff'].eq('None') 
        # data2 = data.loc[~condition, :]
        # data2.dropna(inplace =True)
        for i in lists:
            condition = (data['measurement_type']== i ) & (data['dose_type']== 1)
            high = data.loc[condition, 'diff']
            condition = (data['measurement_type']== i ) & (data['dose_type']== 0)
            low = data.loc[condition, 'diff']           
            #high.drop_duplicates(inplace =True)
            #low.drop_duplicates(inplace =True)
            with localconverter(r.default_converter + pandas2ri.converter):
                r_high = r.conversion.py2rpy(high)
                r_low = r.conversion.py2rpy(low)

            if ((len(high) <= 3) | (len(low)<=3)):
                kolmogorov_pvalue_high.append(0)
                kolmogorov_pvalue_low.append(0)
                var_pvalue.append(0)
                ttest_F_stat.append(0)
                ttest_P_value.append(0)
                wilcox_F_stat.append(0)
                wilcox_P_value.append(0)
            else:               
                h_out = stats.kstest(r_high,'norm')
                l_out = stats.kstest(r_low, 'norm')           
                m_out1= func_vartest(r_high, r_low)     
                m_out2= func_ttest(r_high, r_low)
                m_out3= func_wilcox(r_high, r_low)
                kolmogorov_pvalue_high.append(h_out[1])
                kolmogorov_pvalue_low.append(l_out[1])
                var_pvalue.append(m_out1[2][0])
                ttest_F_stat.append(m_out2[0][0])
                ttest_P_value.append(m_out2[2][0])
                wilcox_F_stat.append(m_out3[0][0])
                wilcox_P_value.append(m_out3[2][0])
        df = pd.DataFrame({'type': lists, 'kolmogorov_pvalue_high' : kolmogorov_pvalue_high, 'kolmogorov_pvalue_low': kolmogorov_pvalue_low, 'var_pvalue' : var_pvalue, 
                           'ttest_F_stat': ttest_F_stat, 'ttest_P_value':ttest_P_value, 'wilcox_F_stat':wilcox_F_stat, 'wilcox_P_value':wilcox_P_value })
        return df
    def describe(data, group):
        lists = list(data['measurement_type'].drop_duplicates())
        data = data.rename(columns={'value_as_number_before':'baseline', 'value_as_number_after': 'F/U'})
        results=[]
        for i in lists:
            print(i)
            condition = data['measurement_type']== i 
            data2 = data.loc[condition, ['measurement_type','baseline', 'F/U','diff','rate'] ]
            columns =['baseline', 'F/U','diff','rate']
            for j in columns:
                a= data2[j].describe().to_frame().transpose()
                a['measurement_type']= i
                results.append(a)
        result = pd.concat(results, axis=0)
        result['group']= group
        return  result
    def describe_pvalue(data, lists):
        tstats=[]
        pvalues=[]
        for i in lists:
            control = data.loc[data['cohort_type']==1, i]
            target = data.loc[data['cohort_type']==0, i]
            tstat, pvalue, df = sm.stats.ttest_ind(control, target)
            tstats.append(tstat)
            pvalues.append(pvalue)
        df = pd.DataFrame({'type': lists, 'tstat': tstats, 'pvalue':pvalues})
        return df 
    def renal_describe(data, steps):
        condition = data['cohort_type']== 0
        target = data.loc[condition,:]
        control = data.loc[~condition,:]
        t_1 = target['egfr'].describe().to_frame().transpose()
        t_2 = target['Creatinine'].describe().to_frame().transpose()
        t_3 = target['BUN'].describe().to_frame().transpose()
        t_ = pd.concat([t_1, t_2, t_3], axis=0)
        t_['measurement_type']= ['egfr', 'Creatinine', 'BUN']
        t_['group']= 'target'
        c_1 = control['egfr'].describe().to_frame().transpose()
        c_2 = control['Creatinine'].describe().to_frame().transpose()
        c_3 = control['BUN'].describe().to_frame().transpose()
        c_ = pd.concat([c_1, c_2, c_3], axis=0)
        c_['measurement_type']= ['egfr', 'Creatinine', 'BUN']
        c_['group']= 'control'
        df = pd.concat([t_, c_], axis=0)
        df['step'] = steps
        return df  
        

    # def dose_preprocess(data):
    #     ## T (high dose) vs Control  
    #     condition = (data['dose_type'] =='high') & ((data['cohort_type']==0)|(data['cohort_type']=='T'))
    #     high_list = data.loc[condition, 'subject_id'].drop_duplicates()
    #     high = data.loc[condition, :].drop_duplicates()
    #     condition = ((data['cohort_type']==1)|(data['cohort_type']=='C'))
    #     control = data.loc[condition, :].drop_duplicates()
    #     sub1 = pd.concat([high, control]).drop_duplicates()
    #     condition = (data['subject_id'].isin(high_list)==False) & ((data['cohort_type']== 0)|(data['cohort_type']=='T'))
    #     low = data.loc[condition,:].drop_duplicates()
    #     sub2 = pd.concat([low, control]).drop_duplicates()
    #     return sub1, sub2 
    def drug_preprocess(data):
        lists =['metformin', 'SU', 'alpha', 'dpp4i', 'gnd', 'sglt2', 'tzd']
        data = data.rename(columns={'value_as_number_before':'baseline', 'value_as_number_after': 'F/U'})
        condition = data['rate'].eq('None') 
        data = data.loc[~condition, :]
        condition = data['baseline'].eq('None') 
        data = data.loc[~condition, :]
        condition = data['F/U'].eq('None') 
        data = data.loc[~condition, :]
        condition = data['drug_group'].eq('None') 
        data = data.loc[~condition, :]
        data = data.dropna()
        results=[]
        for i in lists:
            if i =='metformin': 
                condition = ((data.cohort_type ==0) | (data.cohort_type =='T')) & (data.drug_group.isin(['metformin']))
                j = data.loc[condition,['subject_id','drug_group','measurement_type', 'baseline','F/U','rate','diff']]
                j=j.drop_duplicates()
                results.append(j)
            else:   
                condition = ((data.cohort_type ==0) | (data.cohort_type =='T')) & (data['drug_group'].str.contains(i))
                j = data.loc[condition,['subject_id','drug_group','measurement_type', 'baseline','F/U','rate','diff']]
                j=j.drop_duplicates()
                results.append(j)
        return results 
    def pairedtest (data):
        lists = list(data['measurement_type'].drop_duplicates())
        data = data.rename(columns={'value_as_number_before':'baseline', 'value_as_number_after': 'F/U'})
        func_shapiro = r.r['shapiro.test']
        func_vartest = r.r['var.test']
        func_ttest=r.r['t.test']
        func_wilcox=r.r['wilcox.test']
        data['baseline'] = data['baseline'].astype(float)
        data['F/U'] = data['F/U'].astype(float)
        shapiro_pvalue_post=[]
        shapiro_pvalue_pre=[]
        var_pvalue=[]
        ttest_F_stat =[]
        ttest_P_value=[]
        wilcox_F_stat =[]
        wilcox_P_value=[]      
        condition = data['F/U'].eq('None') 
        data = data.loc[~condition, :]
        condition = data['baseline'].eq('None') 
        data = data.loc[~condition, :]
        data = data.dropna()        
        for j in lists:
            condition = data['measurement_type']== j
            post = data.loc[condition, 'F/U']
            pre = data.loc[condition, 'baseline']
            post = post.drop_duplicates()
            pre= pre.drop_duplicates()
            if ((len(post) < 3) | (len(pre)<3)):
                shapiro_pvalue_post.append(0)
                shapiro_pvalue_pre.append(0)
                var_pvalue.append(0)
                ttest_F_stat.append(0)
                ttest_P_value.append(0)
                wilcox_F_stat.append(0)
                wilcox_P_value.append(0)
            else:
                with localconverter(r.default_converter + pandas2ri.converter):
                    r_post = r.conversion.py2rpy(post)
                    r_pre = r.conversion.py2rpy(pre)                
                shapiro_result_post= func_shapiro(r_post)
                shapiro_result_pre= func_shapiro(r_pre)
                var_result = func_vartest(r_post, r_pre)
                ttest_result= func_ttest(r_post, r_pre)
                wilcox_result= func_wilcox(r_post, r_pre)
                shapiro_pvalue_post.append(shapiro_result_post[1][0])
                shapiro_pvalue_pre.append(shapiro_result_pre[1][0])
                var_pvalue.append(var_result[2][0])
                ttest_F_stat.append(ttest_result[0][0])
                ttest_P_value.append(ttest_result[2][0])
                wilcox_F_stat.append(wilcox_result[0][0])
                wilcox_P_value.append(wilcox_result[2][0])
        df = pd.DataFrame({'type': lists, 'shapiro_pvalue_post' : shapiro_pvalue_post, 'shapiro_pvalue_pre': shapiro_pvalue_pre, 'var_pvalue' : var_pvalue, 
                           'ttest_F_stat': ttest_F_stat, 'ttest_P_value':ttest_P_value, 'wilcox_F_stat':wilcox_F_stat, 'wilcox_P_value':wilcox_P_value })
        return df
    

# 5. Add HealthScore data:
# [1/3] healthscore variabels: before, after 
# [2/3] calculate healthscore 

#     def psmatch(data):
# <<<<<<< HEAD
#         data = Stats.preprocess(data)
#         after_pss=[]
#         lists=['CRP','ESR']
#         for i in lists:         
#             condition = data['measurement_type']== i
#             data = data.loc[condition, ['subject_id', 'measurement_type', 'cohort_type',
# 'SU', 'alpha', 'dpp4i', 'gnd', 'sglt2', 'tzd', 'BUN', 'Creatinine','gender', 'age']]
#             psm = PsmPy(data, treatment='cohort_type', indx='subject_id', exclude=['measurement_type'])
# =======
#         dd = Stats.preprocess(data)
#         after_pss=[]
#         lists=['CRP','ESR']
#         for i in lists:         
#             condition = dd['measurement_type']== i
#             ddd = dd.loc[condition, ['subject_id', 'measurement_type', 'cohort_type','SU', 'alpha', 'dpp4i', 'gnd', 'sglt2', 'tzd', 'BUN', 'Creatinine','gender', 'age']]
#             psm = PsmPy(ddd, treatment='cohort_type', indx='subject_id', exclude= ['measurement_type'])
# >>>>>>> 2b830ff636b935f1fc5cbd505e353fdd6cbb128d
#             psm.logistic_ps(balance = False)
#             # balance= True, This tells PsmPy to create balanced samples when fitting the logistic regression model. 
#             psm.knn_matched(matcher= 'propensity_logit' , replacement=False, caliper=None)
#             # replacement-false(default); determines whether macthing will happen with or without replacement, when replacement is false matching happens 1:1
#             # caliper - None (default), user can specify caliper size relative to std. dev of the control sample, restricting neighbors eligible to match
# <<<<<<< HEAD
#             psm.plot_match(Title='Matching Result', Ylabel='Number of patients', Xlabel= 'propensity_logit' ,names = ['target', 'control'],save=True)
#             psm.effect_size_plot(save=True)
# =======
#             # psm.plot_match(Title='Matching Result', Ylabel='Number of patients', Xlabel= 'propensity_logit' ,names = ['target', 'control'],save=True)
#             # psm.effect_size_plot(save=True)
# >>>>>>> 2b830ff636b935f1fc5cbd505e353fdd6cbb128d
#             after_ps = psm.df_matched
#             after_ps['measurement_type'] = i
#             after_pss.append(after_ps)
#         after_ps = pd.concat([after_pss[0], after_pss[1]])
#         after_ps= after_ps.drop_duplicates()
# <<<<<<< HEAD
#         return after_ps 
# =======
 #rate변수 합치기. !!!!!!!!!!!!!!!!!!!!!!!
#         after_ps = pd.merge(after_ps, dd[['subject_id','measurement_type','rate','value_as_number_before','value_as_number_after']], on=['subject_id', 'measurement_type'], how= 'left') 
#         after_ps=after_ps.replace([np.inf, -np.inf], np.nan).dropna()
#         return after_ps
#     def psmatch2(data):
#         dd = Stats.preprocess(data)
#         after_pss2=[]
#         lists=['CRP','ESR']
#         for i in lists:
#           condition = dd['measurement_type']== i
#           ddd = dd.loc[condition, ['subject_id', 'measurement_type', 'cohort_type','SU', 'alpha', 'dpp4i', 'gnd', 'sglt2', 'tzd', 'BUN', 'Creatinine','gender', 'age']]
#           condition = ddd['cohort_type'] ==1
#           target = ddd.loc[condition, :]
#           condition = ddd['cohort_type'] ==0
#           control = ddd.loc[condition, :]
#           if (len(ddd)==0 )| (len(control) ==0) | (len(target)==0):
#             print("empty types: "+i)
#             after_ps2 = pd.DataFrame()
#             after_pss2.append(after_ps2)
#           else:
#             psm = PsmPy(ddd, treatment='cohort_type', indx='subject_id', exclude= ['measurement_type'])
#             psm.logistic_ps(balance = False)
#             # balance= True, This tells PsmPy to create balanced samples when fitting the logistic regression model. 
#             psm.knn_matched(matcher= 'propensity_logit' , replacement=False, caliper=None)
#             # replacement-false(default); determines whether macthing will happen with or without replacement, when replacement is false matching happens 1:1
#             # caliper - None (default), user can specify caliper size relative to std. dev of the control sample, restricting neighbors eligible to match
#             # psm.plot_match(Title='Matching Result', Ylabel='Number of patients', Xlabel= 'propensity_logit' ,names = ['target', 'control'],save=True)
#             # psm.effect_size_plot(save=True)
#             after_ps2 = psm.df_matched
#             after_ps2['measurement_type'] = i
#             after_ps2= after_ps2.drop_duplicates()
#             after_ps2 = pd.merge(after_ps2, dd[['subject_id','measurement_type','rate','value_as_number_before','value_as_number_after']], on=['subject_id', 'measurement_type'], how= 'left') 
#             after_pss2.append(after_ps2)

#         after_ps3= pd.concat([after_pss2[0], after_pss2[1]])
#         after_ps3= after_ps3.drop_duplicates()
#         after_ps3=after_ps3.replace([np.inf, -np.inf], np.nan).dropna()
#         return after_ps3
    # def png(data, variable, step):
    #     data = Stats.preprocess(data)
    #     condition = ((data['cohort_type']== 'T') | (data['cohort_type']== 1)) & (data['measurement_type']== variable)
    #     target = data.loc[condition,:]
    #     condition = ((data['cohort_type']== 'C') | (data['cohort_type']== 0)) & (data['measurement_type']== variable)
    #     control = data.loc[condition,:]
    #     fig, axes = plt.subplots(2, 5, figsize=(18, 10))
    #     X= ['gender', 'age', 'BUN', 'Creatinine' ,'SU', 'alpha', 'dpp4i', 'gnd', 'sglt2', 'tzd'] 
    #     for i, col in enumerate(X ,1):
    #         plt.subplot(2,5,i)
    #         sns.distplot(control[col],color ='green')
    #         sns.distplot(target[col],color='#B53691')
    #         fig.suptitle( step+' PSmatching  - 2 x 5 axes Box plot with all covariate' )
    #     plt.show()
    #     plt.savefig(step+'_ps.png', dpi=300)
#     def ttest(data):
#         ## all 
# <<<<<<< HEAD
#         after_ps = Stats.psmatch(data)
# =======
#         #after_ps = Stats.psmatch(data)
# >>>>>>> 2b830ff636b935f1fc5cbd505e353fdd6cbb128d
#         lists =['CRP','ESR']
#         t_stats=[]
#         p_vals=[]
#         for i in lists:
# <<<<<<< HEAD
#             condition = ((after_ps['cohort_type']== 'T') | (after_ps['cohort_type']== 1)) & (after_ps['measurement_type']== i)
#             target = after_ps.loc[condition,'rate']
#             condition = ((after_ps['cohort_type']== 'C') | (after_ps['cohort_type']== 0)) & (after_ps['measurement_type']== i)
#             control = after_ps.loc[condition,'rate']
# =======
#             condition = ((data['cohort_type']== 'T') | (data['cohort_type']== 1)) & (data['measurement_type']== i)
#             target = data.loc[condition,'rate']
#             condition = ((data['cohort_type']== 'C') | (data['cohort_type']== 0)) & (data['measurement_type']== i)
#             control = data.loc[condition,'rate']
# >>>>>>> 2b830ff636b935f1fc5cbd505e353fdd6cbb128d
#             t_stat, p_val= stats.ttest_ind(target, control, equal_var = True, alternative='two-sided')
#             t_stats.append(t_stat)
#             p_vals.append(p_val)
#         df= pd.DataFrame({'T-test': ['CRP', 'ESR'],
#                             't_stat':t_stats,
#                             'p_val': p_vals })
#         return df








