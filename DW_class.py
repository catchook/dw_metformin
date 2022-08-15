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
import matplotlib.pyplot as plt
import math
from scipy import stats
import DW_function as ff
##rpy2
import rpy2
from rpy2.robjects.packages import importr
import rpy2.robjects as r
import rpy2.robjects.pandas2ri as pandas2ri
from rpy2.robjects import Formula
pandas2ri.activate()
# import rpy2's package module
import rpy2.robjects.packages as rpackages
from rpy2.robjects.conversion import localconverter
# rpy2
base = importr('base')
utils = importr('utils')
utils=rpackages.importr('utils')
utils.install_packages('MatchIt')
utils.install_packages('stats')
statss= importr('stats')
matchit=importr('MatchIt')


# 1. Simpliyfy N 
# [1/3] Simplify N: only who got ESR, CRP
# [2/3] simplify N: measurement in drug_Exposure
# [3/3] simplify N: ALL: 3 ingredient out, target: not metformin out 

class Simplify:
    def __init__(self, data, t1, lists):
        self.data = data
        self.t1 = t1
        self.lists= lists
    def Pair(data, lists):
        condition= (data['measurement_date'] < data['cohort_start_date']) & (data['measurement_type'].isin(lists))
        before  = data.loc[condition, ['subject_id','measurement_type','measurement_date','cohort_type','value_as_number']]
        before.dropna(inplace=True)
        before['row'] = before.sort_values(['measurement_date'], ascending =False).groupby(['subject_id', 'measurement_type']).cumcount()+1
        condition = before['row']== 1
        before = before.loc[condition,['subject_id','measurement_type','measurement_date','cohort_type','value_as_number']]
        condition= (data['measurement_type'].isin(lists)) & (data['measurement_date'] >= data['cohort_start_date'] ) & (data['measurement_date'] >= data['drug_exposure_start_date']) & (data['measurement_date'] <= data['drug_exposure_end_date'])
        after = data.loc[condition,['subject_id','cohort_type','measurement_type','measurement_date','value_as_number','drug_concept_id','drug_exposure_start_date','drug_exposure_end_date','cohort_start_date']]
        after.dropna(inplace=True)
        pair = pd.merge(before, after, on=['subject_id','measurement_type','cohort_type'], how ='inner', suffixes=('_before','_after'))
        pair=ff.delete_none(pair)
        return pair 
    def Exposure(data, lists):
        pair33 = Simplify.Pair(data, lists)
        pair33['drug_exposure_end_date']=pair33['drug_exposure_end_date'].map(lambda x:datetime.strptime(str(x), '%Y-%m-%d'))
        pair33['drug_exposure_start_date'] = pair33['drug_exposure_start_date'].map(lambda x:datetime.strptime(str(x), '%Y-%m-%d'))
        pair33['measurement_date_after'] = pair33['measurement_date_after'].map(lambda x:datetime.strptime(str(x), '%Y-%m-%d'))
        condition = (pair33['drug_exposure_start_date'] <= pair33['measurement_date_after']) & (pair33['measurement_date_after'] <= pair33['drug_exposure_end_date'])
        exposure = pair33.loc[condition,:]
        exposure=ff.delete_none(exposure)
        return exposure 
    def Ingredient(data, t1, lists):
        ## Step3 : 3 ingredient out (control, target 공통) &  target = metformin 포함 한 것만 추출 
        exposure=Simplify.Exposure(data, lists)
        exposure['drug_concept_id'] = exposure['drug_concept_id'].astype(int)
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
                'measurement_date_after','value_as_number_after','drug_concept_id']], dc2, on=['subject_id', 'measurement_type','measurement_date_after'])
        final.drop_duplicates(inplace=True)
        final=ff.delete_none(final)
        return final 
# 2. Add PS matching data
# [1/3] 1st PS matching: drug history (adm drug group) 
# [2/3] 1st PS matching: BUN, Creatinine
# [3/3] 2nd PS matching: disease history --중간 보고 이후 작성. 

class Drug:
    def __init__(self, data, t1):
        self.data = data
        self.t1 = t1
    def dose(data, t1):
        data['drug_concept_id'] = data['drug_concept_id'].astype(int)
        t1['drug_concept_id'] = t1['drug_concept_id'].astype(int)
        dc = pd.merge(data[['subject_id', 'drug_concept_id','quantity', 'days_supply','cohort_type']], 
            t1[['drug_concept_id','Name','type1','type2']], how = 'left', on= "drug_concept_id")
        dc.drop_duplicates(inplace=True)
    # dose group 
        metformin_dose =ff.extract_number(dc)
        dc['metformin_dose']= metformin_dose
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
        return PS_1st     
    def buncr(data):
        # [2/3] 1st PS matching: BUN, Creatinine
        condition= (data['measurement_date'] < data['cohort_start_date']) & (data['measurement_type'].isin(['Creatinine', 'BUN' ]))
        before  = data.loc[condition, ['subject_id','measurement_type','measurement_date','value_as_number']]
        before.dropna(inplace=True)
        before['row'] = before.sort_values(['measurement_date'], ascending =False).groupby(['subject_id', 'measurement_type']).cumcount()+1
        condition = before['row']== 1
        before = before.loc[condition,['subject_id','measurement_type','value_as_number']]
        before= before.pivot_table(index=['subject_id'], columns =['measurement_type'], values= 'value_as_number', fill_value =0).reset_index()    
        return before 
class Stats:
    def __init__(self, data):
        self.data = data
    def preprocess(data):
        data= ff.delete_none(data)
        data=data.replace([np.inf, -np.inf], np.nan).dropna()
        # data['cohort_type']=data['cohort_type'].replace('T',1)
        # data['cohort_type']=data['cohort_type'].replace('C',0)
        # data['gender']=data['gender'].replace('F',1)
        # data['gender']=data['gender'].replace('M',0)    
        data['value_as_number_after'] = data['value_as_number_after'].astype(float)
        data['value_as_number_before'] = data['value_as_number_before'].astype(float)
        data['rate']= (data['value_as_number_after'] - data['value_as_number_before']) /data['value_as_number_before'] *100
        return data
    def psmatch(data):           
        func_seed = r.r['set_seed']
        func_matchit = r.r['matchit']
        func_seed(1)
        r_out1=func_matchit(formula = Formula('cohort_type ~BUN + Creatinine + gender + age'), data = data, method ='nearest', distance ='logit', replace =False, 
        ratio =1)
        func_match_data = r.r['match.data']
        m_data = func_match_data(r_out1, data =data, distance ='prop.score')
        return m_data
    def normality(data):
        func_shapiro = r.r['shapiro.test']
        lists = list(data['measurement_type'].drop_duplicates())
        T_stat =[]
        C_stat=[]
        T_pvalue=[]
        C_pvalue=[]
        for i in lists:
            condition = (data['cohort_type']==0) & (data['measurement_type']== i )
            target = data.loc[codition, 'rate']
            condition = (data['cohort_type']==1) & (data['measurement_type']== i )
            control = data.loc[codition, 'rate']
            t_out = func_shapiro(target)
            c_out = func_shapiro(control)
            T_stat.append(t_out[0][0])
            T_pvalue.append(t_out[1][0])
            C_stat.append(c_out[0][0])
            C_pvalue.append(c_out[1][0])
        df = pd.DataFrame({'type': lists, 'T_stat' : T_stat, 'T_pvalue': T_pvalue, 'C_stat' : C_stat, 'C_pvalue': C_pvalue })
        return df
    def vartest(data):
        func_vartest = r.r['var.test']
        lists = list(data['measurement_type'].drop_duplicates())
        F_stat =[]
        P_value=[]
        for i in lists:
            condition = data['measurement_type']== i 
            m_data = data.loc[condition, :]
            m_data.drop_duplicates(inplace=True)
            m_out= func_vartest(Formula('rate ~cohort_type'), m_data)
            F_stat.append(m_out[0][0])
            P_value.append(m_out[2][0])
        df = pd.DataFrame({'type': lists, 'F_stat' : F_stat, 'P_value': P_value})
        return df
    def test(data):
        func_ttest=r.r['t.test']
        func_wilcox=r.r['wilcox.test']
        lists = list(data['measurement_type'].drop_duplicates())
        ttest_F_stat =[]
        ttest_P_value=[]
        wilcox_F_stat =[]
        wilcox_P_value=[]
        for i in lists:
            condition = data['measurement_type']== i 
            m_data = data.loc[condition, :]
            m_data.drop_duplicates(inplace=True)
            m_out1= func_ttest(Formula('rate ~cohort_type'), m_data)
            m_out2= func_wilcox(Formula('rate ~cohort_type'), m_data)
            ttest_F_stat.append(m_out1[0][0])
            ttest_P_value.append(m_out1[2][0])
            wilcox_F_stat.append(m_out2[0][0])
            wilcox_P_value.append(m_out2[2][0])
        df = pd.DataFrame({'type': lists, 'ttest_Fstat' : ttest_F_stat, 'ttest_P_value': ttest_P_value,'wilcox_F_stat':wilcox_F_stat, 'wilcox_P_value':wilcox_P_value})
        return df 

    def dose_preprocess(data):
        ## T (high dose) vs Control  
        condition = (data['dose_type'] =='high') & (data['cohort_type']==0)
        high_list = data.loc[condition, 'subject_id'].drop_duplicates()
        high = data.loc[condition, :].drop_duplicates()
        condition = data['cohort_type']==1
        control = data.loc[condition, :].drop_duplicates()
        sub1 = pd.concat([high, control]).drop_duplicates()
        condition = (data['subject_id'].isin(high_list)==False) & (data['cohort_type']==0)
        low = data.loc[condition,:].drop_duplicates()
        sub2 = pd.concat([low, control]).drop_duplicates()
        return sub1, sub2
    def drug_preprocess(data):
        lists =['metformin', 'SU', 'alpha', 'dpp4i', 'gnd', 'sglt2', 'tzd']
        results=[]
        for i in lists:
            if i =='metformin': 
                condition = (data.cohort_type ==0) & (data.drug_group.isin(['metformin']))
                i = data2.loc[condition,['subject_id','drug_group','measurement_type', 'value_as_number_before','value_as_number_after']]
                i.drop_duplicates(inplace=True)
                results.append(i)
            else:   
                condition = (data.cohort_type ==0) & (data['drug_group'].str.contains(i))
                i = data.loc[condition,['subject_id','drug_group','measurement_type', 'value_as_number_before','value_as_number_after']]
                i.drop_duplicates(inplace=True)
                results.append(i)
        return results 
    def pairedtest (data):
        data = ff.delete_none(data)
        lists = list(data['measurement_type'].drop_duplicates())
        func_shapiro = r.r['shapiro.test']
        func_vartest = r.r['var.test']
        func_ttest=r.r['t.test']
        func_wilcox=r.r['wilcox.test']
        
        shapiro_stat =[]
        shapiro_pvalue=[]
        var_stat=[]
        var_pvalue=[]

        data['value_as_number_before'] = data['value_as_number_before'].astype(float)
        data['value_as_number_after'] = data['value_as_number_after'].astype(float)
        shapiro_pvalue_post=[]
        shapiro_pvalue_pre=[]
        var_pvalue=[]
        ttest_F_stat =[]
        ttest_P_value=[]
        wilcox_F_stat =[]
        wilcox_P_value=[]      
        
        for j in lists:
            condition = data['measurement_type']== j
            post = data.loc[codition, 'value_as_number_after']
            pre = data.loc[codition, 'value_as_number_before']
            shapiro_result_post= func_shapiro(post)
            shapiro_result_pre= func_shapiro(pre)
            var_result = func_vartest(post, pre)
            ttest_result= func_ttest(post, pre)
            wilcox_result= func_wilcox(post, pre)
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
# # #rate변수 합치기. !!!!!!!!!!!!!!!!!!!!!!!
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









