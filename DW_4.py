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
import DW_function as ff



# input_file reading 
input_file = sys.argv[1]
cohort_inform= sys.argv[2]

t1 = pd.read_csv(input_file)
t1= t1[['Id', 'Name','type1','type2','ingredient_count']]
# connect 
if __name__=='__main__' : 
    SCHEMA, HOST, c  = ff.connect()

    cohort_info= pd.read_csv(cohort_inform)
    condition = cohort_info['IP'] == HOST
    print("schema:", SCHEMA, "host: ", HOST)
    cohort_info =cohort_info.loc[condition,:]
    cohort_hospital= cohort_info['HOSPITAL'].iloc[0]
    cohort_target= cohort_info['T'].iloc[0].astype(str)
    cohort_control= cohort_info['C1'].iloc[0].astype(str)

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
    m1= ff.save_query(SCHEMA, cohort_target, cohort_control, sql, c)
    print(m1.head())
    # column  지정
    m1.columns=['cohort_definition_id,','cohort_type','subject_id','cohort_start_date', 'cohort_end_date','measurement_concept_id','measurement_type','measurement_date','value_as_number', 'ROW']

    #[1/3] Simplicy N: only who got ESR, CRP
    condition = (m1['ROW']== '1')  & (m1['measurement_type'].isin(['CRP','ESR']))
    before = m1.loc[condition, ['cohort_type', 'subject_id', 'measurement_type','value_as_number']]
    before.dropna(subset=['value_as_number'], inplace=True)
    condition = (m1['ROW']== '0')  & (m1['measurement_type'].isin(['CRP','ESR']))
    after = m1.loc[condition, ['cohort_type', 'subject_id', 'measurement_type','value_as_number','measurement_date']]
    after.dropna(subset=['value_as_number'], inplace=True)
    pair= pd.merge(left =before, right= after, on=['subject_id', 'cohort_type', 'measurement_type'], how ='inner', suffixes=('_before','_after'))

    export = pair[:40]
    export.to_csv("/data/results/"+cohort_hospital+'_paired_measurement_data.csv')
    print("pair-CRP, ESR ")
    del export 
    del before
    del after 
    # who got paired data?
    all_n, CRP_t_n , ESR_t_n, CRP_c_n, ESR_c_n = ff.count_crp_esr(pair)
    print("[1/3] Simplify N: WHO got all CRP, ESR are {} N, Target: CRP- {} N, ESR- {} N, Control: CRP- {} N, ESR- {}N".format(all_n, CRP_t_n , ESR_t_n, CRP_c_n, ESR_c_n))

    # drug table 
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

    d1= ff.save_query(SCHEMA, cohort_target, cohort_control, sql, c)
    # set column 
    d1.columns= ['cohort_type','subject_id','Id','drug_exposure_start_date',
                                'drug_exposure_end_date', 'quantity', 'days_supply']
    d1['Id']=d1['Id'].astype(int)

    #[2/3] simplify N: measurement in drug_Exposure
    dm = pd.merge(left=pair, right = d1, on =['subject_id','cohort_type'], how= 'left')
    ##change str to date
    ff.change_str_date(dm)
    dm['measure_in_drug_exposure'] = dm.apply(lambda x:  1 if ( (x['drug_exposure_start_date'] <= x['measurement_date']) and 
    (x['measurement_date'] <= x['drug_exposure_end_date'])) else 0, axis=1 ) #axis=1 column, compare columns
    condition = dm['measure_in_drug_exposure']== 1
    dm=dm.loc[condition, :]
    # who got measurement in drug exposrue?
    all_n, CRP_t_n , ESR_t_n, CRP_c_n, ESR_c_n = ff.count_crp_esr(dm)
    print("[2/3] simplify N: WHO got measurement data in drug exposrue  are {} N, Target: CRP- {} N, ESR- {} N, Control: CRP- {} N, ESR- {}N".format(all_n, CRP_t_n , ESR_t_n, CRP_c_n, ESR_c_n))
    #[3/3] simplify N: ALL: 3 ingredient out, target: not metformin out  
    dc = pd.merge(dm[['subject_id', 'Id', 'measurement_date', 'measurement_type','quantity', 'days_supply']], 
                t1[['Id','Name','type1','type2']], how = 'left', on= "Id")
    dc.rename(columns={'Id':'drug_concept_id'}, inplace=True)
    dc.drop_duplicates(inplace=True)
        ## dose group 
    metformin_dose =ff.extract_number(dc)
    dc['metformin_dose']= metformin_dose
    dc = dc.astype({'quantity':'float', 'days_supply':'float'})
        # new column: dose_group: high(over 1,000 mg/day) / low  
    dc['metformin_dose_type']=dc.apply(lambda x:'high' if (x['metformin_dose']* x['quantity']/x['days_supply']>=1000.0) else 'low', axis=1)
        # new column: ingredient_count; count type
    dc2= dc.melt(id_vars=['subject_id','measurement_type','measurement_date'], value_vars=['type1','type2'], value_name ='type')
    dc2= dc2.groupby(['subject_id','measurement_type','measurement_date'])['type'].agg( lambda x:list(x)).reset_index()
    dc2['ingredient_count'] = dc2['type'].apply(lambda x: len(set(x)))
        # new column: metformin_count
    dc2['metformin_count'] = dc2['type'].apply(lambda x: x.count("metformin"))
        # exclude 3 ingredient  
    condition= dc2['ingredient_count'] < 3
    dc2= dc2.loc[condition,:]
        # exclude target-non-metformin 
    condition = (dc2['cohort_type']=='T') & (dc2['metformin_count']==0)
    dc2= dc2.loc[~condition, :]
    dc2['drug_group'] =dc2['type'].apply(lambda x:'/'.join(map(str, x)))
    all_n, CRP_t_n , ESR_t_n, CRP_c_n, ESR_c_n = ff.count_crp_esr(dc2)
    print("[3/3] simplify N: ALL: 3 ingredient out, target: not metformin out/ are {} N, Target: CRP- {} N, ESR- {} N, Control: CRP- {} N, ESR- {}N".format(all_n, CRP_t_n , ESR_t_n, CRP_c_n, ESR_c_n))
    export = dc2[:40]
    export.to_csv("/data/results/"+cohort_hospital+'_simplify_final.csv')
    del export
    ## lets make data 
    ## 1) 1st PS matching: drug classification --PS_1st 
    ## 3) measure table: before, after ESR, CRP --dc2
    ## 4) measure table 2: before all  for healthscore age --but it need add age, sex
    ## 5) measure table 3: before BUN, Cr for 1st PS matching --dc2      

    # 1st PS matching: drug classification 
    dc3 = dc2.melt(id_vars=['subject_id'], value_vars=['type1','type2'], value_name='type')
    dc3 = dc3.groupby('subject_id')['type'].agg(lambda x: list(set(x))).reset_index()
    dc3 = dc3.pivot_table(index=['subject_id'], columns ='type', aggfunc= 'size', fill_value =0).reset_index()
    PS_1st= ff.drug_classification(dc3)

    del dc3 

    #2) measure table: before, after ESR, CRP  (after에서 index date에 가까운 것만 고르기)
    
    # new columns: ROWS
    dc2['ROW'] = dc2.sort_values(by='measurement_date', ascending= True).groupby(['subject_id','measurement_type']).cumcount()+1
    export = dc2[:40]
    export.to_csv("/data/results/"+cohort_hospital+'_check_row.csv')
    print (dc2.head(20))
    del export

    ## pick only row =1 
    condition = dc2['ROW']=='1'
    dc2= dc2.loc[condition,:]
    all_n, CRP_t_n , ESR_t_n, CRP_c_n, ESR_c_n = ff.count_crp_esr(dc2)
    print("same number??: total are {} N, Target: CRP- {} N, ESR- {} N, Control: CRP- {} N, ESR- {}N".format(all_n, CRP_t_n , ESR_t_n, CRP_c_n, ESR_c_n))

    ## 4) measure table 2: before, AFTER  all  for healthscore age but it need add  age, sex columns 
    condition = m1['ROW']== '1'  
    before = m1.loc[condition, ['subject_id', 'measurement_concept_id','value_as_number']]
    condition = m1['ROW']== '0' 
    after = m1.loc[condition, ['subject_id', 'measurement_concept_id','value_as_number','measurement_date']]
    del m1
    after['ROW'] = after.sort_values(by='measurement_date', ascending= True).groupby(['subject_id','measurement_concept_id']).cumcount()+1
    condition= after['ROW']== 1
    after= after.loc[condition, :]
    print("is it work? cumcount?; check after table")
    print(after.head(20))
    export = after[:40]
    export.to_csv("/data/results/"+cohort_hospital+'_check_after_table.csv')
    del export
    #before pre healthcare: pivot_Table 
    hc_before = before.pivot(index='subject_id', columns='measurement_concept_id', values='value_as_number')
    hc_after = after.pivot(index='subject_id', columns='measurement_concept_id', values='value_as_number')

    ## add BUN, CR Before DATA to dc2
    condition= before['measurement_concept_id'].isin(['3013682','3016723'])
    before = before.loc[condition,:]
    dc2 = pd.merge(dc2, before, on='subject_id', how='left')
    dc2.rename(columns={'3013682':'BUN', '3016723':'Cr' }, inplace=True)

    del before
    del after 
    del m1
    del d1
    ## select sex, age 
    sql=""" 
        select distinct (case when a.cohort_definition_id = target then 'T'
                            when a.cohort_definition_id = control then 'C' else '' end) as cohort_type
                    ,a.subject_id
                    ,(case when b.gender_concept_id = '8507' then 'M'
                            when b.gender_concept_id = '8532' then 'F' else '' end) as gender
                    ,b.year_of_birth 
                    ,EXTRACT(YEAR FROM a.cohort_start_date) AS start_year
        from cdm_hira_2017_results_fnet_v276.cohort a
            left join  cdm_hira_2017.person b
            on a.subject_id = b.person_id
        where a.cohort_definition_id in (target, control)
        """
    p= ff.save_query(SCHEMA, cohort_target, cohort_control, sql, c)
    # set column 
    p.columns= ['cohort_type','subject_id','gender','year_of_birth', 'start_year']
    p['age'] = p['start_year']- p['year_of_birth']

    # add dc2 & heathscore
    p = p.drop(columns=['year_of_birth', 'start_year'], axis=1)

    dc2= pd.merge(dc2, p, on=['cohort_type','subject_id'], how= 'left')
    hc_before =pd.merge(hc_before, p, on=['cohort_type','subject_id'], how= 'left')
    hc_after =pd.merge(hc_after, p, on=['cohort_type','subject_id'], how= 'left')

    export = hc_before[:40]
    export.to_csv("/data/results/"+cohort_hospital+'_hc_before.csv')
    export = hc_after[:40]
    export.to_csv("/data/results/"+cohort_hospital+'_hc_after.csv')
    print (hc_before.head(20))
    print (hc_after.head(20))
    del export
print("end")
