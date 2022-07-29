#test_ms_Sy
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

cohort_inform= sys.argv[1]
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
#########sy
    sql= """ with before as ( select distinct a.cohort_definition_id,(case when a.cohort_definition_id = target then 'T'
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
                    when b.measurement_concept_id = 3037110 then 'Glucose'
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
                    ,ROW_NUMBER() OVER (
                        PARTITION BY a.subject_id, b.measurement_id 
                        ORDER BY b.measurement_date desc
                    ) as ROW
        from cdm_hira_2017_results_fnet_v276.cohort a
            join  (select * from cdm_hira_2017.measurement  where measurement_concept_id in 
            (3020460, 3015183,3010156,3013707,3013682,3022192,3004249,3027484,3040820,3012888,3016723,3028437,
            3007070,3027114,3038553,3013721,3024561,3006923))  as b
            on a.subject_id = b.person_id
        where a.cohort_definition_id in (target, control)
            and b.measurement_date <= a.cohort_start_date /*measurement  before cohort start*/
            ) select * from before where ROW =1 
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
                    when b.measurement_concept_id = 3037110 then 'Glucose'
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
            join  (select * from cdm_hira_2017.measurement  where measurement_concept_id in 
            (3020460, 3015183,3010156,3013707,3013682,3022192,3004249,3027484,3040820,3012888,3016723,3028437, 
            3007070,3027114,3038553,3013721,3024561,3006923))  as b 
            on a.subject_id = b.person_id
        where a.cohort_definition_id in (target, control)
            and b.measurement_date BETWEEN (a.cohort_start_date + 90) AND  (a.cohort_start_date + 455) /*measurement  AFTER cohort start 90~455DAYS*/
    """

    m2= ff.save_query(SCHEMA, cohort_target, cohort_control, sql, c)
    # column 
    m2.columns=['cohort_definition_id,','cohort_type','subject_id','cohort_start_date', 'cohort_end_date','measurement_concept_id','measurement_type','measurement_date','value_as_number', 'ROW']
    # check file size
    file_size = sys.getsizeof(m2)
    print("m1 file size: ", ff.convert_size(file_size), "bytes")
    
    
    #[1/3] Simplicy N: only who got ESR, CRP
    condition = (m2['ROW']== '1')  & (m2['measurement_type'].isin(['CRP','ESR']))
    before = m2.loc[condition, ['cohort_type', 'subject_id', 'measurement_type','value_as_number']]
    before.dropna(subset=['value_as_number'], inplace=True, axis=0, how='all')
    condition = (m2['ROW']== '0')  & (m2['measurement_type'].isin(['CRP','ESR']))
    after = m2.loc[condition, ['cohort_type', 'subject_id', 'measurement_type','value_as_number','measurement_date']]
    after.dropna(subset=['value_as_number'], inplace=True,axis=0, how='all')
    pair= pd.merge(left =before, right= after, on=['subject_id', 'cohort_type', 'measurement_type'], how ='inner', suffixes=('_before','_after'))
    # who got paired data?
    all_n, CRP_t_n , ESR_t_n, CRP_c_n, ESR_c_n = ff.count_crp_esr(pair)
    print("[1/3] Simplify N: WHO got all CRP, ESR are {} N, Target: CRP- {} N, ESR- {} N, Control: CRP- {} N, ESR- {}N".format(all_n, CRP_t_n , ESR_t_n, CRP_c_n, ESR_c_n))
    # pair.to_csv("/data/results/"+cohort_hospital+'_pair.csv')
    # drug table //left join 이나 inner join 이나 같음, 그러나 inner join 해야 데이터 크기가 작음.  
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
            join  (select * from cdm_hira_2017.drug_exposure where drug_concept_id in 
            (select distinct descendant_concept_id from cdm_hira_2017.concept_ancestor
            where ancestor_concept_id in( 1503297, 43009032,19122137,35198118,1502855,1502809,43009070,1580747,
                                        793143,40166035,1547504,1516766,1525215,35197921,1502826,43009094,1510202,
                                        43009055,44506754,40170911,40239216,43009020,1559684,19097821,1560171,
                                        1597756,19059796,19001409,43009089,1583722,43009051, 793293,45774751,45774435,
                                        44785829,1594973,19033498,43526465,43008991,43013884,44816332,1530014,1529331))) as b
            on a.subject_id = b.person_id
        where a.cohort_definition_id in (target, control)
            and b.drug_exposure_start_date Between a.cohort_start_date and  (a.cohort_start_date + 455)         /*drug exp after cohort start*/
    """
    d1= ff.save_query(SCHEMA, cohort_target, cohort_control, sql, c)
    # set column 
    d1.columns= ['cohort_type','subject_id','Id','drug_exposure_start_date',
                                'drug_exposure_end_date', 'quantity', 'days_supply']
    d1['Id']=d1['Id'].astype(int)
          # check file size
    file_size = sys.getsizeof(d1)
    print("d1 file size: ", ff.convert_size(file_size), "bytes")
    #[2/3] simplify N: measurement in drug_Exposure
    dm = pd.merge(left=pair, right = d1, on =['subject_id','cohort_type'], how= 'left')
    all_n, CRP_t_n , ESR_t_n, CRP_c_n, ESR_c_n = ff.count_crp_esr(dm)
    print("dm table test (left join ) is it N different?? total- {} N, Target: CRP- {} N, ESR- {} N, Control: CRP- {} N, ESR- {}N".format(all_n,
     CRP_t_n , ESR_t_n, CRP_c_n, ESR_c_n))
   
   
    #change str to date
    ff.change_str_date(dm)
    dm['measure_in_drug_exposure'] = dm.apply(lambda x:  1 if ( (x['drug_exposure_start_date'] <= x['measurement_date']) and 
    (x['measurement_date'] <= x['drug_exposure_end_date'])) else 0, axis=1 ) #axis=1 column, compare columns
    condition = dm['measure_in_drug_exposure']== 1
    dm=dm.loc[condition, :]
    file_size = sys.getsizeof(dm)
    print("dm file size: ", ff.convert_size(file_size), "bytes")
    # who got measurement in drug exposrue?
    all_n, CRP_t_n , ESR_t_n, CRP_c_n, ESR_c_n = ff.count_crp_esr(dm)
    print("[2/3] simplify N: WHO got measurement data in d rug exposrue  are {} N, Target: CRP- {} N, ESR- {} N, Control: CRP- {} N, ESR- {}N".format(all_n, CRP_t_n , ESR_t_n, CRP_c_n, ESR_c_n))
    dm.to_csv("/data/results/"+cohort_hospital+'_dm.csv')