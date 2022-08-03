#DW_5 : 합의된 쿼리문으로 변경 
# 1. Simpliyfy N 
# [1/3] Simplify N: only who got ESR, CRP
# [2/3] simplify N: measurement in drug_Exposure
# [3/3] simplify N: ALL: 3 ingredient out, target: not metformin out 
# 2. Add PS matching data
# [1/3] 1st PS matching: drug history (adm drug group) 
# [2/3] 1st PS matching: BUN, Creatinine
# [3/3] 2nd PS matching: disease history
# 3. Add HealthScore data:
# [1/3] healthscore variabels: before, after 
# [2/3] calculate healthscore 
##########################################################################################################################################################################
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
import DW_class as cc 

# input_file reading 
input_file = sys.argv[1]
cohort_inform= sys.argv[2]
t1 = pd.read_csv(input_file)
t1= t1[['Id', 'Name','type1','type2','ingredient_count']]
t1.rename(columns={'Id':'drug_concept_id'}, inplace=True)
cohort_info= pd.read_csv(cohort_inform)
# connect 
if __name__=='__main__' : 
    SCHEMA, HOST, c  = ff.connect()
    condition = cohort_info['IP'] == HOST
    print("schema:", SCHEMA, "host: ", HOST)
    cohort_info =cohort_info.loc[condition,:]
    cohort_hospital= cohort_info['HOSPITAL'].iloc[0]
    cohort_target= cohort_info['T'].iloc[0].astype(str)
    cohort_control= cohort_info['C1'].iloc[0].astype(str)
# sql 
    sql = """ select distinct (case when cohort_definition_id = target then 'T'
      when cohort_definition_id = control then 'C' else '' end) as cohort_type
      ,a.subject_id 
      ,a.cohort_start_date
      ,b.drug_concept_id
      ,b.drug_exposure_start_date
      ,(b.drug_exposure_end_date + 30) as drug_exposure_end_date
      ,(case        when c.measurement_concept_id in (3020460, 3010156) then 'CRP'
                    when c.measurement_concept_id in (3015183, 3013707) then 'ESR'
                    when c.measurement_concept_id = 3013682 then 'BUN' 
                    when c.measurement_concept_id = 3022192 then 'Triglyceride'
                    when c.measurement_concept_id = 3004249 then 'SBP'
                    when c.measurement_concept_id = 3027484 then 'Hb'
                    when c.measurement_concept_id in (3037110, 3040820) then 'Glucose_Fasting'
                    when c.measurement_concept_id = 3016723 then 'Creatinine'
                    when c.measurement_concept_id = 3007070 then 'HDL'
                    when c.measurement_concept_id = 3013721 then 'AST'
                    when c.measurement_concept_id = 3024561 then 'Albumin'
                    else '' 
                    END) as measurement_type
      ,c.measurement_date
      ,c.value_as_number
      ,d.gender_concept_id
      , (case when d.gender_concept_id = 8507 then 'M'
              when d.gender_concept_id = 8532 then 'F') as gender
      ,d.year_of_birth
      ,a.cohort_start_date + interval '1 year 3 month' as end_date 
      from cdm_hira_2017_results_fnet_v276.cohort as a

      join
      (select * from cdm_hira_2017.measurement where measurement_concept_id in (3020460, 3010156, 3015183, 3013707, 3013682, 3022192, 3004249,
                                                3027484, 3037110, 3040820,3016723, 3007070,3013721, 3024561 ))  as c
      on a.subject_id = c.person_id
      
      join
      (select * from cdm_hira_2017.drug_exposure where drug_concept_id in (select distinct descendant_concept_id
                                from cdm_hira_2017.concept_ancestor
                                where ancestor_concept_id in (1503297,43009032,19122137,35198118,1502855,1502809,43009070,1580747,
                                        793143,40166035,1547504,1516766,1525215,35197921,1502826,43009094,1510202,
                                        43009055,44506754,40170911,40239216,43009020,1559684,19097821,1560171,
                                        1597756,19059796,19001409,43009089,1583722,43009051, 793293,45774751,45774435,
                                        44785829,1594973,19033498,43526465,43008991,43013884,44816332,1530014,1529331)))  as b 
      on a.subject_id = b.person_id
     
      join 
      (select * from cdm_hira_2017.person) as d 
      on a.subject_id = d.person_id

      where a.cohort_definition_id in (target, control)
      and b.drug_exposure_start_date between a.cohort_start_date + interval '90 day' and a.cohort_start_date + interval '1 year 3 month'
"""
    m1= ff.save_query(SCHEMA, cohort_target, cohort_control, sql, c)
        # check file size
    m1.columns= ['cohort_type','subject_id','cohort_start_date','drug_concept_id','drug_exposure_start_date',
                                'drug_exposure_end_date', 'measurement_type','measurement_date', 'value_as_number','gender'
                                ,'year_of_birth','end_date']
    file_size = sys.getsizeof(m1)
    print("m1 file size: ", ff.convert_size(file_size), "bytes")
    all_n, CRP_t_n , ESR_t_n, CRP_c_n, ESR_c_n = ff.count_crp_esr(m1)
    print("Whole cohort subject are {} N, \n Target: CRP- {} N, ESR- {} N,  \n Control: CRP- {} N, ESR- {}N".format(all_n, CRP_t_n , ESR_t_n, CRP_c_n, ESR_c_n))
# 1. Simpliyfy N 
# [1/3] Simplify N: only who got ESR, CRP
    s = cc.Simplify
    pair= s.Pair(m1)
    all_n, CRP_t_n , ESR_t_n, CRP_c_n, ESR_c_n = ff.count_crp_esr(pair)
    print("[1/3] Simplify N: WHO got all CRP, ESR are {} N, \n Target: CRP- {} N, ESR- {} N, \n Control: CRP- {} N, ESR- {}N".format(all_n, CRP_t_n , ESR_t_n, CRP_c_n, ESR_c_n))
# [2/3] simplify N: measurement in drug_Exposure
    exposure= s.Exposure(m1)
    all_n, CRP_t_n , ESR_t_n, CRP_c_n, ESR_c_n = ff.count_crp_esr(exposure)
    print("[2/3] Simplify N: measurement in drug exposure, total are {} N, \n Target: CRP- {} N, ESR- {} N, \n Control: CRP- {} N, ESR- {}N".format(all_n, CRP_t_n , ESR_t_n, CRP_c_n, ESR_c_n))
# [3/3] simplify N: ALL: 3 ingredient out, target: not metformin out
    final= s.Ingredient(m1, t1)
    all_n, CRP_t_n , ESR_t_n, CRP_c_n, ESR_c_n = ff.count_crp_esr(final)
    print("[3/3] Simplify N: 3 ingredient out, target: not metformin out , total are {} N, \n Target: CRP- {} N, ESR- {} N, \n Control: CRP- {} N, ESR- {}N".format(all_n, CRP_t_n , ESR_t_n, CRP_c_n, ESR_c_n))

# 2. Add PS matching data
# [1/3] 1st PS matching: drug history (adm drug group) 
# [2/3] 1st PS matching: BUN, Creatinine
# [3/3] 2nd PS matching: disease history
# 3. Add HealthScore data:
# [1/3] healthscore variabels: before, after 
# [2/3] calculate healthscore 
# 4. Add Subgroup analysis
# [1/2] by dose group 
# [2/2] by drug group  
    


# BEFORE, AFTER SEPERATE  + measurement in drug exposure date 
    condition= (m1['measurement_date'] <= m1['cohort_start_date']) & (m1.groupby(['person_id'])['measurement_date'].transform(max) == m1['measurement_date'] )
    before  = m1.loc[condition, :]
    condition = (m1['measurement_date'] >= m1['cohort_start_date'] ) & (m1['measurement_date'] >= m1['drug_exposure_start_date'])&(m1['measurement_date'] <= m1['drug_exposure_end_date'])
    after = m1.loc[condition,:]
    # pair 
    step2 = pd.merge(before, after, on =['cohort_type', 'person_id','cohort_start_date','measurement_type','gender_concept_id','year_of_birth', 'end_date'], how= 'inner')
    file_size = sys.getsizeof(step2)
    print("step2 file size: ", ff.convert_size(file_size), "bytes")
    step2.to_csv("/data/results/"+cohort_hospital+'_step2_c.csv')


# # # cohort<-cohort[person_id %in% c(1926,1830392)]
# #
# # cohort[,drug_exposure_start_date:=NULL]
# # cohort[,drug_exposure_end_date:=NULL]
# # cohort<-unique(cohort)
# #
# # cohort[,cohort_end_date:=NULL]
# # cohort[,cohort_definition_id:=NULL]
# #
# # # library(lubridate)
# cohort_bf<-cohort[ymd(measurement_date)<ymd(cohort_start_date)]
# cohort_bf<-cohort_bf[,.SD[which.max(ymd(measurement_date))][,.(bf_measurement_date=measurement_date,bf_value=value_as_number)],by=c("person_id,m_type")]
# # 
# cohort_af<-cohort[ymd(cohort_start_date)<=ymd(measurement_date)]
# cohort_af<-cohort_af[data.table::between(ymd(measurement_date),ymd(drug_exposure_start_date),ymd(drug_exposure_end_date)+30)]


# ###############################################################################################
# setkey(drug_type,concept_id)
# setkey(cohort_af,drug_concept_id)
# 
# cohort_af<-drug_type[cohort_af]
# cohort_af_drug3<-cohort_af[order(person_id,drug_exposure_start_date)]
# 
# drug3<-unique(cohort_af_drug3[,.(person_id,type1,type2,cohort_type,drug_exposure_start_date)])
# ###############################################################################################
# # 3 kind of drug period
# drug32<-melt(data=drug3,id.vars = c("person_id","drug_exposure_start_date"),variable.name = c("type"), measure.vars =c("type1","type2"))
# drug32[,type:=NULL]
# drug32<-drug32[order(person_id,drug_exposure_start_date)]
# drug32_F<-drug32[, .(N = length(unique(value))), by = c("person_id","drug_exposure_start_date")]
# 
# ###############################################################################################
# 
# drug33<-drug3[cohort_type=="T"&(type1=="metformin"|type2=="metformin")|cohort_type=="C"]
# valid_date1<-unique(drug33[,.(person_id,drug_exposure_start_date)])
# 
# valid_date<-drug32_F[valid_date1,on=c("person_id","drug_exposure_start_date")]
# valid_date<-valid_date[N<=2]
# 
# ###############################################################################################
# cohort_af_met_drug3<-cohort_af_drug3[valid_date,on=c("person_id","drug_exposure_start_date")]
# cohort_af_met_drug2_s<-cohort_af_met_drug3[,.(person_id,drug_exposure_start_date,cohort_type,af_m_type=m_type,af_value=value_as_number,measurement_date)]
# 
# cohort_af_F<-cohort_af_met_drug2_s[,.SD[which.min(ymd(drug_exposure_start_date))],by=c("person_id","af_m_type")]
# 
# cohort_bfaf<-cohort_bf[cohort_af_F,on="person_id"]
# 
# cohort_bfaf<-cohort_bfaf[m_type==af_m_type]
# cohort_bfaf<-cohort_bfaf[,.(person_id,cohort_type,m_type,bf_m_date=bf_measurement_date,drug_start=drug_exposure_start_date,af_m_date=measurement_date,bf_value,af_value)]
# 
# cohort_bfaf$cohort_type <- factor(cohort_bfaf$cohort_type, levels = c("T","C"))
# 
# nrow(cohort_bfaf)
# 
# ###############################################################################################
# # cohort_crp<-cohort_bfaf[m_type=="CRP"]
# # cohort_esr<-cohort_bfaf[m_type=="ESR"]
# #
# # table(cohort_crp$cohort_type)
# # table(cohort_esr$cohort_type)
# 
# 
# # cohort_esr<-cohort_esr[complete.cases(cohort_esr)]
# # cohort_crp<-cohort_crp[complete.cases(cohort_crp)]
# #
# # cohort_f<-rbind(cohort_crp,cohort_esr)
# # cohort_f[,N:=.N,by="person_id"]
# #
# # cohort_f<-cohort_f[order(person_id)]
# #
# # cohort_f[m_type=="ESR"]
# # cohort_f[m_type=="CRP"]
# #
# # cat("how many patient have ESR record in both before and after index date?")
# # table(cohort_f[m_type=="ESR"]$cohort_type)
# # cat("\n")
# #
# # cat("how many patient have CRP record in both before and after index date?")
# # table(cohort_f[m_type=="CRP"]$cohort_type)
# # cat("\n")
# #
# # cohort_f<-cohort_f[N==2]
# # cohort_f<-unique(cohort_f[,.(person_id,cohort_type)])
# #
# # cat("how many patient have ESR and CRP record in both before and after index date?")
# # table(cohort_f$cohort_type)
# # cat("\n")
# #
# # # cohort<-cohort[,.(person_id,cohort_start_date,drug_start,drug_end,index_bf_measurement_date_max,index_af_measurement_date_max)]
# # # cohort<-unique(cohort)
# #


# saveTable<-function(data){
#   fwrite(data,paste0("/data/results/",deparse(substitute(data)),"_",cohort_list[1,1],".csv"))
# }

# cohort_bf<-cohort_bf[!is.na(bf_value)]
# cohort_af<-cohort_af[!is.na(value_as_number)]

# dd<-cohort_af[cohort_bf,on=c("person_id","m_type")]

# nrow(unique(dd,by="person_id"))

# saveTable(dd)
# saveTable(cohort_af)
