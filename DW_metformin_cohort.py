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
#import os

###postgresql 데이터 베이스 접속 
con = pg.connect(database='omop',
                user='dbadmin',
                password = 'INIT@1234',
                host='203.245.2.202',
                port=5432)
c= con.cursor()

# input_file reading
input_file = sys.argv[1]


t1 = pd.read_csv(input_file)
t1= t1[['Id', 'Name','type1','type2','ingredient_count']]

print(t1.head())
print("input_file reading done")
print('')
# create basic table (ID/DRUG/DATE/TYPE (after index date ))
c.execute(""" 
    select distinct a.cohort_definition_id,(case when a.cohort_definition_id = 2479 then 'T'
                         when a.cohort_definition_id = 2480 then 'C' else '' end) as cohort_type
                  ,a.subject_id
                  ,a.cohort_start_date
                  ,a.cohort_end_date
                  ,b.drug_concept_id
                  ,b.drug_exposure_start_date
                  ,b.drug_exposure_end_date
                  ,b.quantity
                  ,b.days_supply
    from cdm_hira_2017_results_fnet_v276.cohort a
        left join  cdm_hira_2017.drug_exposure b
        on a.subject_id = b.person_id
    where a.cohort_definition_id in (2479, 2480)
        and a.cohort_start_date <= b.drug_exposure_start_date /*drug exp after cohort start*/
        and b.drug_concept_id in 
        (select distinct descendant_concept_id from cdm_hira_2017.concept_ancestor
        where ancestor_concept_id in( 1503297, 43009032,19122137,35198118,1502855,1502809,43009070,1580747,
                                    793143,40166035,1547504,1516766,1525215,35197921,1502826,43009094,1510202,
                                    43009055,44506754,40170911,40239216,43009020,1559684,19097821,1560171,
                                    1597756,19059796,19001409,43009089,1583722,43009051, 793293,45774751,45774435,
                                    44785829,1594973,19033498,43526465,43008991,43013884,44816332,1530014,1529331) )
""")

rows=c.fetchall()
t2=[]
for row in rows:   
    row_lists_output=[] 
    for column_index in range(len(row)):
        row_lists_output.append(str(row[column_index]))
    t2.append(row_lists_output)
t3= pd.DataFrame.from_records(t2, columns=['cohort_definition_id,','cohort_type','subject_id','cohort_start_date',
'cohort_end_date','Id','drug_exposure_start_date','drug_exposure_end_date', 'quantity', 'days_supply'])

print(t3.tail())
print('sql success')
t3['Id']=t3['Id'].astype(int)
print("convert drug_concept_id variable in T3 to integer")
print('')

# 1st PS matching: join id,type1,2, ingredient_count  
t4 = pd.merge(left= t3, right= t1, how='left', on='Id')
t4.rename(columns={'Id':'drug_concept_id'}, inplace=True)
print(t4.head(10))
print('join success')
print('')

t51= t4[['cohort_type','subject_id','drug_concept_id','type1','ingredient_count']]
t52= t4[['cohort_type','subject_id','drug_concept_id','type2','ingredient_count']]
t51.drop_duplicates(inplace=True)
t52.drop_duplicates(inplace=True)
print(' type1')
print(t51.head(10))
print(" ")
print("type2")
print(t52.head(10))
print('drop duplicates')
print(" ")

# ##1st PS matching: check error and nan 
# nan1 = t51[t51['type1'].isnull()]
# print(double_nan.head())
# print(" check double Nan of type")
# print("")

# ## extract drug conept_id list of nan 
# drug_list =double_nan['drug_concept_id'].drop_duplicates()
# for i in drug_list:
#     print(i)

# print("extract drug conept_id list of nan")

# long to wide
t51 =t51.pivot_table(index=['subject_id'], columns ='type1', aggfunc= 'size', fill_value =0)
t52 =t52.pivot_table(index=['subject_id'], columns ='type2', aggfunc= 'size', fill_value =0)
t51
t51= t51.rename(columns ={'type1':'type'}) #nan값은 꼭 채우기, 합치게되면 nan이 됨. 
t52= t52.rename(columns={'type2':'type'})

t52.fillna(0)
t52['SU']=0
t52['sglt2']=0

print("total t5")
t5= pd.concat([t51, t52], axis=0)
#t5=t5.reset_index()
print(t5.head())

print("start group by ")

t5=t5.groupby(['subject_id']).agg({"alpha": sum, "dpp4i": sum,"error": sum,"gnd": sum,"metformin": sum,"tzd": sum,"SU": sum,"sglt2": sum})
print(t5.head(10))
print(
     " T5: FOR 1st PS matching: co-drug classification done"
 )
 



