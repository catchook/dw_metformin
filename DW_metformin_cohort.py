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

# T1: ID/DRUG/DATE/TYPE (after index date )
c.execute(""" 
    select distinct a.cohort_definition_id,(case when a.cohort_definition_id = 2479 then 'T'
                         when a.cohort_definition_id = 2480 then 'C' else '' end) as cohort_type
                  ,a.subject_id
                  ,a.cohort_start_date
                  ,a.cohort_end_date
                  ,b.drug_concept_id
                  ,b.drug_exposure_start_date
                  ,b.drug_exposure_end_date

    from cdm_hira_2017_results_fnet_v276.cohort a
        left join  cdm_hira_2017.drug_exposure b
        on a.subject_id = b.person_id

    where a.cohort_definition_id in (2479, 2480)
        and a.cohort_start_date <= b.drug_exposure_start_date /*drug exp after cohort start*/
""")

rows=c.fetchall()
for row in rows:
    row_lists_output=[]
    for column_index in range(len(row)):
        row_lists_output.append(str(row[column_index]))
    print(row_lists_output)

# metformin-drug 되는지. 