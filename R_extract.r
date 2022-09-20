# 환경 설정 
# install.packages("dplyr")
# install.packages("ggplot2")
# install.packages("lubridate")
# install.packages("reshape")
# install.packages("plyr")
# install.packages("vcd")
# install.packages("utils")
# install.packages(c("DBI", "RPostgreSQL"))


## 라이브러리 
library(reshape)
library(lubridate)
library(ggplot2)
library(dplyr)
library(RPostgreSQL)
library(DBI)
library(stringr)
library(utils)
library(data.table)



## 접속 정보 
# (1) DB 접속
drv <- dbDriver("PostgreSQL")
## get environment variables
database <- Sys.getenv("MEDICAL_RECORDS_DATABASE")
user <- Sys.getenv("MEDICAL_RECORDS_USER")
pw <- Sys.getenv("MEDICAL_RECORDS_PW")
url <- Sys.getenv("MEDICAL_RECORDS_URL")
dbhost <- strsplit(url, ":")[[1]][1]
port <- strsplit(url, ":")[[1]][2]
schema <- Sys.getenv("MEDICAL_RECORDS_SCHEMA")
print(c(database, user, pw, url, dbhost, port, schema))
# (2) DB 연결 
# host : server name or localhhost
con <- dbConnect( drv, dbname = database, port = port, user = user, password = pw, host = dbhost)
con
# (3) input_file reading  
cohort_inform <- data.table::as.data.table(read.csv(file= "home/syk/DW_IP_ID_2.csv", header =TRUE))
db_info <- cohort_inform[IP==dbhost]
db_hospital <- db_info$HOSPITAL
db_target <- db_info$T
db_control <- db_info$C1
t1 <- as.data.table(read.csv(file = "home/syk/total.csv", header = TRUE))
t1 <- t1[, c("drug_concept_id", "Name", "type1", "type2")]
print( paste("hospital : ", db_hospital, "schema: ", schema))
setDTthreads(percent = 100)

print(c(schema, db_target, db_control ))
source("home/syk/R_function.r")
####SQL 
sql  <- " select distinct (case when a.cohort_definition_id = target then 'T'
    when a.cohort_definition_id = control then 'C' else '' end) as cohort_type
      ,a.subject_id 
      ,a.cohort_start_date
      ,(a.cohort_start_date + 455) as cohort_end_date
      ,b.drug_concept_id
      ,b.drug_exposure_start_date
      ,(b.drug_exposure_end_date + 30) as drug_exposure_end_date
      ,b.quantity
      ,b.days_supply
      ,(case        when c.measurement_concept_id in (3020460, 3010156) then 'CRP'
                    when c.measurement_concept_id in (3015183, 3013707) then 'ESR'
                    when c.measurement_concept_id in (3013682, 3004295) then 'BUN'
                    when c.measurement_concept_id = 3022192 then 'Triglyceride'
                    when c.measurement_concept_id in (3004249, 21490853) then 'SBP'
                    when c.measurement_concept_id = 3027484 then 'Hb'        
                    when c.measurement_concept_id in (3037110, 3040820) then 'Glucose_Fasting'
                    when c.measurement_concept_id = 3016723 then 'Creatinine'
                    when c.measurement_concept_id = 3007070 then 'HDL'       
                    when c.measurement_concept_id = 3013721 then 'AST'       
                    when c.measurement_concept_id = 3024561 then 'Albumin'   
                    when c.measurement_concept_id in (3015089, 3012064, 3016244) then 'insulin'
                    when c.measurement_concept_id = 3038553 then 'BMI'       
                    when c.measurement_concept_id = 3004410 then 'HbA1c'     
                    when c.measurement_concept_id in (3012888, 21490851) then 'DBP'
                    when c.measurement_concept_id = 3027114 then 'Total cholesterol'
                    when c.measurement_concept_id = 3028437 then 'LDL'       
                    when c.measurement_concept_id in (42529224, 3029187, 42870364) then 'NT-proBNP'
                    else ''  END) as measurement_type
      ,c.measurement_date
      ,c.value_as_number
      , (case when d.gender_concept_id = 8507 then 'M' 
              when d.gender_concept_id = 8532 then 'F' 
        else '' END) as gender
      , (EXTRACT(YEAR FROM a.cohort_start_date)-d.year_of_birth) as age 
      , e.ancestor_concept_id 
      from cdm_hira_2017_results_fnet_v276.cohort as a
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
      (select * from cdm_hira_2017.measurement where measurement_concept_id in (3020460, 3010156, 3015183, 3013707, 3013682, 3004295, 3022192, 3004249,21490853,
                                                3027484, 3037110, 3040820,3016723, 3007070,3013721, 3024561,3015089, 3012064, 3016244,3038553,
                                                3004410, 3012888, 21490851 ,3027114 ,3028437 , 42529224, 3029187, 42870364, 40771922, 3005770, 3025315,3036277 ))  as c
      on a.subject_id = c.person_id
      
      join 
      (select * from cdm_hira_2017.person) as d 
      on a.subject_id = d.person_id
      
      join      
      (select z.person_id as person_id, z.condition_start_date as condition_start_date, y.ancestor_concept_id as ancestor_concept_id 
        from cdm_hira_2017.condition_occurrence as z
        join (SELECT descendant_concept_id, ancestor_concept_id
              FROM cdm_hira_2017.concept_ancestor
              WHERE ancestor_concept_id IN (4329847,316139, 321052 ,381591,434056,4182210, 4063381, 257628, 134442, 80800, 80809, 256197,255348, 4247120 ,4064161, 4212540, 201820, 443767, 442793, 192606, 374022, 4030518, 443392, 4245975, 4029488, 192680,24966, 432851, 439727, 316866, 432867, 132797, 4254542, 4239233, 4245042) ) as y
        on z.condition_concept_id = y.descendant_concept_id ) as e     
      on a.subject_id = e.person_id
where a.cohort_definition_id in (target, control)
      and b.drug_exposure_start_date between (a.cohort_start_date + 90)  and (a.cohort_start_date + 455)
      and e.condition_start_date between (a.cohort_start_date - 365) and a.cohort_start_date "

# save query 
 ## female ='1' , male =0 

data <- ff$save_query(sql, schema, db_target, db_control, con)
file_size <- object.size(data)
print("first sql data file size is  ")
print(file_size, units = "auto")
names(data)[names(data)=='subject_id'] <-  c("ID")
head(data)

# # 1. extract for PS mathcing; drug, disease history, renal values(BUN, Creatinine, eGFR), cci 
#  ## (1) drug history # nolint
drug_history <- ps$drug_history(data, t1) # variables: id, type(all drug list), dummmies variable( SU", "alpha", "dpp4i", "gnd", "metformin", "sglt2", "tzd )
print("drug history")
head(drug_history)
# ## (2) disease history
# make dummies 
data<-data %>% mutate (condition_type = case_when(ancestor_concept_id == 4329847 ~ "MI",
                                            ancestor_concept_id == 316139  ~ "HF",
ancestor_concept_id == 321052  ~ "PV",
ancestor_concept_id %in% c(381591, 434056)  ~ "CV",
ancestor_concept_id == 4182210 ~ 'Dementia',
ancestor_concept_id == 4063381 ~ 'CPD',
ancestor_concept_id %in% c(257628, 134442, 80800, 80809, 256197, 255348) ~ 'Rheuma',
ancestor_concept_id == 4247120 ~ 'PUD',
ancestor_concept_id %in% c(4064161, 4212540) ~ 'MLD',
ancestor_concept_id == 201820 ~ 'D',
ancestor_concept_id %in% c(443767,442793) ~ 'DCC',
ancestor_concept_id %in% c(192606, 374022) ~ 'HP',
ancestor_concept_id %in% c(4030518,	4239233, 4245042) ~ 'Renal',
ancestor_concept_id == 443392  ~ 'M',
ancestor_concept_id %in% c(4245975, 4029488, 192680, 24966) ~ 'MSLD',
ancestor_concept_id == 432851  ~ 'MST',
ancestor_concept_id == 439727  ~ 'AIDS',
ancestor_concept_id == 316866  ~ 'HT',
ancestor_concept_id == 432867  ~ 'HL',
ancestor_concept_id == 132797  ~ 'Sepsis',
ancestor_concept_id == 4254542 ~ 'HTT',
TRUE ~ 'error'
))
disease_history <- ps$disease_history(data) # variables: id, type(all disease list), dummies variabel: codition_types
print("disease history")
head(disease_history)
## (3)  renal values # V : "ID", "measurement_date", "BUN", "Creatinine", "egfr"
renal <- ps$renal(data)
print("renal")
head(renal)
## (4) cci 
cci <- ps$cci(disease_history)
print("cci")
head(cci)
## (5) combind
n1 <- length(unique(drug_history$ID))
n2 <- length(unique(disease_history$ID))
n3 <- length(unique(renal$ID))
n4 <- length(unique(cci$ID))
print( paste0("total N, drug_history: ",n1, "disease_history : ", n2, "renal :", n3, "cci :", n4) )
ps <- join_all(list(drug_history, disease_history, renal, cci))

# 2. Simplify 
#(1) pair 
pair <- simplify$pair(data)
print("pair")
head(pair)

#(2) exposure
exposure <- simplify$exposure(pair)

print("exposure")
head(exposure)
#(3) rule out 
ruleout<- simplify$ruleout(exposure, t1)

print("ruleout")
head(ruleout)
# 3. combine data
total  <- left_join(ruleout, ps, by= "ID")

print("total")
head(total)

# id -> 난수로 대체. 
id <-total$ID
new_ids <-c()
while (TRUE) {
  new_ids <- append(new_ids, random_id(n=1))
  if (length(new_ids) == length(id)){
    break
  }}
total$ID <- new_ids
total$hospital <- db_hospital
print("total")
head(total)

## file 내보내기 
# total 
write.csv(total, "home/syk/results/total.csv")
file_size <- object.size(total)
print("final sql data file size is  ")
print(file_size, units = "auto")