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
library(plyr)
library(dplyr)
library(DBI)
library(RPostgreSQL)
library(stringr)
library(utils)
library(data.table)
library(ids)
library(pryr)

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
db_info <- cohort_inform[IP == dbhost]
db_hospital <- db_info$HOSPITAL
db_target <- db_info$T
db_control <- db_info$C1
t1 <- as.data.table(read.csv(file = "home/syk/total.csv", header = TRUE))
t1 <- t1[, c("drug_concept_id", "Name", "type1", "type2")]
print(paste("hospital : ", db_hospital, "schema: ", schema))
setDTthreads(percent = 100)

print(c(schema, db_target, db_control))
source("home/syk/R_function.r")
######################################################################## SQL ############################################################################# 
sql1 <-"SELECT distinct (case when a.cohort_definition_id = target then 'T'
                               when a.cohort_definition_id = control then 'C' else 'error' end) as cohort_type
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
                    else 'error'  END) as measurement_type
      ,c.measurement_date
      ,c.value_as_number
      , (case when d.gender_concept_id = 8507 then 'M' 
              when d.gender_concept_id = 8532 then 'F' 
        else '' END) as gender
      , (EXTRACT(YEAR FROM a.cohort_start_date)-d.year_of_birth) as age 
      , e.ancestor_concept_id
      
      FROM cdm_hira_2017_results_fnet_v276.cohort as a      
      JOIN
      (select * from cdm_hira_2017.drug_exposure where drug_concept_id in (select distinct descendant_concept_id
                                from cdm_hira_2017.concept_ancestor
                                where ancestor_concept_id in (1503297,43009032,19122137,35198118,1502855,1502809,43009070,1580747,
                                        793143,40166035,1547504,1516766,1525215,35197921,1502826,43009094,1510202,
                                        43009055,44506754,40170911,40239216,43009020,1559684,19097821,1560171,
                                        1597756,19059796,19001409,43009089,1583722,43009051, 793293,45774751,45774435,
                                        44785829,1594973,19033498,43526465,43008991,43013884,44816332,1530014,1529331)))  as b 
      on a.subject_id = b.person_id
     
     JOIN
      (select * from cdm_hira_2017.measurement where measurement_concept_id in (3020460, 3010156, 3015183, 3013707, 3013682, 3004295, 3022192, 3004249,490853,
                                                3027484, 3037110, 3040820,3016723, 3007070,3013721, 3024561,3015089, 3012064, 3016244,3038553,
                                                3004410, 3012888, 21490851 ,3027114 ,3028437 , 42529224, 3029187, 42870364, 40771922, 3005770, 3025315,  3036277 ))  as c
      on a.subject_id = c.person_id

      JOIN
      (select * from cdm_hira_2017.person) as d 
      on a.subject_id = d.person_id
      
      JOIN
      (select z.person_id as person_id, z.condition_start_date as condition_start_date, y.ancestor_concept_id as ancestor_concept_id 
        from cdm_hira_2017.condition_occurrence as z
        join (SELECT descendant_concept_id, ancestor_concept_id
              FROM cdm_hira_2017.concept_ancestor
              WHERE ancestor_concept_id IN (4329847,316139, 321052 ,381591,434056,4182210, 4063381, 257628, 134442, 80800, 80809, 256197,255348, 4247120 ,4064161, 4212540, 201820, 443767, 442793, 192606, 374022, 4030518, 443392, 4245975, 4029488, 192680,24966, 432851, 439727, 316866, 432867, 132797, 4254542, 4239233, 4245042) ) as y
        on z.condition_concept_id = y.descendant_concept_id ) as e
      on a.subject_id = e.person_id

      WHERE a.cohort_definition_id in (target, control)
      and b.drug_exposure_start_date between (a.cohort_start_date + 90)  and (a.cohort_start_date + 455)
      and e.condition_start_date between (a.cohort_start_date - 365) and a.cohort_start_date
       "
# save query 
## female ='1' , male =0 
data1 <- ff$save_query(sql1, schema, db_target, db_control, con)
file_size <- object.size(data1)
print("first sql data file size is ")
print(file_size, units = "auto")
names(data1)[names(data1)== 'subject_id' ] <-  c("ID")
head(data1)

#change chr to date class
data2 <- ff$chr_to_date(data1)
rm(data1)
# delete no measurement_Type (error)
data3 <- data2[!(data2$measurement_type == 'error'), ]
rm(data2)
# delete no cohort_type (error)
data4 <- data3[!(data3$cohort_type == 'error'), ]
rm(data3)

##check n 
check_n1 <- ff$count_n(data4, '1. total') 
check_n1_t <- ff$count_total(data4, '1. total')
print('check n : 1. total N  ')

## 고혈압 약제 추가 
sql2 = "
      SELECT a.subject_id, count(b.drug_concept_id) as hypertension_drug 
       FROM cdm_hira_2017_results_fnet_v276.cohort as a
       LEFT OUTER JOIN  (SELECT  distinct  B.drug_concept_id,  B.person_id, B.drug_exposure_start_date 
                         FROM cdm_hira_2017.drug_exposure as B
                         WHERE B.drug_concept_id in (SELECT distinct C.descendant_concept_id  FROM cdm_hira_2017.concept_ancestor as C 
                                                     WHERE C.ancestor_concept_id in (1308842, 1317640, 40226742 , 1367500, 1347384 , 43009001, 1346686, 1351557, 40235485, 19102107, 1342439, 1334456, 1373225, 1310756, 1308216, 19122327, 1341927, 19050216, 1340128, 43009074, 1328165, 1307863, 1319880, 1318853, 1318137, 19071995, 19015802, 19004539, 1353776, 43008998, 43009017, 43009040, 35198057, 1332418, 1353766, 1314577, 1313200, 1307046, 1386957, 19049145, 13468203, 950370, 1338005, 43009042, 1322081, 1314002, 43009035, 1319998, 1363053, 1350489, 1341238,974166,1395058 ,978555 ,19018517 ,956874 , 942350 ,19010493 ,1309799 ,970250 ,991382 ,929435 ,19036797))
                                                     ) as b
      ON    a.subject_id = b.person_id
      WHERE a.cohort_definition_id in (target, control) 
      AND   b.drug_exposure_start_date between (a.cohort_start_date - 365) and a.cohort_start_date
      GROUP BY a.subject_id
    "
data2 <- ff$save_query(sql2, schema, db_target, db_control, con)
file_size <- object.size(data2)
print("2nd sql data file size is ")
print(file_size, units = "auto")
names(data2)[names(data2)== 'subject_id' ] <-  c("ID")

## 고지혈증 약제 추가
sql3 = "SELECT a.subject_id, count(b.drug_concept_id) as hyperlipidemia_drug 
        FROM cdm_hira_2017_results_fnet_v276.cohort as a
        LEFT OUTER JOIN  (SELECT  distinct  B.drug_concept_id,  B.person_id, B.drug_exposure_start_date
                         FROM cdm_hira_2017.drug_exposure as B
                         WHERE B.drug_concept_id in (SELECT distinct C.descendant_concept_id  FROM cdm_hira_2017.concept_ancestor as C 
                                                     WHERE ancestor_concept_id in (1545958, 1549686, 1592085, 40165636, 1551860, 1510813, 1539403, 19022956,1551803,1558242 ,19050375 ,1598658 ,43560137 ,19060156 ,19095309 ,19029644 ,46275447 ,46287466 ,1526475
                                                    ,42874246,1560305))
                                                    ) as b
       ON    a.subject_id = b.person_id
       WHERE a.cohort_definition_id in (target, control)  
       AND   b.drug_exposure_start_date between (a.cohort_start_date - 365) and a.cohort_start_date
       GROUP BY a.subject_id "
data3 <- ff$save_query(sql3, schema, db_target, db_control, con)
file_size <- object.size(data3)
print("3rd sql data file (hyperlipidemia) size is  ")
print(file_size, units = "auto")
names(data3)[names(data3) == 'subject_id'] <-  c("ID")
# head(data3)
##############check memory 1 #########################
print("check memory::: sql ")
mem_used()
#########################################################################  1. extract for PS mathcing ##########################################################
# # ## (3)  renal values # V : "ID",  "BUN", "Creatinine", "egfr"
# # str(data1)
renal <- ps$renal(data4)
print("renal")
#head(renal)
# # # # (1) drug history 
drug_history <- ps$drug_history(data4, t1) # variables: id, type(\all drug list), dummmies variable( SU", "alpha", "dpp4i", "gnd", "metformin", "sglt2", "tzd )
print("drug history")
#head(drug_history)

# # # ## (2) disease history
disease_history <- ps$disease_history(data4, data2, data3) # variables: id, dummies variabel: codition_types
print("disease history")
#head(disease_history)

# # ## (4) cci
print("check before calculating cci ")
str(disease_history) 
cci <- ps$cci(disease_history)
print("cci")
#head(cci)

# # # ## (5) combind
n1 <- length(unique(drug_history$ID))
n2 <- length(unique(disease_history$ID))
n3 <- length(unique(renal$ID))
n4 <- length(unique(cci$ID))
print( paste("total N, drug_history: ", n1, "disease_history : ", n2, "renal :", n3, "cci :", n4) )
ps <- plyr::join_all(list(drug_history, disease_history, cci), by ='ID')
ps2 <- left_join(ps, renal, by='ID')
rm(ps)
ps3 <- unique(ps2)
rm(ps2)
##############check memory 2 #########################
print("check memory::: ps ")
rm(data2)
rm(data3)
rm(drug_history)
rm(disease_history)
rm(cci)
rm(renal)
mem_used()
######################################################################### 2. Simplify ######################################################################
#(1) pair 
pair <- simplify$pair(data4)
print("pair")
#head(pair)
# ###check n 
check_n2 <- ff$count_n( pair, '2. pair') 
check_n2_t <- ff$count_total(pair, '2. pair') 
print('check n : 2. pair N ')
# # #(2) exposure
exposure <- simplify$exposure(pair)
print("exposure")
#head(exposure)
# ###check n 
check_n3<-ff$count_n( exposure, '3. exposure') 
check_n3_t <- ff$count_total( exposure, '3. exposure') 
print('check n : 3. exposure N  ')
# # # #(3) rule out 
ruleout<- simplify$ruleout(exposure, t1)
print("ruleout")
#head(ruleout)
####check n 
check_n4<- ff$count_n( ruleout, '4. ruleout') 
check_n4_t<- ff$count_total( ruleout, '4. ruleout') 
print('check n : 4. ruleout N  ')
##############check memory 2 #########################
print("check memory::: simplify")
rm(pair)
rm(exposure)
rm(data4)
mem_used()
######################################################################### 3. combine data ######################################################################
total  <- left_join( ps3, ruleout, by= "ID")
print("total")
head(total)
# 전처리 
# delete no measurement_Type (error)
total1 <- total[!(total$measurement_type == 'error'), ]
rm(total)
# delete no cohort_type (error)
total2 <- total1[!(total1$cohort_type == 'error'), ]
rm(total1)
# delete no gender(error)
total3 <- total2[!(total2$gender == 'error'), ]
rm(total2)
# delete value_as_number == 0
total4 <- total3[!(total3$value_as_number.before == 0),]
rm(total3)
ids <- unique(total4$ID)
new_ids <- c()
while (TRUE) {
  new_ids <- append(new_ids, ids::random_id(n=1))
  if (length(new_ids) == length(ids)){
    break
   }}
IDS <- as.data.frame(cbind(ids, new_ids))
print("merge id, new_ids")
total5 <- merge(total4, IDS, by.x ='ID', by.y= 'ids')
rm(total4)
total5$hospital <- db_hospital
print("show:: after merge")
str(total5)
print("delete original ID")
total6 <- total5 %>% select(-ID)
rm(total5)
total7 <- as.data.frame(total6)
rm(total6)
names(total7)[names(total7) == 'new_ids'] <-c("ID")
print("REPLACE new_ids TO ID")
# ## file 내보내기 
file_size <- object.size(total7)
print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! success extract step !!!!!!!!!!!!!!!!!!!!!!!  sql data file size is  ")
print(file_size, units = "auto")
##############check memory 3 #########################
print("check memory::: combine")
rm(ruleout)
rm(ps3)
mem_used()
######################################################################### 4. save ######################################################################
# ##sample 

print("rbind count_n")
count <- rbind(check_n1, check_n2, check_n3, check_n4)
print("rbind count_total")
count_t <- rbind(check_n1_t, check_n2_t, check_n3_t, check_n4_t)
##############check memory 3 #########################
print("check memory::: combine")
mem_used()
#sample <- total1[1:1000,]
#write.csv(sample, paste0("/data/results/sample_", db_hospital ,".csv")) 
write.csv(total7, paste0("/data/results/total_", db_hospital ,".csv")) 
write.csv(count, paste0("/data/results/count_", db_hospital ,".csv")) 
write.csv(count_t, paste0("/data/results/count_t_", db_hospital ,".csv")) 
