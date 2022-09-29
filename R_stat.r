# 0. setting env.
source("/home/syk/R_function.r")
library(data.table)
library(tableone)
library(reshape)
library(lubridate)
library(modules)
library(ggplot2)
library(dplyr)
library(RPostgreSQL)
library(DBI)
library(stringr)
library(utils)
library(data.table)
library(plyr)
library(ids)
library(MatchIt)
library(purrr)
library(tidyr)
# read data 
data <- fread("path", fill =TRUE)

##################################################################### 1. delete outlier #################################################################### 
## before trim 
### check_n: original N / figure / smd  
check_n1 <- ff$count_n(data, '1. Total(original)')
stat$fig(data,  '1_Total')
stat$smd(data, '1_Total')
## trim n%
### check_n: trim N / figure / smd
data <- ff$trim(data, 3)
check_n2 <- ff$count_n(data, "2. trim 3%")
stat$fig(data, "2_trim")
stat$smd(data, "2_trim") 
## delete value_as_number.before 0 values
data <- data %>% filter(value_as_number.before != 0)
check_n3 <- ff$count_n(data, "3. delete outlier")
stat$fig(data, "3_delete_outlier")
stat$smd(data, "3_delte_outlier")
## new columns: rate, diff
data$diff <- data$value_as_number.after - data$value_as_number.before
data$rate <- data$diff/ data$value_as_number.before  
#################################################################### 2. propensity score matching  ######################################
## select columsn for ps matching
ps <-  unique(data[, c("cohort_type", "age",  "gender", "SU", "alpha"   ,"dpp4i", "gnd", "sglt2" ,  "tzd" ,  
                  "BUN",  "Creatinine" ,  "MI"   , "HF"   ,   "PV"     ,   "CV"    ,   "CPD" ,   "Rheuma" , 
                  "PUD",    "MLD"      ,   "DCC" ,  "HP"  ,  "Renal",  "MSLD"  ,  "AIDS"   ,   "HT2" ,   "HL2",   
                  "Sepsis" ,  "HTT"    ,      "ID"      ,    "egfr",  'cci' )])   
colSums(is.na(ps))
ps[is.na(ps)] <- 0
## convert cohort_Type, age; chr to numeric
ps$cohort_type <- ifelse(ps$cohort_type =='C', 1, 0)
ps$gender <- ifelse(ps$gender =='M', 0, 1)
## 2:1 matching
m.out <- matchit(cohort_type ~ cci + age + gender + Creatinine + BUN + egfr + SU + alpha+ dpp4i + gnd + sglt2 + tzd + MI + HF +PV + CV + CPD + Rheuma + PUD + MLD + DCC + HP + Renal + MSLD + AIDS + HT2+ HL2 + Sepsis+ HTT , data = ps, method ='nearest', distance ='logit',  ratio =2)
summary(m.out)
m.data <- match.data(m.out, data = ps, distance = 'prop.score')
id <- m.data %>% distinct(ID)
data2 <- left_join(id, data, by = 'ID')
## check n / figure /smd 
check_n4 <- ff$count_n(data2, "4. ps_matching")
stat$fig(data2, "4_psmatch")
stat$smd(data2, "4_psmatch")
#################################################################### 3. stat  ############################################################################
print("check stat data:::")
str(data2)
## 3-1) target vs control  
### normality, var, t-test, wilcox
test_rate <- stat$test_rate(data2)
test_diff <- stat$test_diff(data2)
### paired t-test (t vs c, sub analysis by dose type)
paired_test <- stat$ptest_drug(data2)
## 3-2) high vs low 
###################### ps matching 
ps <-  unique(data[, c("cohort_type", "age",  "gender", "SU", "alpha"   ,"dpp4i", "gnd", "sglt2" ,  "tzd" ,  
                  "BUN",  "Creatinine" ,  "MI"   , "HF"   ,   "PV"     ,   "CV"    ,   "CPD" ,   "Rheuma" , 
                  "PUD",    "MLD"      ,   "DCC" ,  "HP"  ,  "Renal",  "MSLD"  ,  "AIDS"   ,   "HT2" ,   "HL2",   
                  "Sepsis" ,  "HTT"    ,   "ID"  ,    "egfr",  'cci' , 'dose_type' )])   
colSums(is.na(ps))
ps[is.na(ps)] <- 0
## convert cohort_Type, age; chr to numeric
#ps$cohort_type <- ifelse(ps$cohort_type =='C', 1, 0)
ps$gender <- ifelse(ps$gender =='M', 0, 1)
ps$dose_type <- ifelse(ps$dose_type =='high', 1 , 0)
## 1 : 1 matching
m.out <- matchit(dose_type ~ cci + age + gender + Creatinine + BUN + egfr + SU + alpha+ dpp4i + gnd + sglt2 + tzd + MI + HF +PV + CV + CPD + Rheuma + PUD + MLD + DCC + HP + Renal + MSLD + AIDS + HT2+ HL2 + Sepsis+ HTT , data = ps, method ='nearest', distance ='logit',  ratio =1)
summary(m.out)
m.data <- match.data(m.out, data = ps, distance = 'prop.score')
id <- m.data %>% distinct(ID)
data3 <- left_join(id, data, by = 'ID')
## check n / figure /smd 
check_n5 <- ff$count_n(data3, "1. dose_type_psmatch")
stat$fig(data3, "1_dose_type_psmatch")
stat$smd(data3, "1_dose_type_psmatch")
###################### dose stat 
print("check dose data:::")
str(data3)
### normality, var, t-test, wilcox (all:: rate, diff)
dose_diff_rate <- stat$dose_diff_rate(data3)
### paired t-test (t vs c, sub analysis by dose type)
dose_paired_test <- stat$ptest_drug(data3)

#################################################################### 4. save ############################################################################

write.csv(test_rate, paste0("/data/results/test_rate.csv")) 
write.csv(test_diff, paste0("/data/results/test_diff.csv")) 
write.csv(paired_test, paste0("/data/results/paired_test.csv")) 
write.csv(dose_diff_rate, paste0("/data/results/dose_diff_rate.csv")) 
write.csv(dose_paired_test, paste0("/data/results/dose_paired_test.csv")) 
count <- rbind(check_n1, check_n2, check_n3, check_n4, check_n5)
write.csv(count, paste0("/data/results/stat_count.csv")) 
