# 0. setting env.

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
library(plyr)
library(ids)
library(MatchIt)
library(purrr)
library(tidyr)
library(pryr)
library(moonBook)
print("library done")
# source("/home/syk/R_function.r")
source("R_function.r")
print("read function module ")
##############################데이터 합치기:: combine data############################################################################################ 
# setwd('/home/syk/data')
# filenames <- list.files(path = getwd())
# numfiles <- length(filenames)
# #data <- fread("/home/syk/data.csv", fill =TRUE)
# total <- do.call(rbind, lapply(filenames, read.csv))
# #write.csv(total, '/home/syk/total.csv')
# print("data read done ")
# str(total)
# # ##############check memory 4 #########################
# print("check memory::: combine data")
# mem_used()
################## SELECT LATEST DATA 
#  ## select latest measurement_data
# data2 <- data2 %>% dplyr::arrange( ID, measurement_type, desc(measurement_date.after)) %>% dplyr::group_by(ID, measurement_type) %>% dplyr::mutate( row = row_number())
# print("1")
# data2<- as.data.frame(data2)
# data2 <- data2 %>% dplyr::filter(row == 1)
# print("2")
# data2 <- as.data.frame(data2)
# data2 <- subset(data2, select = -c(row))
# print("select latest data")
# str(data2)
##################################################################### 1. delete outlier #################################################################### 
## before trim 
# ### check_n: original N / figure / smd  
# N1 <- ff$count_n( total, '1. before trim')
# N1_t <- ff$count_total( total, '1. before trim')
# stat$fig(total,  '1_before_trim')
# stat$smd(total, '1_before_trim')
# print("check original::")
# # ## trim n%
# # ### check_n: trim N / figure / smd
# data <- ff$trim(total, 3)
# print("trim done")

# N2 <- ff$count_n( data, "2. trim 3%")
# N2_t <- ff$count_total( data, '2. trim 3%')
# print("check_n2:: trim 3%")
# stat$fig(data, "2_trim")
# stat$smd(data, "2_trim")

# # ## delete value_as_number.before 0 values
# data1 <- data %>% filter(value_as_number.before != 0)
# print("delete value_as_number.before 0 values")
# N3 <- ff$count_n( data1, "3. delete outlier")
# N3_t <- ff$count_total( data1, "3. delete outlier")
# stat$fig(data1, "3_delete_outlier")
# stat$smd(data1, "3_delte_outlier")
# ## new columns: rate, diff
# data1$diff <- data1$value_as_number.after - data1$value_as_number.before
# data1$rate <- data1$diff/ data1$value_as_number.before  
# # # ##############check memory 2 #########################
# print("check memory::: delete outlier")
# rm(total)
# rm(data)
# mem_used()
# print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! success delete outlier  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
# #################################################################### 2. propensity score matching  ######################################
# ## select columsn for ps matching
# ps <-  unique(data1[, c("cohort_type", "age",  "gender", "SU", "alpha"   ,"dpp4i", "gnd", "sglt2" ,  "tzd" ,  
#                   "BUN",  "Creatinine" ,  "MI"   , "HF"   ,   "PV"     ,   "CV"    ,   "CPD" ,   "Rheuma" , 
#                   "PUD",    "MLD"      ,   "DCC" ,  "HP"  ,  "Renal",  "MSLD"  ,  "AIDS"   ,   "HT2" ,   "HL2",   
#                   "Sepsis" ,  "HTT"    ,      "ID"      ,    "egfr",  'cci' )])   
# ps$egfr[is.na(ps$egfr)] <- 90
# colSums(is.na(ps))
# ps[is.na(ps)] <- 0
# ## convert cohort_Type, age; chr to numeric
# ps$cohort_type <- ifelse(ps$cohort_type =='C', 1, 0)
# ps$gender <- ifelse(ps$gender =='M', 0, 1)
# # ## 2:1 matching
# m.out <- matchit(cohort_type ~ cci + age + gender + Creatinine + BUN + egfr + SU + alpha+ dpp4i + gnd + sglt2 + tzd + MI + HF +PV + CV + CPD + Rheuma + PUD + MLD + DCC + HP + Renal + MSLD + AIDS + HT2+ HL2 + Sepsis+ HTT , data = ps, method ='nearest', distance ='logit',  ratio =2)
# summary(m.out)
# m.data <- match.data(m.out, data = ps, distance = 'prop.score')
# id <- m.data %>% distinct(ID)
# data2 <- left_join(id, data1, by = 'ID')
# ## check n / figure /smd 
# N4 <- ff$count_n(data2, "4. ps_matching")
# N4_t <- ff$count_total(data2, "4. ps_matching")
# stat$fig(data2, "4_psmatch")
# stat$smd(data2, "4_psmatch")
# # ##############check memory 2 #########################
# print("check memory::: ps matching")
# rm(ps)
# rm(m.out)
# rm(m.data)
# mem_used()
# print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! success propensity score matching  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
#################################################################### 3. stat  ############################################################################
data2 <- fread("./data/data.csv", fill =TRUE)
print("check stat data:::")
str(data2)
## 3-1) target vs control  
### normality, var, t-test, wilcox
print("start test rate ")
stat$test_rate2(data2)
# data2 <- setDT(data2)
# stat<- data2[,.(ID, cohort_type, measurement_type,  rate)]
# stat<- unique(stat)
# rate <- dcast(stat, ID + cohort_type ~ measurement_type, value.var = c('rate'))
# names(rate)[names(rate) == 'Total cholesterol'] <-  c("Total_cholesterol")
# colSums(!is.na(rate))
# tb <- mytable(cohort_type ~ CRP + ESR + BUN + Triglyceride + SBP + Total_cholesterol + Hb + Glucose_Fasting + Creatinine +  HDL + AST + Albumin + insulin +
# BMI + HbA1c + DBP +  LDL + NT-proBNP , data = rate,  method = 3,  catMethod = 0, show.all = T) 
# print("rate test mytable")
# print(tb)
# mycsv(tb, file='test2.csv')

test_rate <- stat$test_rate(data2)
print("start test diff")
stat$test_diff2(data2)
test_diff <- stat$test_diff(data2)
### paired t-test (t vs c)
print("start paired_test")
stat$ptest_drug2(data2)
paired_test <- stat$ptest_drug(data2)
# # ##############check memory 3 #########################
print("check memory::: stat")
rm(data2)
mem_used()
print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!success target vs control stat!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ")
# ## 3-2) high vs low 
# ###################### ps matching
# ## select only target 
# target <- data1 %>% dplyr::filter(cohort_type=='T') 
# rm(data1)
# ps <-  unique(target[, c( "age",  "gender", "SU", "alpha"   ,"dpp4i", "gnd", "sglt2" ,  "tzd" ,  
#                   "BUN",  "Creatinine" ,  "MI"   , "HF"   ,   "PV"     ,   "CV"    ,   "CPD" ,   "Rheuma" , 
#                   "PUD",    "MLD"      ,   "DCC" ,  "HP"  ,  "Renal",  "MSLD"  ,  "AIDS"   ,   "HT2" ,   "HL2",   
#                   "Sepsis" ,  "HTT"    ,   "ID"  ,    "egfr",  'cci' , 'dose_type' )])   
# ps$egfr[is.na(ps$egfr)] <- 90
# colSums(is.na(ps))
# ps[is.na(ps)] <- 0
# ## convert cohort_Type, age; chr to numeric
# #ps$cohort_type <- ifelse(ps$cohort_type =='C', 1, 0)
# ps$gender <- ifelse(ps$gender =='M', 0, 1)
# ps$dose_type <- ifelse(ps$dose_type =='high', 1 , 0)
# ## 1 : 1 matching
# m.out <- matchit(dose_type ~ cci + age + gender + Creatinine + BUN + egfr + SU + alpha+ dpp4i + gnd + sglt2 + tzd + MI + HF +PV + CV + CPD + Rheuma + PUD + MLD + DCC + HP + Renal + MSLD + AIDS + HT2+ HL2 + Sepsis+ HTT , data = ps, method ='nearest', distance ='logit',  ratio =1)
# summary(m.out)
# m.data <- match.data(m.out, data = ps, distance = 'prop.score')
# id <- m.data %>% distinct(ID)
# data3 <- left_join(id, target, by = 'ID')
# # ##############check memory 3 #########################
# print("check memory:::high vs low psmatching")
# rm(ps)
# rm(m.out)
# rm(m.data)
# rm(target)
# mem_used()
# print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!  HIGH VS LOW ::: success propensity score matching  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
# ## check n / figure /smd 
# N5 <- ff$dose_count_n( data3, "1. dose_type_psmatch")
# N5_t <- ff$dose_count_total( data3, "1. dose_type_psmatch")
# print("N5_t")
# str(N5_t)
# stat$dose_fig(data3, "1_dose_type_psmatch")
# stat$dose_smd(data3, "1_dose_type_psmatch")
# print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!  HIGH VS LOW ::: SUCCESS check N , FIG, SMD  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
# ###################### dose stat 
# print("check dose data:::")
# str(data3)
# ### normality, var, t-test, wilcox (all:: rate, diff)
# dose_diff_rate <- stat$dose_diff_rate(data3)
# ### paired t-test (sub analysis by dose type)
# dose_paired_test <- stat$dose_ptest(data3)
# # ##############check memory 3 #########################
# print("check memory:::high vs low dose stat")
# rm(data3)
# mem_used()
# print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!  HIGH VS LOW ::: SUCCESS dose stat !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
# #################################################################### 4. save ############################################################################

write.csv(test_rate, paste0("/data/results/test_rate1.csv")) 
write.csv(test_diff, paste0("/data/results/test_diff1.csv")) 
write.csv(paired_test, paste0("/data/results/paired_test1.csv")) 
# write.csv(dose_diff_rate, paste0("/data/results/dose_diff_rate.csv")) 
# write.csv(dose_paired_test, paste0("/data/results/dose_paired_test.csv")) 
print("rbind count ")
count <- rbind(N1, N2, N3, N4)
write.csv(count, paste0("/data/results/stat_count.csv")) 
# print("rbind count_t")
# N1_t <- subset(N1_t, select = c(C, T, step))
# N2_t <- subset(N2_t, select = c(C, T, step))
# print("N1_T")
# str(N1_t)
# print("N2_t")
# str(N2_t)
# print("N3_t")
# str(N3_t)
# print("N4_t")
# str(N4_t)
count_t <- rbind(N1_t, N2_t, N3_t, N4_t)
write.csv(count_t, paste0("/data/results/stat_count_t.csv")) 
# ##dose type
# write.csv(N5, paste0("/data/results/dose_stat_count.csv")) 
# write.csv(N5_t, paste0("/data/results/dose_stat_count_t.csv")) 

