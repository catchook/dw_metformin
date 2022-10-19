
# 0. setting env.
### add year, drug_period
##earliest version 
#basic + year -eGFR
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
#####argument#######
trim_n = 4
print("trim_n %")
print(trim_n)
print("4 d ")
# a : BUN  + Creatinine
# b: Creatinine
# c : hospital + BUN  + Creatinine
# d : hospital + Creatinine
formula <-  cohort_type ~  hospital +  Creatinine + year + cci + age + gender + SU + alpha+ dpp4i + gnd + sglt2 + tzd + MI + HF +PV + CV + CPD + Rheuma + PUD + MLD + DCC + HP + Renal + MSLD + AIDS + HT2+ HL2 + Sepsis+ HTT 
##############################데이터 합치기:: combine data############################################################################################ 

setwd('/home/syk/data')
filenames <- list.files(path = getwd())
numfiles <- length(filenames)
#data <- fread("/home/syk/data.csv", fill =TRUE)
data <- do.call(rbind, lapply(filenames, read.csv))
#write.csv(total, '/home/syk/total.csv')
print("data read done ")
file_size <- object.size(data)
print("original data size is ")
print(file_size, units = "auto")
#write.csv(data, paste0('data.csv'))
print("check data")
str(data)
# # ##############check memory 4 #########################
print("check memory::: combine data")
mem_used()
##################################################################### 1. delete outlier #################################################################### 
## before trim 
# ### check_n: original N / figure / smd  
N1 <- ff$count_n( data, '1. before trim')
N1_t <- ff$count_total( data, '1. before trim')
stat$fig(data,  '1_before_trim', 5, 0.5)
stat$smd(data, '1_before_trim')
print("check original::")
# ## trim n%
# ### check_n: trim N / figure / smd
data <- ff$trim(data, trim_n)
print("trim done")

# N2 <- ff$count_n( data, "2. trim 3%")
# N2_t <- ff$count_total( data, '2. trim 3%')
# print("check_n2:: trim 3%")
# stat$fig(data, "2_trim", 1, 0.05)
# stat$smd(data, "2_trim")

# # ## delete value_as_number.before 0 values
data <- data %>% filter(value_as_number.before != 0)
file_size <- object.size(data)
print("delete outlier data size is ")
print(file_size, units = "auto")
print("delete value_as_number.before 0 values")
# N3 <- ff$count_n( data, "3. delete outlier")
# N3_t <- ff$count_total( data, "3. delete outlier")
# stat$fig(data, "3_delete_outlier", 1, 0.05)
# stat$smd(data, "3_delte_outlier")
## new columns: rate, diff
data$diff <- data$value_as_number.after - data$value_as_number.before
data$rate <- data$diff/ data$value_as_number.before  
## delete drug_group; error & NA
data <- data %>% filter(!grepl('error', drug_group))
data <- data[!is.na(data$drug_group),]

# # # ##############check memory 2 #########################
print("check memory::: delete outlier")
# rm(total)
# rm(data)
file_size <- object.size(data)
print("data step 2, size is ")
print(file_size, units = "auto")
mem_used()
print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! success delete outlier :: good job!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
# #################################################################### 2. propensity score matching  ######################################
## select columsn for ps matching
ps <-  unique(data[, c("cohort_type", "age",  "gender", "SU", "alpha"   ,"dpp4i", "gnd", "sglt2" ,  "tzd" ,  
                  "BUN",  "Creatinine" ,  "MI"   , "HF"   ,   "PV"     ,   "CV"    ,   "CPD" ,   "Rheuma" , 
                  "PUD",    "MLD"      ,   "DCC" ,  "HP"  ,  "Renal",  "MSLD"  ,  "AIDS"   ,   "HT2" ,   "HL2",   
                  "Sepsis" ,  "HTT"    ,      "ID"      ,    "egfr",  'cci', 'year', 'hospital' )])   
ps$egfr[is.na(ps$egfr)] <- 90
ps[is.na(ps)] <- 0
# ## convert cohort_Type, age; chr to numeric
ps$cohort_type <- ifelse(ps$cohort_type =='C', 1, 0)
ps$gender <- ifelse(ps$gender =='M', 0, 1)
# # ## 2:1 matching
print("test:: matchit :::: formla version")
m.out <- matchit(formula , data = ps, method ='nearest', distance ='logit',  ratio =2)
summary(m.out)
m.data <- match.data(m.out, data = ps, distance = 'prop.score')
id <- m.data %>% distinct(ID)
data2 <- left_join(id, data, by = 'ID')
# ## check n / figure /smd 
N4 <- ff$count_n(data2, "4. ps_matching")
N4_t <- ff$count_total(data2, "4. ps_matching")
# stat$fig(data2, "4_psmatch" , 1, 0.01)
stat$smd(data2, "4_psmatch")
# # ##############check memory 2 #########################
print("check memory::: ps matching")
file_size <- object.size(ps)
print("ps, size is ")
print(file_size, units = "auto")
file_size <- object.size(data2)
print("ps + data , size is ")
print(file_size, units = "auto")
rm(ps)
rm(m.out)
rm(m.data)
mem_used()
print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! success propensity score matching  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
#################################################################### 3. stat  ############################################################################
#data2 <- fread("data.csv", fill =TRUE)
## remove na
data2 <- data2[!is.na(data2$drug_exposure_end_date),]
data2 <- data2[!is.na(data2$drug_exposure_start_date),] 
data2 <- data2[!is.na(data2$measurement_date.after),]
data2 <- data2[!is.na(data2$measurement_date.before),]
print("check stat data:::")
str(data2)
## 3-1) target vs control  
print("start cum drug period")
stat$cum_period(data2)
### normality, var, t-test, wilcox
print("start test rate2 ")
stat$test_rate2(data2)
print("start test diff2 ")
stat$test_diff2(data2)
print("start ptest2 ")
stat$ptest_drug2(data2)

print('start test rate:: :: add cum_period:: automatic save')
stat$test_rate(data2)
print("start test diff")
stat$test_diff(data2)
# ### paired t-test (t vs c)
print("start paired_test")
stat$ptest_drug(data2)

print("start cum drug period")
stat$cum_period(data2)

print('success good job!')
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


print('success good job!')
# write.csv(dose_diff_rate, paste0("/data/results/dose_diff_rate.csv")) 
# write.csv(dose_paired_test, paste0("/data/results/dose_paired_test.csv")) 
# print("rbind count ")
count <- rbind(N1, N4)
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
count_t <- rbind(N1_t,N4_t)
write.csv(count_t, paste0("/data/results/stat_count_t.csv")) 
# ##dose type
# write.csv(N5, paste0("/data/results/dose_stat_count.csv")) 
# write.csv(N5_t, paste0("/data/results/dose_stat_count_t.csv")) 

