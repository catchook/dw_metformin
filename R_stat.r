
# 0. setting env.
### add year, drug_period
##earliest version 
#basic + year -eGFR

library(psych)
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
library(tableone)
print("library done")
source("/home/syk/R_function.r")
#source("R_function.r")
print("read function module ")
#####argument#######

formula  <-  cohort_type ~ BUN + Creatinine + year + cci + age + gender + SU + alpha+ dpp4i + gnd + sglt2 + tzd + MI + HF +PV + CV + CPD + Rheuma + PUD + MLD + DCC + HP + Renal + MSLD + AIDS + HT2+ HL2 + Sepsis+ HTT 
#############################데이터 합치기:: combine data############################################################################################ 
##파일 변경 
# setwd('/home/syk/data/dw1206')
# filenames <- list.files(path = getwd())
# numfiles <- length(filenames)
# data <- do.call(rbind, lapply(filenames, read.csv))
# print("data read done ")
# file_size <- object.size(data)
#  print("original data size is ")
#  print(file_size, units = "auto")
 #write.csv(data, paste0('/home/syk/data_1206.csv'))
#  print("check data")
#  str(data)
# # # ##############check memory 4 #########################
  print("check memory::: combine data")
  mem_used()

#  ##################################################################### 1. delete outlier #################################################################### 
#  print("# # ## delete value_as_number.before 0 values & delete error & NA")
#     data <- data %>% filter(value_as_number.before != 0)
#     data <- data %>% filter(!grepl('error', drug_group))
#     data$diff <- data$value_as_number.after - data$value_as_number.before
#     data$rate <- data$diff/ data$value_as_number.before  
#     data<- ff$trim4(data)

# # # # ##############check memory 2 #########################
#  print("check memory::: delete outlier")
#  rm(total)
#  file_size <- object.size(data)
#  print("data step 2, size is ")
#  print(file_size, units = "auto")
#  mem_used()
#  print("trim done")
# print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! success delete outlier :: good job!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
# # # #################################################################### 2. propensity score matching  ######################################
# # # # ## select columsn for ps matching
# ps <-  unique(data[, c("cohort_type", "age",  "gender", "year", "SU", "alpha"   ,"dpp4i", "gnd", "sglt2" ,  "tzd" ,  
#                   "BUN",  "Creatinine" ,  "MI"   , "HF"   ,   "PV"     ,   "CV"    ,   "CPD" ,   "Rheuma" , 
#                   "PUD",    "MLD"  , "D"    ,   "DCC" ,  "HP"  ,  "Renal",  "M"  , "MSLD", "MST",  "AIDS"  ,
#                  "HT2" ,   "HL2",  "Sepsis" ,  "HTT"    ,      "ID"      ,   'cci', 'year' )])   
# ps[is.na(ps)] <- 0
# # # # ## convert cohort_Type, age; chr to numeric
# ps$cohort_type <- ifelse(ps$cohort_type =='C', 1, 0)
# ps$gender <- ifelse(ps$gender =='M', 0, 1)

# # # # # ## 2:1 matching
# print("test:: matchit :::: formla version")
# m.out <- matchit(formula , data = ps, method ='nearest', distance ='logit',  ratio = 2)
# summary(m.out)
# m.data <- match.data(m.out, data = ps, distance = 'prop.score')
# id <- m.data %>% distinct(ID)
# data2 <- left_join(id, data, by = 'ID')

# # # # # ##############check memory 2 #########################
# print("check memory::: ps matching")
# file_size <- object.size(ps)
# print("ps, size is ")
# print(file_size, units = "auto")
# file_size <- object.size(data2)
# print("ps + data , size is ")
# print(file_size, units = "auto")
#  rm(ps)
#  rm(m.out)
#  rm(m.data)
#  rm(data)
#  mem_used()
#   print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! success propensity score matching  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
# # # # #################################################################### 3. stat  ############################################################################
# # # # ## remove na
# print("remove na ")
# data2<- data2[ !is.na(data2$measurement_date.after) & !is.na(data2$measurement_date.before) ,] 
#write.csv(data2, "/home/syk/data_1209.csv")
data2 <- read.csv("/home/syk/data_1207.csv")
# print("main stat")
# stat$test_stat(data2)
# #stat$test_rate2(data2, "total")
# print("sub1 anaysis")
# stat$test_sub1(data2)
print("sub2 analysis")
print("define dose group")
#data3 <-stat$dosegroup(data2, 1000, 2) # dose_type1 : >= 1000, dose_type2 : > 1000
#data4 <- stat$dosegroup(data2, 1500, 2)
data5 <- stat$dosegroup(data2, 1500, 3)
rm(data2)
#str(data3)
print(" compare dose group")
#stat$test_dose(data3, 1000,  2 )
#stat$test_dose(data4, 1500,  2 )
stat$test_dose(data5, 1500,  3 )
print("compare dose group vs control")
stat$test_sub2(data5, 1500, 3)
# print("##################################################################### 3-1) high vs low / middle vs low  #############################################################") 
# stat1 <- rbind(high2, low2)
# #stat2 <- rbind(middle2, low2)
# #str(stat1)
# print("start dose_rate2")
# print("1")
# stat$dose_rate3(stat1, 'high')
# #stat$dose_rate3(stat2, "middle")
# stat$dose_diff3(stat1, "high")
# #stat$dose_diff3(stat2, "middle")
# print("##################################################################### 3-2) high vs control / middle vs control / low vs control #################################################################")
# # ## 실험군 vs 대조군의 실험군을 사용해서 고용량, 중용량,  저용량 비교. 
# print("target vs control by dose")
# high2_c <-rbind(high2, control)
# low2_c <- rbind(low2, control)

# # ### dose_rate 
# print("high")
# stat$test_rate2(high2_c, "high")
# print("low")
# stat$test_rate2(low2_c, "low")
# print("start test diff2 ")
# stat$test_diff2(high2_c, "high2")
# stat$test_diff2(low2_c, "low2")
# print("start ptest2 ")
# stat$ptest_drug2(high, "high")
# stat$ptest_drug2(middle, "middle")
# stat$ptest_drug2(low, "low")
# stat$ptest_drug2(high2, "high2")
# stat$ptest_drug2(low2, "low2")
#print('start test rate:: :: add cum_period:: automatic save')
# stat$test_rate(high, "high")
# stat$test_rate(middle, "middle")
# stat$test_rate(low, "low")
# # # print("##################################################################### 3-3) by dose, ptest ##################################################################### #####################################################################") 
#print("start dose ptest2 ")
#stat$dose_ptest_drug2(target)
# # ##############check memory 3 #########################
# print("check memory:::high vs low dose stat")
# rm(data2)
# rm(target)
# rm(control)
# rm(stat1)
# rm(stat2)
# rm(high_c)
# rm(middle_c)
# rm(low_c)
# mem_used()
# # # # #################################################################### 4. save ############################################################################
# ggsave("/data/results/dose_fig.png", fig1, device="png", dpi=300, width=15, height =5)
# write.csv(dose_summary, "/data/results/dose_summary.csv")
print('success good job!')
# # write.csv(dose_diff_rate, paste0("/data/results/test1/dose_diff_rate.csv")) 
# #write.csv(dose_paired_test, paste0("/data/results/test1/dose_paired_test.csv")) 
# # print("rbind count ")
# # count <- rbind(N1,N2,N3)
# # write.csv(count, paste0("/data/results/high_stat_count.csv")) 
# # rm(count)
# # count <- rbind(n1,n2, n3)
# # write.csv(count, paste0("/data/results/low_stat_count.csv")) 
# # # print("rbind count_t")
# # count_t <- rbind(N1_t, N2_t, N3_t)
# # write.csv(count_t, paste0("/data/results/high_stat_count_t.csv")) 
# # rm(count_t)
# # count_t <- rbind(n1_t,n2_t, n3_t)
# # write.csv(count_t, paste0("/data/results/low_stat_count_t.csv")) 
# # ##dose type
# # write.csv(N5, paste0("/data/results/dose_stat_count.csv")) 
# # write.csv(N5_t, paste0("/data/results/dose_stat_count_t.csv")) 


 rm(list=ls())