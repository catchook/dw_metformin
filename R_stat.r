
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
# trim_n = 1.585
# print("trim_n %")
# print(trim_n)

# a : BUN + Creatinine
# b:  BUN + eGFR
# c : Creatinine
# d : eGFR
formula  <-  cohort_type ~ BUN + Creatinine + year + cci + age + gender + SU + alpha+ dpp4i + gnd + sglt2 + tzd + MI + HF +PV + CV + CPD + Rheuma + PUD + MLD + DCC + HP + Renal + MSLD + AIDS + HT2+ HL2 + Sepsis+ HTT 
#############################데이터 합치기:: combine data############################################################################################ 
##파일 변경 
setwd('/home/syk/data/dw1206')
filenames <- list.files(path = getwd())
numfiles <- length(filenames)
data <- do.call(rbind, lapply(filenames, read.csv))
print("data read done ")
file_size <- object.size(data)
print("original data size is ")
print(file_size, units = "auto")
#write.csv(data, paste0('/home/syk/data_1206.csv'))
# print("check data")
# str(data)
# # ##############check memory 4 #########################
print("check memory::: combine data")
mem_used()

##################################################################### 1. delete outlier #################################################################### 
## before trim 
# ### check_n: original N / figure / smd  
# N1 <- ff$count_n( data, '1. before trim')
# N1_t <- ff$count_total( data, '1. before trim')
# stat$fig(data,  '1_before_trim', 5, 0.5)
# stat$smd(data, '1_before_trim')
# print("check original::")
# ## trim n%
# ### check_n: trim N / figure / smd
# N2 <- ff$count_n( data, "2. trim 3%")
# N2_t <- ff$count_total( data, '2. trim 3%')
# print("check_n2:: trim 3%")
# stat$fig(data, "2_trim", 1, 0.05)
# stat$smd(data, "2_trim")

print("# # ## delete value_as_number.before 0 values & delete error & NA")
data <- data %>% filter(value_as_number.before != 0)
data <- data %>% filter(!grepl('error', drug_group))
data <- data[!is.na(data$drug_group),]
copy <- as.data.table(data)
target <- copy[cohort_type =="T" & measurement_type =='CRP',.(ID, cohort_type, measurement_type, hospital, value_as_number.before, value_as_number.after)]
control <- copy[cohort_type =="C" & measurement_type =='CRP',.(ID, cohort_type, measurement_type, hospital, value_as_number.before, value_as_number.after)]
target <- na.omit(target)
control <- na.omit(control)
t1  <- summary(target)
t11 <- describe(target[,.(value_as_number.before, value_as_number.after)], quant =c(.25, .75))
c1 <- summary(control)
c11 <- describe(control[,.(value_as_number.before, value_as_number.after)], quant =c(.25, .75))
R1 <- rbind(t1, c1)
R11 <- rbind(t11, c11)
write.csv(R1, "/data/results/R1.csv")
write.csv(R11, "/data/results/R11.csv")
print("trim  % ")

rm(copy)
rm(target)
rm(control)
# file_size <- object.size(data)
# print("delete outlier data size is ")
# print(file_size, units = "auto")
# print("delete value_as_number.before 0 values")
# N3 <- ff$count_n( data, "3. delete outlier")
# N3_t <- ff$count_total( data, "3. delete outlier")
# stat$fig(data, "3_delete_outlier", 1, 0.05)
# stat$smd(data, "3_delte_outlier")
## new columns: rate, diff
data$diff <- data$value_as_number.after - data$value_as_number.before
data$rate <- data$diff/ data$value_as_number.before  
# ## delete drug_group; error & NA
#data<- ff$trim(data, trim_n)
data<- ff$trim4(data)

copy <- as.data.table(data)
target <- copy[cohort_type =="T" & measurement_type =='CRP',.(ID, cohort_type, measurement_type, hospital, value_as_number.before, value_as_number.after)]
control <- copy[cohort_type =="C" & measurement_type =='CRP',.(ID, cohort_type, measurement_type, hospital, value_as_number.before, value_as_number.after)]
target <- na.omit(target)
control <- na.omit(control)
t2  <- summary(target)
t21 <- describe(target[,.(value_as_number.before, value_as_number.after)], quant =c(.25, .75))
c2 <- summary(control)
c21 <- describe(control[,.(value_as_number.before, value_as_number.after)], quant =c(.25, .75))
R2 <- rbind(t2, c2)
R21 <- rbind(t21, c21)
write.csv(R2, "/data/results/R2.csv")
write.csv(R21, "/data/results/R21.csv")
rm(copy)
rm(target)
rm(control)
# # # ##############check memory 2 #########################
# print("check memory::: delete outlier")
# # rm(total)
# # rm(data)
# file_size <- object.size(data)
# print("data step 2, size is ")
# print(file_size, units = "auto")
# mem_used()
# data<- read.csv("/home/syk/data12012.csv")
# data <- ff$trim4(data)
# print("trim done")

# #write.csv(data, '/home/syk/data.csv')
# print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! success delete outlier :: good job!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
# # #################################################################### 2. propensity score matching  ######################################
# # # ## select columsn for ps matching
ps <-  unique(data[, c("cohort_type", "age",  "gender", "year", "SU", "alpha"   ,"dpp4i", "gnd", "sglt2" ,  "tzd" ,  
                  "BUN",  "Creatinine" ,  "MI"   , "HF"   ,   "PV"     ,   "CV"    ,   "CPD" ,   "Rheuma" , 
                  "PUD",    "MLD"  , "D"    ,   "DCC" ,  "HP"  ,  "Renal",  "M"  , "MSLD", "MST",  "AIDS"  ,
                 "HT2" ,   "HL2",  "Sepsis" ,  "HTT"    ,      "ID"      ,   'cci', 'year' )])   
ps[is.na(ps)] <- 0
# # ## convert cohort_Type, age; chr to numeric
ps$cohort_type <- ifelse(ps$cohort_type =='C', 1, 0)
ps$gender <- ifelse(ps$gender =='M', 0, 1)

# # # # ## 2:1 matching
print("test:: matchit :::: formla version")
m.out <- matchit(formula , data = ps, method ='nearest', distance ='logit',  ratio = 2)
summary(m.out)
m.data <- match.data(m.out, data = ps, distance = 'prop.score')
id <- m.data %>% distinct(ID)
data2 <- left_join(id, data, by = 'ID')

#write.csv(data2, "/home/syk/data.csv")
# # ## check n / figure /smd 
# # N4 <- ff$count_n(data2, "4. ps_matching")
# # N4_t <- ff$count_total(data2, "4. ps_matching")
# # stat$fig(data2, "4_psmatch" , 1, 0.01)
#stat$smd(data2, "psmatch")

# # # # ##############check memory 2 #########################
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
 rm(data)
 mem_used()
 print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! success propensity score matching  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
# # #################################################################### 3. stat  ############################################################################
# # ## remove na
print("remove na ")
data2<- data2[ !is.na(data2$measurement_date.after) & !is.na(data2$measurement_date.before) ,] 
print("check stat data:::")

# str(data2)
# write.csv(data2, "/home/syk/data.csv") 
# # ## 3-1) target vs control  
### normality, var, t-test, wilcox
 print("start test rate2 ")
data2 <- as.data.frame(data2)
print("for dw , crp summry ")
copy <- as.data.table(data2)
target <- copy[cohort_type =="T" & measurement_type =='CRP',.(ID, cohort_type, measurement_type, hospital, value_as_number.before, value_as_number.after)]
control <- copy[cohort_type =="C" & measurement_type =='CRP',.(ID, cohort_type, measurement_type, hospital, value_as_number.before, value_as_number.after)]
target <- na.omit(target)
control <- na.omit(control)
t3  <- summary(target)
t31 <- describe(target[,.(value_as_number.before, value_as_number.after)], quant =c(.25, .75))
c3 <- summary(control)
c31 <- describe(control[,.(value_as_number.before, value_as_number.after)], quant =c(.25, .75))
R3 <- rbind(t3, c3)
R31 <- rbind(t31, c31)
write.csv(R3, "/data/results/R3.csv")
write.csv(R31, "/data/results/R31.csv")
rm(copy)
rm(target)
rm(control)



stat$test_rate2(data2, 'total')
#stat$test_rate(data2, "total")
# # N5 <- ff$count_n(data2, "5. test_rate")
# # N5_t <- ff$count_total(data2, "5. test_rate")
print("start test diff2 ")
stat$test_diff2(data2, 'total')
print("start ptest2 ")
stat$ptest_drug2(data2, "total")

print("#####################################용량군, 비용량군 crp 전후 비교 ####################################################")

# # ##############check memory 3 #########################
print("check memory::: stat")
mem_used()
print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!success target vs control stat!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ")
print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! DEfine  dose_type  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
# ### 용량별 분포 확인
print(" define dose group seperately")
print("dose group change")
data2$dose_type <- ifelse(data2$total_dose > 1500,  "high", "low") #메일 요청사항 , ifelse(dat1$total_dose >= 1500, "high" , "low") #추가 요청사항
## 용량군 재정의 
## 3개 병원은 dose 로 정의. 나머지 병원은 그대로. 
dat1 <- data2 %>% filter(hospital %in% c("CHAMC", "KHMC"))%>% select(-c(total_dose, dose_type, dose_list2))
#  print("show me dat1") #
dat1$dose_list2 <- lapply(dat1$dose_list1, function(x) strsplit(unlist(x), ","))
dat1$total_dose <- sapply(dat1$dose_list2, function(x) sum(as.numeric(unlist(x))))
# data2$dose_type <- ifelse(data2$total_dose > 1000, ifelse(data2$total_dose ) "high", "low") #메일 요청사항 
# print("start cum drug period")
# stat$cum_period(data2)
print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! grouping dose type !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
# #####
target <- data2 %>% filter(cohort_type =='T')
 #fig1<-ggplot(target, aes(x = total_dose )) + geom_histogram( color ='gray80', alpha=0.2, position="identity", binwidth=10) +  geom_density() + xlim(0, 3000)
# dose_summary <-as.array(summary(target$total_dose))
stat$dose_smd(target)
control <- data2 %>% dplyr::filter(cohort_type=='C')
# ### 용량 군 정의 
print("start 2 dose group define ")
high2 <- target[which(target$dose_type == 'high'),]
low2  <- target[which(target$dose_type == 'low'),]
n5 <- length(unique(high2$ID) )
n7 <- length(unique(low2$ID) )
print( paste("high dose :", n5, "low dose:", n7) )
print("##################################################################### 3-1) high vs low / middle vs low  #############################################################") 
stat1 <- rbind(high2, low2)
#stat2 <- rbind(middle2, low2)
#str(stat1)
print("start dose_rate2")
print("1")
stat$dose_rate3(stat1, 'high')
#stat$dose_rate3(stat2, "middle")
stat$dose_diff3(stat1, "high")
#stat$dose_diff3(stat2, "middle")
print("##################################################################### 3-2) high vs control / middle vs control / low vs control #################################################################")
# ## 실험군 vs 대조군의 실험군을 사용해서 고용량, 중용량,  저용량 비교. 
print("target vs control by dose")
high2_c <-rbind(high2, control)
low2_c <- rbind(low2, control)

# ### dose_rate 
print("high")
stat$test_rate2(high2_c, "high")
print("low")
stat$test_rate2(low2_c, "low")
print("start test diff2 ")
stat$test_diff2(high2_c, "high2")
stat$test_diff2(low2_c, "low2")
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