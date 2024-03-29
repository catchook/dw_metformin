# env setting
# install.packages("modules")
# install.packages("dplyr")
# install.packages("MatchIt")
# install.packages("tableone")
# install.packages("optmatch")
# install.packages("moonBook")
# install.packages("data.table")
# install.packages("modules")
# install.packages("ggplot2")
# install.packages("dplyr")
# install.packages("purrr")
# install.packages("tidyr")
# install.packages("gridExtra")
# install.packages("cowplot")
# install.packages("knitr")
# install.packages("sjlabelled")
# install.packages("lawstat")
# install.packages("gplots")
# install.packages("nortest")
# install.packages("stringr")
# install.packages("reshape2")
# install.packages("reshape")
# install.packages("strex")

##library 
library(moonBook)
library(MatchIt)
library(dplyr)
library(tableone)
library(optmatch)
library(data.table)
library(modules)
library(ggplot2)
library(purrr)
library(tidyr)
library(gridExtra)
library(cowplot)
library(knitr)
library(sjlabelled)
library(lawstat)
library(gplots)
library(nortest)
library(stringr)
library(reshape2)
library(reshape)
library(strex)
library(modules)
library(data.table)
library(RPostgreSQL)
library(lubridate)
library(ggpubr)
library(base)

# ## 기본 함수
ff <- module({
    import("DBI") 
    import("RSQLite")
    import("dplyr")
    import("ggplot2")
    import("gridExtra")
    import("cowplot")
    import("sjlabelled")
    import("moonBook")
    import("stats")
    import("gplots")
    import("stats")
    import("lawstat")
    import("grDevices")
    import("nortest")
    import('data.table')
    import('RPostgreSQL')
    import('lubridate')
    import('base')
    import('utils')
    import('tidytable')
    import('reshape2')
    import('tidyr')

save_query <- function(sql, schema, db_target, db_control, con){
    sql <- gsub("_results_fnet_v276", "_results_dq_v276", sql)
    sql <- gsub("cdm_hira_2017", schema, sql) 
    sql <- gsub("target", db_target, sql)
    sql <- gsub("control", db_control, sql)
    result <- dbGetQuery(con, sql)
    result <- unique(result)
    result <- as.data.frame(result)
    return(result)
    }

count_n <- function(df, step){
  table<-df %>% group_by(measurement_type, cohort_type) %>% summarise(pl = n_distinct(ID)) %>% arrange(measurement_type)
  tt <- as.data.frame(table)
  groups <- split(tt, tt$cohort_type)
  print("start header.true function in count_n ")
  header.true <- function(df) {
  names(df) <- as.character(unlist(df[1,]))
  df[-1,] }
  target <- as.data.frame(t(groups$T[, c('measurement_type', "pl")]))
  target <- header.true(target)
  control <- as.data.frame(t(groups$C[, c('measurement_type',"pl")]))
  control <- header.true(control)
  cols <- c("CRP", "ESR","BUN", "Triglyceride",  "SBP", "Hb", 'Glucose_Fasting',  'Creatinine', "HDL",  "AST", "Albumin", "insulin", "BMI", "HbA1c", "DBP",'Total cholesterol', 'LDL', 'NT-proBNP')
  n = length(cols)
  control_list = vector("list", length = n)
  target_list = vector("list", length = n )
  for(i in cols) {
    x <- ifelse(is.null(control[[i]]), 0, control[[i]])
    y <- ifelse(is.null(target[[i]]), 0, target[[i]])
    control_list[[i]] <- x
    target_list[[i]] <- y
      }
  control <- t(do.call(rbind, control_list))
  target <- t(do.call(rbind, target_list))
  total <- data.frame(cohort_type=c('Control' ,'Target'), rbind(control, target))
  total$step <- step
  print("show count_n")
  str(total)
  ###################### delete intermediate file 
  rm(table)
  rm(tt)
  rm(groups)
  rm(target)
  rm(control)
  ######################
  return(total)
  }

 count_total <- function(df, step){
  table  <- df %>% group_by(cohort_type) %>% summarise(N= n_distinct(ID))
  ttable <- t(table)
  ttable  <- as.data.frame(ttable)
  print("start header.true function in count_total")
  header.true <- function(df) {
  names(df) <- as.character(unlist(df[1,]))
  df[-1,] }
  table  <- header.true(ttable)
  table$step <- step
  table <- subset(table, select = c( T, C , step))
  print("show count_total")
  str(table)
  ###################### delete intermediate file 
  rm(ttable)
  ######################
  return(table)
 }


dose_count_n <- function(df, step){
  table<-df %>% group_by(measurement_type, dose_type) %>% summarise(pl = n_distinct(ID)) %>% arrange(measurement_type)
  tt <- as.data.frame(table)
  groups <- split(tt, tt$dose_type)
  print("dose_count_n::: start header.true function in count_n ")
  header.true <- function(df) {
  names(df) <- as.character(unlist(df[1,]))
  df[-1,] }
  high <- as.data.frame(t(groups$high[, c('measurement_type', "pl")]))
  high <- header.true(high)
  low <- as.data.frame(t(groups$low[, c('measurement_type',"pl")]))
  low <- header.true(low)
  cols <- c("CRP", "ESR","BUN", "Triglyceride",  "SBP", "Hb", 'Glucose_Fasting',  'Creatinine', "HDL",  "AST", "Albumin", "insulin", "BMI", "HbA1c", "DBP",'Total cholesterol', 'LDL', 'NT-proBNP')
  n = length(cols)
  high_list = vector("list", length = n)
  low_list = vector("list", length = n )
  for(i in cols) {
    x <- ifelse(is.null(high[[i]]), 0, high[[i]])
    y <- ifelse(is.null(low[[i]]), 0, low[[i]])
    high_list[[i]] <- x
    low_list[[i]] <- y
      }
  high <- t(do.call(rbind, high_list))
  low <- t(do.call(rbind, low_list))
  total <- data.frame(dose_type=c('high' ,'low'), rbind(high, low))
  total$step <- step
  print("show count_n")
  str(total)
  ###################### delete intermediate file 
  rm(table)
  rm(tt)
  rm(groups)
  rm(high)
  rm(low)
  ######################
  return(total)
  }

 dose_count_total <- function(df, step){
  table  <- df %>% group_by(dose_type) %>% summarise(N= n_distinct(ID))
  ttable <- t(table)
  ttable  <- as.data.frame(ttable)
  print("start header.true function in count_total")
  header.true <- function(df) {
  names(df) <- as.character(unlist(df[1,]))
  df[-1,] }
  table  <- header.true(ttable)
  table$step <- step
  print("dose_count_total:::show count_total")
  str(table)
  ###################### delete intermediate file 
  rm(ttable)
  ######################
  return(table)
 }


chr_to_date <- function(data){
  chr_cols <- c("cohort_start_date", "cohort_end_date", "drug_exposure_start_date", "drug_exposure_end_date")
  data[, chr_cols] <- lapply(data[, chr_cols], as.Date)
  return(data)
}
                  ## t1 데이터 합치기.
trim <- function(data, n) {
a <- (100-n)*0.01
b <- n*0.01 
print("how much trim?")
print(b)
print("replace cr, bun null to 1, 10 ")
data$Creatinine[is.na(data$Creatinine)] <- 1
data$BUN[is.na(data$BUN)] <- 10
cr_high <- stats::quantile(data$Creatinine, a, na.rm =TRUE)
cr_low <- stats::quantile(data$Creatinine, b, na.rm = TRUE)
bun_high <- stats::quantile(data$BUN, a, na.rm = TRUE)
bun_low <- stats::quantile(data$BUN, b, na.rm = TRUE)
data <- subset(data, cr_low <= Creatinine & Creatinine <= cr_high )
data <- subset(data, bun_low <= BUN & BUN <= bun_high )
#data <- as.data.table(data)
#data_c <-data[measurement_type =='CRP',]
#data<- data[measurement_type !='CRP',]
#print("replace cr, bun null to 1, 10 ")
#crp_high <- stats::quantile(data_c$value_as_number.before, a, na.rm =TRUE)
#crp_max <- max(data_c$value_as_number.before, na.rm =TRUE)
#crp_low <- stats::quantile(data$value_as_number.before, b, na.rm = TRUE)
#crp_high2 <- stats::quantile(data_c$value_as_number.after, a, na.rm =TRUE)

#crp_max2 <- max(data_c$value_as_number.after, na.rm =TRUE)
#crp_low2 <- stats::quantile(data$value_as_number.after, b, na.rm = TRUE)
#data_c <- subset(data_c,  value_as_number.before < crp_max )
#data_c <- subset(data_c,  value_as_number.after < crp_max2 )
#data <- rbind(data, data_c)
return(data)
}

trim2 <- function(data, n) {
a <- (100-n)*0.01
b <- n*0.01 
print("how much trim?")
print(b)
print("select only crp")
data <- as.data.table(data)
data_c <-data[measurement_type =='CRP',]
data<- data[measurement_type !='CRP',]
#print("replace cr, bun null to 1, 10 ")
crp_high <- stats::quantile(data_c$value_as_number.before, a, na.rm =TRUE)
#crp_low <- stats::quantile(data$value_as_number.before, b, na.rm = TRUE)
crp_high2 <- stats::quantile(data_c$value_as_number.after, a, na.rm =TRUE)
#crp_low2 <- stats::quantile(data$value_as_number.after, b, na.rm = TRUE)
data_c <- subset(data_c,  value_as_number.before <= crp_high )
data_c <- subset(data_c,  value_as_number.after <= crp_high2 )
data <- rbind(data, data_c)

return(data)
}
trim3 <- function(data){

  data$Creatinine[is.na(data$Creatinine)] <- 1
  data$BUN[is.na(data$BUN)] <- 10
  print("na ")
  # cr_high <- stats::quantile(data$Creatinine, a, na.rm =TRUE)
  # cr_low <- stats::quantile(data$Creatinine, b, na.rm = TRUE)
  # bun_high <- stats::quantile(data$BUN, a, na.rm = TRUE)
  # bun_low <- stats::quantile(data$BUN, b, na.rm = TRUE)
  # data <- subset(data, cr_low <= Creatinine & Creatinine <= cr_high )
  # data <- subset(data, bun_low <= BUN & BUN <= bun_high )
  # data <- subset(data, Creatinine <= cr_high )
  # data <- subset(data, BUN <= bun_high )
  print("crp, bun high")

   data<-data[-which(data$BUN>summary(data$BUN)[5] + 1.5*IQR(data$BUN)),]
   data<-data[-which(data$Creatinine> summary(data$Creatinine)[5] + 1.5*IQR(data$Creatinine)),]
  # print("select only crp")
  data <- as.data.table(data)
  data_c <-data[measurement_type =='CRP',]
  data<- data[measurement_type !='CRP',]
  data_c <- data_c[!is.na(data_c$value_as_number.after),]
  data_c <- data_c[!is.na(data_c$value_as_number.before),]
  print("1")
  #data_c <-data_c[-which(data_c$value_as_number.before>summary(data_c$value_as_number.before)[5] + 1.5*IQR(data_c$value_as_number.before)),]
  #data_c <-data_c[-which(data_c$value_as_number.after>summary(data_c$value_as_number.after)[5] + 1.51*IQR(data_c$value_as_number.after)),]
  #crp_max <- max(data_c$value_as_number.before)
  crp_max2 <- max(data_c$value_as_number.after)
  print("2")
  #data_c <- subset(data_c,  value_as_number.before < crp_max )
  data_c <- subset(data_c,  value_as_number.after < crp_max2 )
  data <- rbind(data, data_c)
  print(summary(data[data$measurement_type =='CRP', data$value_as_number.before]))
  print(summary(data[data$measurement_type =='CRP', data$value_as_number.after]))

return(data)
}

trim4 <- function(data){

  data$Creatinine[is.na(data$Creatinine)] <- 1
  data$BUN[is.na(data$BUN)] <- 10
  print("na ")
  # cr_high <- stats::quantile(data$Creatinine, a, na.rm =TRUE)
  # cr_low <- stats::quantile(data$Creatinine, b, na.rm = TRUE)
  # bun_high <- stats::quantile(data$BUN, a, na.rm = TRUE)
  # bun_low <- stats::quantile(data$BUN, b, na.rm = TRUE)
  # data <- subset(data, cr_low <= Creatinine & Creatinine <= cr_high )
  # data <- subset(data, bun_low <= BUN & BUN <= bun_high )
  # data <- subset(data, Creatinine <= cr_high )
  # data <- subset(data, BUN <= bun_high )
  print("crp, bun high")

   data<-data[-which(data$BUN>summary(data$BUN)[5] + 1.5*IQR(data$BUN)),]
   data<-data[-which(data$Creatinine> summary(data$Creatinine)[5] + 1.5*IQR(data$Creatinine)),]
  # print("select only crp")
  # data <- as.data.table(data)
  # data <- data[!is.na(data$value_as_number.after),]
  # data <- data[!is.na(data$value_as_number.before),]
  # data1<- data[measurement_type =='CRP',]
  # target_c <-data1[ cohort_type =="T",]
  # control_c <-data1[cohort_type =="C",]
  # data2<- data[measurement_type !='CRP',]

  print("1")
  #data_c <-data_c[-which(data_c$value_as_number.before>summary(data_c$value_as_number.before)[5] + 1.5*IQR(data_c$value_as_number.before)),]
  #target_c <- target_c[-which(target_c$value_as_number.after>summary(target_c$value_as_number.after)[5] + 1.5*IQR(target_c$value_as_number.after)),]
  #crp_max <- max(data_c$value_as_number.before)
  #crp_max2 <- max(target_c$value_as_number.after)
  print("2")
  #data_c <- subset(data_c,  value_as_number.before < crp_max )
  #target_c <- subset(target_c,  value_as_number.after < crp_max2 )
  # rm(data)
  # rm(data1)
  # data <- rbind(data2, target_c, control_c)
  # print(summary(data[data$measurement_type =='CRP', data$value_as_number.before]))
  # print(summary(data[data$measurement_type =='CRP', data$value_as_number.after]))

return(data)
}

drug_period <- function(data4, t1){
                              print("start:: merge t1  & drug_data") 
                        drug_data <- left_join(data4, t1[,c("drug_concept_id","Name","type1","type2")], by = c("drug_concept_id") ) 
                        rm(data4)
                        # 1. error 는 불포함 
                        print("start:: delete error")
                        drug_data <- drug_data[drug_data$type1 !='error'& drug_data$type2 != 'error', ]
                        # 2. melt 
                        print("start:: arrange & melt")
                        drug_data <- drug_data %>% arrange(ID, drug_exposure_start_date, drug_exposure_end_date2) %>% melt( id.vars= c("ID", "drug_exposure_start_date", "drug_exposure_end_date2","period") , measure.vars = c('type1' ,'type2')) 
                        drug_data <- drug_data %>% select(-variable)

                        # '빈칸'은 삭제 
                        print("start:: delete blank")
                        drug_data <- drug_data[drug_data$value != '' & !(is.na(drug_data$value)),]
                        drug_data <- unique(drug_data)
                        print("!!!!!!!!!!!!!!!show me melt!!!!!!!!!!!!!!!!!!!! ")
                        str(drug_data)
                        print("how many na?")
                        print(colSums(is.na(drug_data)) )
                        # # pivot wider 
                        print("start:: - pivot wider")
                        drug_data2 <- drug_data %>% tidyr::pivot_wider(names_from = value, values_from = period , values_fill = 0)
                        drug_data2 <- as.data.frame(drug_data2)
                        rm(drug_data)        
                        print("!!!!!!!!!!!!!!!show me pivot wider !!!!!!!!!!!!!!!!!!!! ")
                        str(drug_data2)
                        print("how many na?")
                        print(colSums(is.na(drug_data2)))
                        ## need full drug variable
                        cols <- c( 'metformin', 'SU','sglt2','dpp4i','tzd','alpha', 'gnd')
                        drug_data2[ cols[ !(cols %in% colnames(drug_data2)) ] ] <-0
                        print("show me drug_data2")
                        str(drug_data2)
                        print("how many na?")
                        print( colSums(is.na(drug_data2)) )
                        # ## cumsum ..ID, start date, end date , drug 7
                        print("start:: cumsum - using data.table new version ~!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ")
                        dt <- data.table(drug_data2)
                        rm(drug_data2)
                        setorder(dt, ID, drug_exposure_start_date, drug_exposure_end_date2)[,c('cum.met', 'cum.su', 'cum.sglt', 'cum.dpp', 'cum.tzd', 'cum.alpha', 'cum.gnd')
                                                                                            :=.(cumsum(metformin), cumsum(SU), cumsum(sglt2), cumsum(dpp4i), cumsum(tzd), cumsum(alpha), cumsum(gnd)), by = ID ]

                        # drug_data3<-drug_data2  %>% arrange(ID, drug_exposure_start_date, drug_exposure_end_date2) %>%
                        #             group_by(ID, drug_exposure_start_date) %>% 
                        #             group_by(ID) %>%
                        #             mutate_at( vars(metformin, sglt2, SU, gnd, dpp4i, tzd, alpha),cumsum) %>%
                        #             dplyr::distinct(ID, drug_exposure_start_date,drug_exposure_end_date2,metformin, sglt2, SU, gnd, dpp4i, tzd, alpha )
                        # drug_data3 <- as.data.frame(drug_data3)
                        # names(drug_data3) <-c("ID", "drug_exposure_start_date", 'drug_exposure_end_date2', "cum_metformin", "cum_sglt2","cum_SU", "cum_gnd", "cum_dpp4i", "cum_tzd", "cum_alpha")
                        # print("drug period str")
#                        str(drug_data3)
#                        rm(drug_data2)
                        return(dt)

}
drug_period_merge <- function(ruleout, drug_data){
                      print("start::cumsum - subset1")
                      ruleout_sub1 <- subset(ruleout, select = c(ID, measurement_type, measurement_date.after))
                      ruleout_sub1 <- unique(ruleout_sub1)
                      print("start::cumsum - merge ruleout subset")
                      drug_data2 <- left_join(drug_data, ruleout_sub1, by = c('ID')) 
                      drug_data3 <- drug_data2 %>% filter(drug_exposure_start_date <= measurement_date.after & measurement_date.after <= drug_exposure_end_date2)
                      print('merge data:: drug_data')
                      rm(ruleout_sub1)
                      rm(drug_data2)
                      total <- left_join(ruleout, drug_data3[,c('ID','cum.met', 'cum.su', 'cum.sglt', 'cum.dpp', 'cum.tzd', 'cum.alpha', 'cum.gnd',
                         'measurement_date.after','measurement_type')], by=c("ID", 'measurement_date.after','measurement_type') )
                      rm(drug_data2)
                      print('total')
                      str(total)
                      return(total)

}
no_error_id <- function(total, db_hospital){
                ### delete no measurement_Type (error)
              total <- total[!(total$measurement_type == 'error'), ]
              ### delete no cohort_type (error)
              total <- total[!(total$cohort_type == 'error'), ]
              ### delete no gender(error)
              total <- total[!(total$gender == 'error'), ]
              ### delete value_as_number == 0
              total <- total[!(total$value_as_number.before == 0),]
              ids <- unique(total$ID)
              new_ids <- c()
              while (TRUE) {
                new_ids <- append(new_ids, ids::random_id(n=1))
                if (length(new_ids) == length(ids)){
                  break
                }}
              IDS <- as.data.frame(cbind(ids, new_ids))
              print("merge id, new_ids")
              total1 <- merge(total, IDS, by.x ='ID', by.y= 'ids')
              total1$hospital <- db_hospital
              print("show:: after merge")
              str(total1)
              print("delete original ID")
              total1 <- total1 %>% select(-ID)
              total1 <- as.data.frame(total1)
              names(total1)[names(total1) == 'new_ids'] <-c("ID")
              print("REPLACE new_ids TO ID")
              rm(total)
              rm(IDS)
              return(total1)
}

})


# ## Stat module 
stat <- module({
  import("dplyr")
  import("ggplot2")
  import("gridExtra")
  import("cowplot")
  import("sjlabelled")
  import("moonBook")
  import("stats")
  import("gplots")
  import("stats")
  import("lawstat")
  import("grDevices")
  import("nortest")
  import("tableone")
  import('moonBook')
  import('ggpubr')
  import('data.table')
  import('utils')
  import('base')
  import('gridExtra')
  import('scales')
  import('tableone')
  import("psych")
## 시각화 , 단계별로 
fig <- function(df, step,a ,b ) {
  df <- df %>% mutate( cohort_type2 = recode(cohort_type,  "C"="control", "T" ="target"))
  cohort_type_ = as.factor(df$cohort_type2)
  print("options")
  options(repr.plot.width=15, repr.plot.height =5)
  print("ggplot")
  a1 <- df %>% ggplot(aes(x=BUN, fill= cohort_type_)) + theme_classic() + geom_histogram(color="gray80", alpha=0.2, position = "identity", binwidth = a) 
  a2 <- df %>% ggplot(aes(x=Creatinine, fill= cohort_type_)) + theme_classic() + geom_histogram(color="gray80", alpha=0.2, position = "identity", binwidth=b)
  a3 <- df %>% ggplot(aes(x=age, fill= cohort_type_)) + theme_classic() + geom_histogram(color="gray80", alpha=0.2, position = "identity", binwidth = 5)
  print("grid1")
  fig <- plot_grid(a1, a2, a3, labels =c("BUN", "Creatinine", "Age"), align = "h", ncol=3)
  title <- ggdraw() + draw_label( paste0(step, " numeric variable") , fontface = "bold") 
  print("grid2")
  fig <- plot_grid(title, fig, cols = 1, rel_heights = c(0.1, 1))
  print("ggsave")
  ggsave(paste0("/data/results/fig/", step ,"_numeric.png"), fig, device = "png",  dpi=300, width=15, height=5)
  options(repr.plot.width = 15, repr.plot.height = 10)
  print("b1")
  b1=df %>% ggplot(aes(x=SU, fill= cohort_type_ )) + theme_classic() + geom_bar()
  print("b2")
  b2=df %>% ggplot(aes(x= alpha, fill= cohort_type_)) + theme_classic()+ geom_bar()
  print("b3")
  b3=df %>% ggplot(aes(x=dpp4i, fill= cohort_type_)) + theme_classic()+ geom_bar()
  print("b4")
  b4=df %>% ggplot(aes(x=gnd, fill= cohort_type_)) + theme_classic()  + geom_bar()
  print("b5")  
  b5=df %>% ggplot(aes(x=sglt2, fill= cohort_type_)) + theme_classic()+ geom_bar()
  print("b6")
  b6=df %>% ggplot(aes(x=tzd, fill= cohort_type_)) + theme_classic()  + geom_bar()
  print("fig")
  fig<- plot_grid(b1, b2 ,b3, b4, b5, b6,labels=c("SU", "Alpha", "DPP-4I", "GND", "SGLT2","TZD") , ncol=3)
  title<-ggdraw() + draw_label( paste0(step, " co-medication "),  fontface = "bold") 
  print("fig2")
  fig2<-plot_grid(title, fig, ncol = 1, rel_heights = c(0.1, 1))
  print("ggsave")
  ggsave(paste0("/data/results/fig/",step,"_co_drug.png"), fig2 , device = "png",  dpi=300, width=15, height=10)
}

############################

dose_fig <- function(df, step) {
  dose_type_ = as.factor(df$dose_type)
  print("options")
  options(repr.plot.width=15, repr.plot.height =5)
  print("ggplot")
  a1 <- df %>% ggplot(aes(x=BUN, fill= dose_type_)) + theme_classic() + geom_histogram(color="gray80", alpha=0.2, position = "identity", bins=100) 
  a2 <- df %>% ggplot(aes(x=Creatinine, fill= dose_type_)) + theme_classic() + geom_histogram(color="gray80", alpha=0.2, position = "identity", bins= 100)
  a3 <- df %>% ggplot(aes(x=age, fill= dose_type_)) + theme_classic() + geom_histogram(color="gray80", alpha=0.2, position = "identity", bins=100)
  print("grid1")
  fig <- plot_grid(a1, a2, a3, labels =c("BUN", "Creatinine", "Age"), align = "h", ncol=3)
  title <- ggdraw() + draw_label( paste0(step, " numeric variable") , fontface = "bold") 
  print("grid2")
  fig <- plot_grid(title, fig, cols = 1, rel_heights = c(0.1, 1))
  print("ggsave")
  ggsave(paste0("/data/results/fig/dose_", step ,"_numeric.png"), fig, device = "png",  dpi=300, width=15, height=5)
  options(repr.plot.width = 15, repr.plot.height = 10)
  print("b1")
  b1=df %>% ggplot(aes(x=SU, fill= dose_type_ )) + theme_classic() + geom_bar()
  print("b2")
  b2=df %>% ggplot(aes(x= alpha, fill= dose_type_)) + theme_classic()+ geom_bar()
  print("b3")
  b3=df %>% ggplot(aes(x=dpp4i, fill= dose_type_)) + theme_classic()+ geom_bar()
  print("b4")
  b4=df %>% ggplot(aes(x=gnd, fill= dose_type_)) + theme_classic()  + geom_bar()
  print("b5")  
  b5=df %>% ggplot(aes(x=sglt2, fill= dose_type_)) + theme_classic()+ geom_bar()
  print("b6")
  b6=df %>% ggplot(aes(x=tzd, fill= dose_type_)) + theme_classic()  + geom_bar()
  print("fig")
  fig<- plot_grid(b1, b2 ,b3, b4, b5, b6,labels=c("SU", "Alpha", "DPP-4I", "GND", "SGLT2","TZD") , ncol=3)
  title<-ggdraw() + draw_label( paste0("by dose: ",step, " co-medication "),  fontface = "bold") 
  print("fig2")
  fig2<-plot_grid(title, fig, ncol = 1, rel_heights = c(0.1, 1))
  print("ggsave")
  ggsave(paste0("/data/results/fig/dose_",step,"_co_drug.png"), fig2 , device = "png",  dpi=300, width=15, height=10)
}

###############################
## ps매칭 전후, smd
smd <- function( DF, step ){
  # new table create
  vars <- c("age", "cci", "BUN",  "Creatinine" ,  "cohort_type", "gender", "year", "SU", "alpha"   ,"dpp4i", "gnd", "sglt2" ,  "tzd" ,  
                  "MI"   , "HF"   ,   "PV"     ,   "CV"    ,   "CPD" ,   "Rheuma" , 
                  "PUD",    "MLD"  , "D"    ,   "DCC" ,  "HP"  ,  "Renal",  "M"  , "MSLD", "MST",  "AIDS"  ,
                 "HT2" ,   "HL2",  "Sepsis" ,  "HTT" ) 
  catVars <- c("year", "SU", "alpha"   ,"dpp4i", "gnd", "sglt2" ,  "tzd" ,  
                  "MI"   , "HF"   ,   "PV"     ,   "CV"    ,   "CPD" ,   "Rheuma" , 
                  "PUD",    "MLD"  , "D"    ,   "DCC" ,  "HP"  ,  "Renal",  "M"  , "MSLD", "MST",  "AIDS"  ,
                 "HT2" ,   "HL2",  "Sepsis" ,  "HTT")

  t1 <- CreateTableOne(vars = vars, factorVars = catVars, strata = 'cohort_type', data = DF, test = TRUE)
  table1 <- print(t1, smd = TRUE) 
  write.csv(table1, file = paste0("/data/results/test/smd_",step,"table.csv"))
  
  # label생성 
  print("start smd")
  DF$gender <- set_label(DF$gender, label = "gender")
  DF$gender <- set_labels(DF$gender, labels= c("male" = 'M' , "female" = 'F'))
  print("smd:: gender done")
  DF$cohort_type <- set_label(DF$cohort_type, label ="cohort_type")
  DF$cohort_type <- set_labels(DF$cohort_type, labels=c("target"='T', "control"='C'))
  print("smd:: cohort_type done")
  out = mytable(cohort_type ~ age + gender + year + SU + sglt2 + dpp4i + gnd +tzd + alpha + BUN + Creatinine + MI + HF +PV + 
                    CV + CPD + Rheuma+ PUD +MLD + D + DCC + HP + Renal + M + MSLD + MST+ AIDS + HT2+ HL2 + Sepsis+ HTT + cci , data =DF, max.ylev =2)
  print("out done")
  mycsv(out, file = paste0("/data/results/test/smd_", step, ".csv"))
  print("mycsv done")

  ###################### delete intermediate file 
  rm(DF)
  ######################
} ##이후 mycsv(out, file="") 파일명으로 단계 구분하기. 
## dose type version: ps매칭 전후, smd
dose_smd <- function(DF ){
  # label생성 
  print("start dose_smd::")
  DF$gender <- set_label(DF$gender, label ="gender")
  DF$gender<- set_labels(DF$gender, labels=  c("male" = 'M' , "female" = 'F'))
  print("dose_smd:: gender_type done")
  out = mytable(dose_type ~ age + gender + year + SU + sglt2 + dpp4i + gnd +tzd + alpha + BUN + Creatinine + MI + HF +PV + 
                CV + CPD + Rheuma+ PUD +MLD + D + DCC + HP + Renal + M + MSLD + MST+ AIDS + HT2+ HL2 + Sepsis+ HTT + cci , data =DF, max.ylev =2)
  print("dose_smd::: out done")
  mycsv(out, file = paste0("/data/results/dose_smd.csv"))
  print("dose _smd:: mycsv done")
  ###################### delete intermediate file 
  rm(DF)
  ######################

} ##이후 mycsv(out, file="") 파일명으로 단계 구분하기. 

test_stat <- function(stat){
 stat <- setDT(stat)
 stat <- stat[,.(ID, cohort_type, measurement_type, rate, diff, value_as_number.before, value_as_number.after)]
 stat <- unique(stat)
 stat<- na.omit(stat)
 print("!!!!!!!!!!!!!!!!!!!!!!!create sumamry results table")
 baseline <-as.data.table(describeBy(value_as_number.before  ~measurement_type +cohort_type, data = stat, mat =TRUE, digits =2, quant=c(.25,.75)))
 followup <-as.data.table(describeBy(value_as_number.after  ~measurement_type +cohort_type, data = stat, mat =TRUE, digits =2, quant=c(.25,.75)))
 diff <-as.data.table(describeBy(diff ~measurement_type +cohort_type, data = stat, mat =TRUE, digits =2, quant=c(.25,.75)))
 rate <-as.data.table(describeBy(rate ~measurement_type +cohort_type, data = stat, mat =TRUE, digits =2, quant=c(.25,.75)))
 baseline$class <- "baseline"
 followup$class <- "F/U"
 diff$class <-"diff"
 rate$class <- "rate"
 total <- rbind(baseline, followup, diff, rate)
 total[order(group1, group2)]
 print(str(total))
 print("split target, control ")
 rt <- total[group2 =='T', .(class, group1,  n, mean, sd, min, Q0.25, median, Q0.75, max)]
 rt[order(group1)] 
 rc <- total[group2 =='C', .(class, group1, n, mean, sd, min, Q0.25, median, Q0.75, max)]
 rc[order(group1)]
 print("combind target, control ")
 r_sum <- cbind(rt, rc)
# colnames(r_sum) <- c("group1",  "t_n", "t_mean", "t_sd", "t_min", "t_1q", "t_median", "t_3q", "t_max", "group2","c_n","c_mean", "c_sd", "c_min", "c_1q", "c_median", "c_3q", "c_max")
  # 복용군 먼저 
 print("!!!!!!!!!!!!!!create p-vale table")
 rate <- stat[,.(ID, cohort_type, measurement_type, rate)]
 rate <- unique(rate)
 diff <- stat[,.(ID, cohort_type, measurement_type, diff)]
 diff <- unique(diff)
 print("start decast")
 rate <- dcast(rate, ID+cohort_type ~ measurement_type, value.var = c("rate"))
 diff <- dcast(diff, ID+cohort_type ~ measurement_type, value.var = c("diff"))
 print("rate")
 str(rate)
 rate<-  rate %>% rename('Total_cholesterol' = 'Total cholesterol',
                         'NTproBNP'= "NT-proBNP")
 diff<-  diff %>% rename('Total_cholesterol' = 'Total cholesterol',
                         'NTproBNP'= "NT-proBNP")

 print(" create summary result table")
 tb_rate <- mytable(cohort_type ~ CRP + ESR + BUN + Triglyceride + SBP + Total_cholesterol + Hb + Glucose_Fasting + Creatinine +  HDL + AST + Albumin + insulin + BMI + HbA1c + DBP +  LDL +  NTproBNP , data = rate,  method = 3,  catMethod = 0, show.all = T, max.ylev =2, digits = 2)
 tb_diff <- mytable(cohort_type ~ CRP + ESR + BUN + Triglyceride + SBP + Total_cholesterol + Hb + Glucose_Fasting + Creatinine +  HDL + AST + Albumin + insulin + BMI + HbA1c + DBP +  LDL +  NTproBNP , data = diff,  method = 3,  catMethod = 0, show.all = T, max.ylev =2, digits = 2)
 mycsv(tb_rate, file = "/data/results/test/main_rate.csv" )
 mycsv(tb_diff, file = "/data/results/test/main_diff.csv" )
 tb_rate2 <-tb_rate$res
 tb_rate2<- tb_rate2[c("cohort_type", "p2", "p3", "N")]
 colnames(tb_rate2) <- c("group1", "rate_ttest", "rate_wilcox")
 tb_diff2 <-tb_diff$res
 tb_diff2<- tb_diff2[c("cohort_type", "p2", "p3", "N")]
 colnames(tb_diff2) <- c("group1", "diff_ttest", "diff_wilcox") 
 tb_diff2 <- as.data.table(tb_diff2)
 tb_rate2 <- as.data.table(tb_rate2)
 tb <- merge(tb_rate2, tb_diff2, by ="group1")
 tb[order(group1)]
 print("r_sum")
 head(r_sum)
 print("tb")
 head(tb)

 #result <- merge(r_sum, tb, by = "group1")
 # output 
  write.csv(r_sum, "/data/results/test/main_summary.csv")
  write.csv(tb, "/data/results/test/main_stat.csv")
#write.csv(result, "/data/results/test2/main_stat.csv")
}

test_sub1 <- function(data){
  data <- setDT(data)
  stat <- data[cohort_type =='T',.(ID, drug_group, measurement_type, value_as_number.before, value_as_number.after)]
  stat2 <- data[,.(ID, cohort_type, measurement_type, value_as_number.before, value_as_number.after)]
  stat <- unique(stat)
  stat2 <- unique(stat2)
  stat2<- na.omit(stat2)
  fun <- function(x){ return(round(x, 2))}
  # 전체 실험군 및 대조군 비교 
    r_pre <- describeBy(value_as_number.before ~ cohort_type + measurement_type , data =stat2, mat= TRUE, digits =2, quant=c(.25, .75))
    r_post <- describeBy(value_as_number.after ~ cohort_type + measurement_type , data =stat2, mat= TRUE, digits =2, quant=c(.25, .75))
    r_pre$step <- "pre"
    r_post$step <- "post"
    r <- rbind(r_pre, r_post)
    r<- as.data.table(r)
    r1<- r[,.(group1, group2, step, n, mean, sd, min, Q0.25, median, Q0.75, max )]
    r1[order(group2, step, group1 )]
    write.csv(r1, "/data/results/test1/sub1_total_summary.csv")
    target <- stat2[cohort_type =='T',]
    control <- stat2[cohort_type =="C",]
     # t.test result: p.value, conf.int, mean of difference
    ttest <- by(target, target$measurement_type, function(x) 
             t.test(x$value_as_number.before, x$value_as_number.after, mu=0, alt="two.sided", 
             paired=TRUE, conf.level=0.95)[c(3,5)])
    ttest2<- rbindlist(ttest)
    ttest2$type <- rownames(ttest)
    # wilcox test: p.value, 
    wilcox <- by(target, target$measurement_type, function(x) 
             wilcox.test(x$value_as_number.before, x$value_as_number.after,
             paired=TRUE)[3])
    wilcox2 <- data.frame(wilcox_pvalue = unlist(wilcox))
    wilcox2$type<- rownames(wilcox2)
    print("rbind stat ")

    r2 <- merge(ttest2, wilcox2, by = "type")
    colnames(r2) <-c("type", "ttest", "mean_diff", "wilcox")
    r2[,2:4] <- data.frame(lapply(r2[,2:4], fun ))
    r2$group <-"target"
    write.csv(r2, "/data/results/test1/sub1_target_summary.csv")
    rm(ttest)
    rm(ttest2)
    rm(wilcox)
    rm(wilcox2)
    ttest <- by(control, control$measurement_type, function(x) 
             t.test(x$value_as_number.before, x$value_as_number.after, mu=0, alt="two.sided", 
             paired=TRUE, conf.level=0.95)[c(3,5)])
    ttest2<- rbindlist(ttest)
    ttest2$type <- rownames(ttest)
    # wilcox test: p.value, 
    wilcox <- by(control, control$measurement_type, function(x) 
             wilcox.test(x$value_as_number.before, x$value_as_number.after,
             paired=TRUE)[3])
    wilcox2 <- data.frame(wilcox_pvalue = unlist(wilcox))
    wilcox2$type<- rownames(wilcox2)
    print("rbind stat ")
    r3 <- merge(ttest2, wilcox2, by = "type")
    colnames(r3) <-c("type", "ttest", "mean_diff", "wilcox")
    r3[,2:4] <- data.frame(lapply(r3[,2:4], fun ))
    r3$group <-"control"
    write.csv(r3, "/data/results/test1/sub1_control_summary.csv")

  #실험군내 비교 정의 다시 한번 
  stat <- stat %>% mutate(drug_group2 = case_when(grepl('SU', drug_group) ~ 'SU', 
                         grepl('alpha', drug_group) ~ 'alpha', 
                         grepl('gnd', drug_group) ~ 'gnd', 
                         grepl('tzd', drug_group) ~ 'tzd', 
                         grepl('dpp4i', drug_group) ~ 'dpp4i', 
                         grepl('sglt2', drug_group) ~ 'sglt2', 
                         TRUE  ~ 'metformin'))
  ###########################
 # 갹 약물 군 마다, summary, stat table 생성
 drug_list=c("alpha", "SU", "dpp4i", "sglt2", 'metformin', "tzd","gnd") #gnd, tzd 는 모든 컬럼에 대해 값이 많이 없기 때문에 따로 예외 사항
 for ( i in drug_list){
    print(paste0("what drug?", i))
    drug <- stat[grep(i, stat$drug_group2), .(ID, drug_group2, measurement_type, value_as_number.before, value_as_number.after)]
    print(paste0(i, "summary table"))
    r_pre <- describeBy(value_as_number.before ~ measurement_type + drug_group2, data = drug, mat= TRUE, digits =2, quant=c(.25, .75))
    r_post <- describeBy(value_as_number.after ~ measurement_type + drug_group2, data = drug, mat= TRUE, digits =2, quant=c(.25, .75))
    r_pre$step <- "pre"
    r_post$step <- "post"
    r <- rbind(r_pre, r_post)
    r<- as.data.table(r)
    r1<- r[,.(group2, group1, step, n, mean, sd, min, Q0.25, median, Q0.75, max )]
    r1[order(group1)]
    print(paste0(i, "stat table"))
    # mytable 을 사용할 수 없음
    # t.test result: p.vlue, conf.int, mean of difference
    #gnd --bmi, nt-probnp
    if (i %in% c("gnd")) {
      write.csv(r1, paste0("/data/results/test1/", i, "_summary.csv"))
      #bmi는 오직 한개 
    drug<- drug[measurement_type !="BMI",]
    ttest <- by(drug, drug$measurement_type, function(x) 
             t.test(x$value_as_number.before, x$value_as_number.after, mu=0, alt="two.sided", 
             paired=TRUE, conf.level=0.95)[c(3,5)])
    ttest2<- rbindlist(ttest)
    ttest2$type <- rownames(ttest)
    # wilcox test: p.value, 
    wilcox <- by(drug, drug$measurement_type, function(x) 
             wilcox.test(x$value_as_number.before, x$value_as_number.after,
             paired=TRUE)[3])
    wilcox2 <- data.frame(wilcox_pvalue = unlist(wilcox))
    wilcox2$type<- rownames(wilcox2)
    print("rbind stat ")
    r2 <- merge(ttest2, wilcox2, by = "type")
    colnames(r2) <-c("type", "ttest", "mean_diff", "wilcox")
    fun <- function(x){ return(round(x, 2))}
    r2[,2:4] <- data.frame(lapply(r2[,2:4], fun ))
    write.csv(r2, paste0("/data/results/test1/", i,"_stat.csv"))
    }else{
    ttest <- by(drug, drug$measurement_type, function(x) 
             t.test(x$value_as_number.before, x$value_as_number.after, mu=0, alt="two.sided", 
             paired=TRUE, conf.level=0.95)[c(3,5)])
    ttest2<- rbindlist(ttest)

    ttest2$type <- rownames(ttest)
    # wilcox test: p.value, 
    wilcox <- by(drug, drug$measurement_type, function(x) 
             wilcox.test(x$value_as_number.before, x$value_as_number.after,
             paired=TRUE)[3])
    wilcox2 <- data.frame(wilcox_pvalue = unlist(wilcox))
    wilcox2$type<- rownames(wilcox2)
    print("rbind stat ")
    r2 <- merge(ttest2, wilcox2, by = "type")
    colnames(r2) <-c("type", "ttest", "mean_diff", "wilcox")
    fun <- function(x){ return(round(x, 2))}
    r2[,2:4] <- data.frame(lapply(r2[,2:4], fun ))
    R <- merge(r1, r2, by.x= "group1", by.y ="type")
    write.csv(R, paste0("/data/results/test1/", i,"_paried_test.csv"))
    }

 }


}

test_sub2 <- function(stat,dose, group_num){
 stat <- setDT(stat)
 stat <- stat[,.(ID, cohort_type, dose_type1, measurement_type, rate, diff, value_as_number.before, value_as_number.after)]
 stat <- unique(stat)
 stat<- na.omit(stat)
 print("!!!!!!!!!!!!!!!!!!!!!!!create sumamry results table")
 target<- stat[cohort_type== 'T',]
 control <-stat[cohort_type =='C',]
 
 if(group_num==2){
      dose_list <- c("high", "low")
  }else{
      dose_list <-c("high", "middle", "low")
  }

  for(i in dose_list){
    dose_group <- target[dose_type1== i, ]
    data <- rbind(dose_group, control)
    baseline <-as.data.table(describeBy(value_as_number.before  ~measurement_type +cohort_type, data = data, mat =TRUE, digits =2, quant=c(.25,.75)))
    followup <-as.data.table(describeBy(value_as_number.after  ~measurement_type +cohort_type, data = data, mat =TRUE, digits =2, quant=c(.25,.75)))
    diff <-as.data.table(describeBy(diff ~measurement_type +cohort_type, data = data, mat =TRUE, digits =2, quant=c(.25,.75)))
    rate <-as.data.table(describeBy(rate ~measurement_type +cohort_type, data = data, mat =TRUE, digits =2, quant=c(.25,.75)))
    baseline$class <- "baseline"
    followup$class <- "F/U"
    diff$class <-"diff"
    rate$class <- "rate"
    total <- rbind(baseline, followup, diff, rate)
    total[order(group1, group2)]
    print(str(total))
    print("split target, control ")
    rt <- total[group2 =='T', .(class, group1,  n, mean, sd, min, Q0.25, median, Q0.75, max)]
    rt[order(group1)] 
    rc <- total[group2 =='C', .(class, group1, n, mean, sd, min, Q0.25, median, Q0.75, max)]
    rc[order(group1)]
    print("combind target, control ")
    r_sum <- merge(rt, rc, by =c('group1', 'class'))
#    write.csv(r_sum, paste0("/data/results/test2/",dose, "_",i,"_sub2_summary.csv"))
     print("!!!!!!!!!!!!!!create p-vale table")
    rate <- data[,.(ID, cohort_type, measurement_type, rate)]
    rate <- unique(rate)
    diff <- data[,.(ID, cohort_type, measurement_type, diff)]
    diff <- unique(diff)
    print("start decast")
    rate <- dcast(rate, ID+cohort_type ~ measurement_type, value.var = c("rate"))
    diff <- dcast(diff, ID+cohort_type ~ measurement_type, value.var = c("diff"))
    print("rate")
    str(rate)
    rate<-  rate %>% rename('Total_cholesterol' = 'Total cholesterol',
                            'NTproBNP'= "NT-proBNP")
    diff<-  diff %>% rename('Total_cholesterol' = 'Total cholesterol',
                            'NTproBNP'= "NT-proBNP")

    print(" create summary result table")
    tb_rate <- mytable(cohort_type ~ CRP + ESR + BUN + Triglyceride + SBP + Total_cholesterol + Hb + Glucose_Fasting + Creatinine +  HDL + AST + Albumin + insulin + BMI + HbA1c + DBP +  LDL +  NTproBNP , data = rate,  method = 3,  catMethod = 0, show.all = T, max.ylev =2, digits = 2)
    tb_diff <- mytable(cohort_type ~ CRP + ESR + BUN + Triglyceride + SBP + Total_cholesterol + Hb + Glucose_Fasting + Creatinine +  HDL + AST + Albumin + insulin + BMI + HbA1c + DBP +  LDL +  NTproBNP , data = diff,  method = 3,  catMethod = 0, show.all = T, max.ylev =2, digits = 2)
      
    tb_rate2 <-tb_rate$res
    tb_rate2<- tb_rate2[c("cohort_type", "p2", "p3", "N")]
    colnames(tb_rate2) <- c("group1", "rate_ttest", "rate_wilcox")
    tb_diff2 <-tb_diff$res
    tb_diff2<- tb_diff2[c("cohort_type", "p2", "p3", "N")]
    colnames(tb_diff2) <- c("group1", "diff_ttest", "diff_wilcox") 
    tb_diff2 <- as.data.table(tb_diff2)
    tb_rate2 <- as.data.table(tb_rate2)
    tb <- merge(tb_rate2, tb_diff2, by ="group1")
    tb[order(group1)]
    print("r_sum")
     print(    head(r_sum))
    print("tb")
    print(head(tb))
    result <- merge(r_sum, tb, by = "group1")
    # output 
    write.csv(result, paste0("/data/results/test2/",dose, "_", i,"_sub2_stat.csv"))
    mycsv(tb_rate, file = paste0("/data/results/test2/", dose, "_", i, "_sub2_rate.csv" ))
    mycsv(tb_diff, file = paste0("/data/results/test2/", dose, "_", i, "_sub2_diff.csv" ))
  }











}



test_rate2 <- function(stat, dose){
  #필요한 값만 추출
  replace(stat$cohort_type, stat$cohort_type =='C', 'control') -> stat$cohort_type
  replace(stat$cohort_type, stat$cohort_type =='T', 'target') -> stat$cohort_type
  stat<-setDT(stat)
  stat<- stat[,.(ID, cohort_type, measurement_type,  rate)]
  stat<- unique(stat)
  rate <- dcast(stat, ID + cohort_type ~ measurement_type, value.var = c('rate'))
  names(rate)[names(rate) == 'Total cholesterol'] <-  c("Total_cholesterol")
  names(rate)[names(rate) == 'NT-proBNP'] <-  c("NTproBNP")
  print("show me the rate::test_Rate2::dcast")
  print(colSums(!is.na(rate)))
#  rate  <- as.data.frame(rate)
  # for( i in 3:ncol(rate)){
  #   require('ggpubr')
  #   name<-  colnames(rate)[i]
  #   rate <- subset(rate, !is.na(i))
  #   p <- ggplot(rate, aes(x= cohort_type, y=rate[,i], color = cohort_type))+
  #      ggpubr::stat_compare_means(aes(group= cohort_type) ) + geom_boxplot() +
  # #   ggpubr::stat_compare_means(aes(group =as.factor(cohort_type) )) +
  #    labs (title =paste(name,' rate by cohort_type'), x= "cohort type" , y = name) +
  #    scale_x_discrete(limits =c("target", "control")) +
  #    scale_y_continuous(limits=c(-1,1))
  #   ggsave(p, file=paste0("/data/results/test2/fig_rate/",dose, "_rate_", name,".png"),  dpi=300)  
  #   rm(p)
  # }

  # T test
  # tb <- mytable(cohort_type ~ CRP + ESR + BUN + Triglyceride + SBP + Total_cholesterol + Hb + Glucose_Fasting + Creatinine +  HDL + AST + Albumin + insulin +
  #     BMI + HbA1c + DBP +  LDL +  NTproBNP , data = rate,  method = 3,  catMethod = 0, show.all = T, max.ylev =2 )
print("1")
  tb <- mytable(cohort_type ~ CRP + ESR + BUN + Triglyceride +  Total_cholesterol+ SBP +Hb + Glucose_Fasting + Creatinine + HDL + AST +Albumin +insulin + BMI +
   HbA1c + DBP  + LDL + NT-proBNP , data = rate,   method = 3, catMethod = 0,   show.all = T, max.ylev =2, digits =3)
print("2")
  mycsv(tb, file = paste0('/data/results/test1/',dose,'test_rate2.csv'))
  rm(tb)
  print("test tb")
  ttest <- t.test(CRP ~ cohort_type, data = rate)
  wilcox <- wilcox.test(CRP ~ cohort_type, data = rate)
  print(ttest)
  print(wilcox)
    #########################delete intermediate data##################################
  }
   #########################delete intermediate data##################################
test_diff2 <- function(stat, dose){
  #필요한 값만 추출
  stat<-setDT(stat)
  stat<- stat[,.(ID, cohort_type, measurement_type,  diff)]
  stat<- unique(stat)
  diff <- dcast(stat, ID + cohort_type ~ measurement_type, value.var = c('diff'))
  names(diff)[names(diff) == 'Total cholesterol'] <-  c("Total_cholesterol")
  print("show me the diff::test_diff2::dcast")
  colSums(!is.na(diff))
  str(diff)
  head(diff)
  print(" ") 
  rm(stat)
  # T test
  tb <- mytable(cohort_type ~ CRP + ESR + BUN + Triglyceride +  Total_cholesterol+ SBP +Hb + Glucose_Fasting + Creatinine + HDL + AST +Albumin +insulin + BMI +
   HbA1c + DBP  + LDL + NT-proBNP , data = diff,   method = 3, catMethod = 0,   show.all = T, max.ylev =2, digits =3)
  # performs a Shapiro-Wilk test to decide between normal or non-normal
  # Perform chisq.test first. If warning present, perform fisher test
       
  print("diff test mytable")
  mycsv(tb, file = paste0('/data/results/test2/',dose,'test_diff2.csv'))
    rm(tb)
      print("test tb")
  ttest <- t.test(CRP ~ cohort_type, data = diff)
  wilcox <- wilcox.test(CRP ~ cohort_type, data = diff)
  print(ttest)
  print(wilcox)
}

dosegroup <- function(data, int, group_num){
# ## 용량군 재정의 
# ## 2개 병원은 dose 로 정의. 나머지 병원은 그대로. 
print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! grouping dose type !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

group_num <- as.numeric(group_num)
data <-setDT(data)
col<-c("dose_type", "dose_list2")
data[cohort_type =='T',!..col]
col2<- c("total_dose")
print("change chamc, khmc")
data2 <- data[hospital %in% c("CHAMC","KHMC"), !..col2]
data2[,":=" (dose_list2 =  strsplit(unlist(dose_list1), ","))]
data2$total_dose <- sapply(data2$dose_list2, function(x) sum(as.numeric(unlist(x))))
col3 <- c("dose_list2") 
data2[,!..col3]
print("rbind exception, data")
data3 <- data[!hospital %in% c("CHAMC", 'KHMC')]
rm(data)
data <- rbind(data2, data3)
print("show data")
print(str(data))
print("define dose_group")
if(group_num == 2){
data$dose_type1 <- ifelse(data$total_dose >  int, "high", "low") #메일 요청사항 
#data$dose_type2 <- ifelse(data$total_dose > int,  "high", "low")

}else{
data$dose_type1 <- ifelse(data$total_dose>= 1000, ifelse(data$total_dose >= 1500, "high","middle"), "low" )
}
return(data) }

test_dose <- function(stat,dose, group_num){

 stat <- setDT(stat)
 #cols <-c("ID","dose_type1", 'measurement_type', 'rate', 'diff', 'value_as_number.before', 'value_as_number.after')
 stat <- stat[,.(ID, dose_type1, measurement_type, rate, diff, value_as_number.before, value_as_number.after)]
 stat <- unique(stat)
 stat<- na.omit(stat)
 print("!!!!!!!!!!!!!!!!!!!!!!!create sumamry results table")
 baseline <-as.data.table(describeBy(value_as_number.before ~ measurement_type + dose_type1 , data = stat, mat =TRUE, digits =2, quant=c(.25,.75)))
 followup <-as.data.table(describeBy(value_as_number.after  ~ measurement_type + dose_type1, data = stat, mat =TRUE, digits =2, quant=c(.25,.75)))
 diff <-as.data.table(describeBy(diff ~measurement_type + dose_type1, data = stat, mat =TRUE, digits =2, quant=c(.25,.75)))
 rate <-as.data.table(describeBy(rate ~measurement_type + dose_type1, data = stat, mat =TRUE, digits =2, quant=c(.25,.75)))
 baseline$class <- "baseline"
 followup$class <- "F/U"
 diff$class <-"diff"
 rate$class <- "rate"
 total <- rbind(baseline, followup, diff, rate)
 total[order(group1, group2)]
 print(str(total))
 print("split dose_group")
 if(group_num ==2){
  high <- total[group2 =='high', .(class, group1,  n, mean, sd, min, Q0.25, median, Q0.75, max)]
  high[order(group1)] 
  high$group <- "high"
  low <- total[group2 =='low', .(class, group1, n, mean, sd, min, Q0.25, median, Q0.75, max)]
  low[order(group1)]
  low$group <- "low"
  print("combind high, low")
  dose_sum <- cbind(high, low)
  write.csv(dose_sum, paste0("/data/results/test2/2_group_",dose,"_type1_summary.csv"))
 print("!!!!!!!!!!!!!!create p-vale table")
  rate <- stat[,.(ID, dose_type1, measurement_type, rate)]
  rate <- unique(rate)
  diff <- stat[,.(ID, dose_type1, measurement_type, diff)]
  diff <- unique(diff)
 print("start decast")
  rate <- dcast(rate, ID+ dose_type1 ~ measurement_type, value.var = c("rate"))
  diff <- dcast(diff, ID+ dose_type1 ~ measurement_type, value.var = c("diff"))
 print("rate")
 str(rate)
 rate<-  rate %>% rename('Total_cholesterol' = 'Total cholesterol',
                         'NTproBNP'= "NT-proBNP")
 diff<-  diff %>% rename('Total_cholesterol' = 'Total cholesterol',
                         'NTproBNP'= "NT-proBNP")

 print(" create summary result table")
 tb_rate <- mytable(dose_type1 ~ CRP + ESR + BUN + Triglyceride + SBP + Total_cholesterol + Hb + Glucose_Fasting + Creatinine +  HDL + AST + Albumin + insulin + BMI + HbA1c + DBP +  LDL +  NTproBNP , data = rate,  method = 3,  catMethod = 0, show.all = T, max.ylev =2, digits = 2)
 tb_diff <- mytable(dose_type1 ~ CRP + ESR + BUN + Triglyceride + SBP + Total_cholesterol + Hb + Glucose_Fasting + Creatinine +  HDL + AST + Albumin + insulin + BMI + HbA1c + DBP +  LDL +  NTproBNP , data = diff,  method = 3,  catMethod = 0, show.all = T, max.ylev =2, digits = 2)
 mycsv(tb_rate, file = paste0("/data/results/test2/2_group_",dose, "_type1_rate.csv" ))
 mycsv(tb_diff, file =  paste0("/data/results/test2/2_group_", dose, "_type1_diff.csv" ))
 } else{
       high <- total[group2 =='high', .(class, group1,  n, mean, sd, min, Q0.25, median, Q0.75, max)]
        high[order(group1)] 
          high$group <- "high"
       middle <- total[group2 =='middle', .(class, group1,  n, mean, sd, min, Q0.25, median, Q0.75, max)]
        middle[order(group1)] 
          middle$group <- "middle"
        low <- total[group2 =='low', .(class, group1, n, mean, sd, min, Q0.25, median, Q0.75, max)]
        low[order(group1)]
          low$group <- "low"
        print("combind high, low")
        dose_sum <- cbind(high, middle, low)
        write.csv(dose_sum, paste0("/data/results/test2/3_group_dose_summary.csv"))
        print("!!!!!!!!!!!!!!create p-vale table")
        stat2 <- stat[dose_type1 !='middle',]
        stat3 <- stat[dose_type1 != 'high',]
        rate2 <- stat2[,.(ID, dose_type1, measurement_type, rate)]
        rate2 <- unique(rate2)
        diff2 <- stat2[,.(ID, dose_type1, measurement_type, diff)]
        diff2 <- unique(diff2)

        rate3 <- stat3[,.(ID, dose_type1, measurement_type, rate)]
        rate3 <- unique(rate3)
        diff3 <- stat3[,.(ID, dose_type1, measurement_type, diff)]
        diff3 <- unique(diff3)
        print("start decast")
        rate2 <- dcast(rate2, ID + dose_type1 ~ measurement_type, value.var = c("rate"))
        diff2 <- dcast(diff2, ID + dose_type1 ~ measurement_type, value.var = c("diff"))
        rate3 <- dcast(rate3, ID + dose_type1 ~ measurement_type, value.var = c("rate"))
        diff3 <- dcast(diff3, ID + dose_type1 ~ measurement_type, value.var = c("diff"))
        print("rate")
        str(rate)
        rate2<-  rate2 %>% rename('Total_cholesterol' = 'Total cholesterol',
                                'NTproBNP'= "NT-proBNP")
        diff2<-  diff2 %>% rename('Total_cholesterol' = 'Total cholesterol',
                                'NTproBNP'= "NT-proBNP")
        rate3<-  rate3 %>% rename('Total_cholesterol' = 'Total cholesterol',
                                'NTproBNP'= "NT-proBNP")
        diff3<-  diff3 %>% rename('Total_cholesterol' = 'Total cholesterol',
                                'NTproBNP'= "NT-proBNP")                                

        print(" create summary result table")
        tb_rate2 <- mytable(dose_type1 ~ CRP + ESR + BUN + Triglyceride + SBP + Total_cholesterol + Hb + Glucose_Fasting + Creatinine +  HDL + AST + Albumin + insulin + BMI + HbA1c + DBP +  LDL +  NTproBNP , data = rate2,  method = 3,  catMethod = 0, show.all = T, max.ylev =2, digits = 2)
        tb_diff2 <- mytable(dose_type1 ~ CRP + ESR + BUN + Triglyceride + SBP + Total_cholesterol + Hb + Glucose_Fasting + Creatinine +  HDL + AST + Albumin + insulin + BMI + HbA1c + DBP +  LDL +  NTproBNP , data = diff2,  method = 3,  catMethod = 0, show.all = T, max.ylev =2, digits = 2)
        mycsv(tb_rate2, file = paste0("/data/results/test2/3_group_high_low_rate.csv" ))
        mycsv(tb_diff2, file =  paste0("/data/results/test2/3_group_high_low_diff.csv" ))
        tb_rate3 <- mytable(dose_type1 ~ CRP + ESR + BUN + Triglyceride + SBP + Total_cholesterol + Hb + Glucose_Fasting + Creatinine +  HDL + AST + Albumin + insulin + BMI + HbA1c + DBP +  LDL +  NTproBNP , data = rate3,  method = 3,  catMethod = 0, show.all = T, max.ylev =2, digits = 2)
        tb_diff3 <- mytable(dose_type1 ~ CRP + ESR + BUN + Triglyceride + SBP + Total_cholesterol + Hb + Glucose_Fasting + Creatinine +  HDL + AST + Albumin + insulin + BMI + HbA1c + DBP +  LDL +  NTproBNP , data = diff3,  method = 3,  catMethod = 0, show.all = T, max.ylev =2, digits = 2)
        mycsv(tb_rate3, file = paste0("/data/results/test2/3_group_middle_low_rate.csv" ))
        mycsv(tb_diff3, file =  paste0("/data/results/test2/3_group_middle_low_diff.csv" ))
 }
 }


sum_test <- function(data){
  #target
  target <- data[cohort_type =="T",.(value_as_number.before, value_as_number.after, diff, rate)]
  r  <- describeBy(value_as_number.before + value_as_number.after + diff +rate ~ measurement_type, data= d1, mat =TRUE, digits =5)
  r1 <-describe(value_as_number.before + value_as_number.after + diff + rate ~ measurement_type, data= target, quant=c(.25,.75)) 
}




# ## rate: 정규성, 등분산성, ttest, wilcox, 
## rate: mean(평균),min, q1, median, q3, max 
test_rate <- function(stat, dose){  # dose: high, low, total
  results<- data.frame( m_type= NA,  target_mean = NA, target_sd = NA, control_mean = NA, control_sd = NA, ttest_p= NA,  wilcoxon_pvalue = NA)
  
  #m_list <- unique(stat$measurement_type)
  m_list <- c("CRP", "ESR", "AST", "Albumin","BUN", "Creatinine","DBP",  "SBP", "Glucose_Fasting", "HbA1c",  "insulin",  "HDL", "LDL", "Total cholesterol", "Triglyceride", "Hb",   "BMI",  "NT-proBNP")
#  m_list <- c("CRP", "Albumin", "BUN" ,"Creatinine")
  for( j in m_list){
      target <- stat %>% dplyr::filter(measurement_type == j  & cohort_type =='T')  %>% distinct(ID, rate) 
      control<- stat %>% dplyr::filter(measurement_type == j  & cohort_type =='C')  %>% distinct(ID, rate) 
#      total <- stat %>% dplyr::filter(measurement_type == j )  %>% distinct(ID, cohort_type, rate) 
      # target <- stat %>% dplyr::filter(measurement_type == j  & cohort_type =='T') %>% distinct(ID, diff) 
      # control<- stat %>% dplyr::filter(measurement_type == j  & cohort_type =='C') %>% distinct(ID, diff) 
      # total <- stat %>% dplyr::filter(measurement_type == j ) %>% distinct(ID, cohort_type, diff)


      target <- as.data.frame(target)  
      control  <- as.data.frame(control) 
  #    total <- as.data.frame(total) 

      if ((nrow(target) <3 | nrow(target) >5000) | (nrow(control) <3 |nrow(control) >5000)) {
        out<- data.frame(j,NA, NA, NA, NA ,NA, NA )
        names(out)<-names(results)
        results<- rbind(results, out)
        rm(out)
          } else {      
 
        ttest <- t.test(target$rate, control$rate)
        wilcox <- wilcox.test(target$rate, control$rate)
        ## 소수점 2자리에서 반올림
        ttest_p<- round(ttest$p.value, digits = 5)
        wilcox <- round(wilcox$p.value, digits =5) 
        means <- as.numeric(gsub("^[a-z]","", ttest$estimate) )
        targetmean <- round(means[1], digits=5)
        targetsd <- round(sd(target$rate, digits =5))
        controlmean <- round(means[2], digits=5)
        controlsd <- round(sd(control$rate), digits =5)
 
        ## 합치기 
        outs <- data.frame( j, targetmean, targetsd, controlmean, controlsd, ttest_p, wilcox)
        names(outs) <- names(results)
        results <- rbind(results, outs)
        rm(outs)
 
        ## 그림 
        # bars<- tapply(total$rate, total$cohort_type, mean )
        # lower<- tapply(total$rate, total$cohort_type, function(x) t.test(x)$conf.int[1])
        # upper<- tapply(total$rate, total$cohort_type, function(x) t.test(x)$conf.int[2])
        # print("check test_Rate:: bars which one are control?, check bars ")
        # print(bars)
        # png(file=paste0("/data/results/test1/fig_rate/",dose, "_" ,j,"_rate_test.png"))
        # barplot2(bars, space=0.4, xlim=c(0,3.0), plot.ci=TRUE, ci.l = lower, ci.u= upper, ci.color ="maroon", ci.lwd=4, names.arg=c("control","target"), col=c("coral", "darkkhaki"), xlab ="cohort type", ylab= "rate", 
        # main=paste0(j,"Rate by cohort type with Confidence Interval" ) )
        # dev.off()
      }
    }
      results <- na.omit(results)

      print("test_rate::: save")
      write.csv(results, paste0("/data/results/test1/", dose,"_test_rate1.csv")) 
      ###################### delete intermediate file 
      # rm(target)
      # rm(control)
      # rm(total)
      # rm(results)
      ######################
  }


cum_period  <- function(stat){
  results <- data.frame(cohort_type= NA, d_type = NA,  m_type= NA, cum_metformin = NA, cum_sglt2 = NA, cum_su =NA, cum_gnd = NA, cum_dpp4i = NA, cum_tzd = NA, cum_alpha =NA )
  drug_list=c("alpha", "SU", "gnd","tzd","dpp4i", "sglt2", 'metformin')
  total <- stat %>% distinct(ID, cohort_type, cum.met, cum.su, cum.sglt, cum.dpp, cum.tzd, cum.alpha, cum.gnd, drug_group, measurement_type)
  m_list <- unique(total$measurement_type)
#  target <- stat %>% mutate(drug_group2 = case_when(grepl('SU', drug_group) ~ 'SU', grepl('alpha', drug_group) ~ 'alpha', grepl('gnd', drug_group) ~ 'gnd', grepl('tzd', drug_group) ~ 'tzd', grepl('dpp4i', drug_group) ~ 'dpp4i', grepl('sglt2', drug_group) ~ 'sglt2', TRUE  ~ 'metformin'))
  ## 전체 실험군, 대조군의 누적 약물 복용 기간
  print("cum_period::: start for loop of total") 
  for (j in m_list){
      target <- total %>% dplyr::filter(measurement_type == j & cohort_type =='T')
      control <- total %>% dplyr::filter(measurement_type ==j & cohort_type == 'C')
      target <- unique(target)
      control <- unique(control)

      ## 약물 누적 복용 기간 추가  
      target_metformin <- mean(target$cum.met, na.rm = TRUE)
      target_sglt2 <- mean(target$cum.sglt, na.rm = TRUE)
      target_su <- mean(target$cum.su, na.rm = TRUE)
      target_gnd <- mean(target$cum.gnd, na.rm = TRUE)
      target_dpp4i <- mean(target$cum.dpp, na.rm = TRUE)
      target_tzd <- mean(target$cum.tzd, na.rm = TRUE)
      target_alpha <- mean(target$cum.alpha, na.rm = TRUE)
      
      out2 <- data.frame(cohort_type ='target', d_type ='total', m_type = j, cum_metformin = target_metformin, cum_sglt2 = target_sglt2, cum_su = target_su, cum_gnd = target_gnd, 
      cum_dpp4i = target_dpp4i, cum_tzd = target_tzd, cum_alpha = target_alpha)
      names(out2)<-names(results)
      results <- rbind(results, out2)
      
      control_metformin <- mean(control$cum.met, na.rm = TRUE)
      control_sglt2 <- mean(control$cum.sglt, na.rm = TRUE)
      control_su <- mean(control$cum.su, na.rm = TRUE)
      control_gnd <- mean(control$cum.gnd, na.rm = TRUE)
      control_dpp4i <- mean(control$cum.dpp, na.rm = TRUE)
      control_tzd <- mean(control$cum.tzd, na.rm = TRUE)
      control_alpha <- mean(control$cum.alpha, na.rm = TRUE)
      
      out3 <- data.frame(cohort_type ='control',d_type = 'total', m_type = j, cum_metformin = control_metformin, cum_sglt2 = control_sglt2, cum_su = control_su, cum_gnd = control_gnd, 
      cum_dpp4i = control_dpp4i, cum_tzd = control_tzd, cum_alpha = control_alpha)
      names(out3)<-names(results)
      results<- rbind(results, out3)
      print(paste("cum_priod::mtype: ", j))
      rm(out2)
      rm(out3)
      rm(target)
      rm(control)
      }
      results <- na.omit(results)
      print("cum_period_tc ::: save")
      write.csv(results, paste0("/data/results/test2/cum_drug_period_tc.csv")) 
 
   target <- total  %>% dplyr::filter(cohort_type =='T') %>% mutate(drug_group2 = case_when(grepl('SU', drug_group) ~ 'SU', grepl('alpha', drug_group) ~ 'alpha', grepl('gnd', drug_group) ~ 'gnd', grepl('tzd', drug_group) ~ 'tzd', grepl('dpp4i', drug_group) ~ 'dpp4i', grepl('sglt2', drug_group) ~ 'sglt2', TRUE  ~ 'metformin'))
   m_list <- unique(target$measurement_type)
  results_drug2 <- data.frame(cohort_type= NA, d_type = NA,  m_type= NA, cum_metformin = NA, cum_sglt2 = NA, cum_su =NA, cum_gnd = NA, cum_dpp4i = NA, cum_tzd = NA, cum_alpha =NA ) 
  print("cum_period::: start for loop of drug_type") 
  for( i in drug_list){
    target_drug <-target[grep(i, target$drug_group2), ]
    for (j in m_list){  
      target_ <- target_drug %>% dplyr::filter(measurement_type == j)
      target_ <- as.data.frame(target_)
        ## 약물 누적 복용 기간 추가 
            ## 약물 누적 복용 기간 추가  
      target_metformin <- mean(target_$cum.met, na.rm = TRUE)
      target_sglt2 <- mean(target_$cum.sglt, na.rm = TRUE)
      target_su <- mean(target_$cum.su, na.rm = TRUE)
      target_gnd <- mean(target_$cum.gnd, na.rm = TRUE)
      target_dpp4i <- mean(target_$cum.dpp, na.rm = TRUE)
      target_tzd <- mean(target_$cum.tzd, na.rm = TRUE)
      target_alpha <- mean(target_$cum.alpha, na.rm = TRUE)
      
      out2 <- data.frame(cohort_type ='target', d_type =i, m_type = j, cum_metformin = target_metformin, cum_sglt2 = target_sglt2, cum_su = target_su, cum_gnd = target_gnd, 
      cum_dpp4i = target_dpp4i, cum_tzd = target_tzd, cum_alpha = target_alpha)
      names(out2)<-names(results_drug2)
      results_drug2 <- rbind(results_drug2, out2)
      rm(out2)
       }}
   results_drug2 <- na.omit(results_drug2)
   print("cum_drug_period::: save")
   write.csv(results_drug2, paste0("/data/results/test2/cum_drug_period_drug_type.csv")) 
      }

# 용량별 t-test, paired t-test
# high dose vs low dose  ttest 

dose_rate2 <- function(stat, dose){
  #필요한 값만 추출
  stat<-setDT(stat)
  stat<- stat[,.(ID, dose_type, measurement_type,  rate)]
  stat<- unique(stat)
  rate <- dcast(stat, ID + dose_type ~ measurement_type, value.var = c('rate'))
  names(rate)[names(rate) == 'Total cholesterol'] <-  c("Total_cholesterol")
  names(rate)[names(rate) == 'NT-proBNP'] <-  c("NTproBNP")
  print("show me the rate::dose_Rate2::dcast")
  print(colSums(!is.na(rate)))
  rate  <- as.data.frame(rate)

  tb <- mytable(dose_type ~ CRP + ESR + BUN + Triglyceride + SBP + Total_cholesterol + Hb + Glucose_Fasting + Creatinine +  HDL + AST + Albumin + insulin +
      BMI + HbA1c + DBP +  LDL +  NTproBNP , data = rate,  method = 3,  catMethod = 0, show.all = T, max.ylev =2, digits =3)

  mycsv(tb, file = paste0('/data/results/test2/',dose, '_dose_rate2.csv'))
  rm(tb)
  print("test tb")
  ttest <- t.test(CRP ~ cohort_type, data = rate)
  wilcox <- wilcox.test(CRP ~ cohort_type, data = rate)
  print(ttest)
  print(wilcox)
    #########################delete intermediate data##################################
  }

dose_diff2 <- function(stat, dose){
  #필요한 값만 추출
  stat<-setDT(stat)
  stat<- stat[,.(ID, dose_type, measurement_type,  diff)]
  stat<- unique(stat)
  diff <- dcast(stat, ID + dose_type ~ measurement_type, value.var = c('diff'))
  names(diff)[names(diff) == 'Total cholesterol'] <-  c("Total_cholesterol")
  names(diff)[names(diff) == 'NT-proBNP'] <-  c("NTproBNP")
  print("show me the rate::dose_diff2::dcast")
  print(colSums(!is.na(diff)))
  diff  <- as.data.frame(diff)
  # for( i in 3:ncol(rate)){
  #   require('ggpubr')
  #   name <-  colnames(rate)[i]
  #   rate <- subset(rate, !is.na(i))
  #   print("1")
  #   p <- ggplot(rate, aes(x= dose_type, y=rate[,i], color = dose_type))+
  #        ggpubr::stat_compare_means(aes(group= dose_type)) + geom_boxplot() +
  # #   ggpubr::stat_compare_means(aes(group =as.factor(cohort_type) )) +
  #    labs (title =paste(name,' rate by dose_type'), x= "dose type" , y = name) +
  #    scale_x_discrete(limits =c( "dose", "low")) +
  #    scale_y_continuous(limits=c(-1,1))
  #   print("2")
  #   ggsave(p, file=paste0("/data/results/test2/fig_rate/",dose ,"_dose_", name," rate.png"),  dpi=300)  
  #   print("3")
  #   rm(p)
#  }

  # T test
  tb <- mytable(dose_type ~ CRP + ESR + BUN + Triglyceride + SBP + Total_cholesterol + Hb + Glucose_Fasting + Creatinine +  HDL + AST + Albumin + insulin +
      BMI + HbA1c + DBP +  LDL +  NTproBNP , data = diff,  method = 3,  catMethod = 0, show.all = T, max.ylev =2, digits =3)

  mycsv(tb, file = paste0('/data/results/test2/',dose, '_dose_diff2.csv'))
  rm(tb)

    #########################delete intermediate data##################################
  }



dose_ptest_drug2 <- function(data1){ ##각 서브 약물군 별로 데이터가 별로 없으면 에러 발생 가능
# target만의 데이터 생성 
  stat<-setDT(data1)
  dose_list =c("high", "middle", 'low')
  m_list <-c('CRP', 'ESR', 'BUN', 'Triglyceride', 'SBP', 'Total_cholesterol', 'Hb', 'Glucose_Fasting', 'Creatinine', 'HDL', 'AST', 'Albumin', 'insulin', 'HbA1c', 'DBP', 'LDL' ,'NTproBNP')
  for( i in dose_list){
    target_dose <- data1[grep(i, data1$dose_type),.(ID, measurement_type, value_as_number.before, value_as_number.after, dose_type)]
    target_dose <- unique(target_dose)
    print("1")
      pre_  <- dcast(target_dose, ID + dose_type  ~ measurement_type, value.var=c('value_as_number.before'))
      pre_$step <-'pre'
      post_ <- dcast(target_dose, ID + dose_type ~ measurement_type, value.var=c('value_as_number.after'))
      post_$step <-'post'
      total <- rbind(pre_, post_)
      total<- as.data.frame(total)
      total$step <- as.factor(total$step)
      print("2")
      names(total)[names(total) == 'Total cholesterol'] <-  c("Total_cholesterol")
      names(total)[names(total) == 'NT-proBNP'] <-  c("NTproBNP")
      rm(pre_)
      rm(post_)
      print("3")

      tb <- mytable(step ~ CRP + ESR + BUN + Triglyceride + SBP +Hb + Glucose_Fasting + Creatinine + HDL + AST +Albumin +insulin + BMI + HbA1c + DBP +
                Total_cholesterol + LDL + NTproBNP, data = total, method =3, catMethod=0, show.all =T, digits =3)
      mycsv(tb, file = paste0('/data/results/test2/',i, '_dose_rate_ptest2.csv'))
      rm(tb)
      print("4")
    for (j in m_list){  
        print(j)
        target_ <- subset(total, !is.na(j)) 
        str(target_)
        print("5")
        print("show target_")
        str(target_)
        p <- ggplot(data = target_,  aes(x= step, y= target_[,j], color =step)) +
          ggpubr::stat_compare_means(aes(group= step)) +
          geom_boxplot(position = position_dodge(width=0.9), outlier.shape =NA)+
          scale_y_continuous(limits=quantile(target_[,j], c(0.1, 0.9), na.rm=TRUE))+
          labs(title =paste0(i," ", j, " paired_test"), x= "step", y= j) +
          scale_x_discrete(limits=c("pre", "post"))
        ggsave(p, file = paste0('/data/results/test2/fig_ptest/dose/',i,"_dose_", j, '_ptest.png'), dpi=300)
        rm(p)
        rm(target_dose)
    } 
  }
}


## diff: 정규성, 등분산성, ttest, wilcox, describe 
dose_diff_rate <- function(stat){
  high <- stat %>% dplyr::filter(dose_type == 'high')
  low <- stat %>% dplyr::filter(dose_type == 'low')
  results<- data.frame( m_type= NA, rate_normality_high= NA, rate_normality_low= NA, rate_variance= NA, rate_ttest_p = NA,  rate_wilcox = NA, rate_high_mean = NA, rate_low_mean = NA, diff_normality_high= NA  , diff_normality_low= NA, diff_variance= NA, diff_ttest_p = NA,  diff_wilcox = NA, diff_high_mean = NA, diff_low_mean = NA)
  m_list <- unique(stat$measurement_type)
  for( j in m_list){

      high_m <- high %>% dplyr::filter(measurement_type==j) %>% dplyr::distinct(ID, rate, diff, dose_type)
      low_m <- low %>% dplyr::filter(measurement_type ==j ) %>% dplyr::distinct(ID, rate, diff, dose_type)
      total_m <- stat %>% dplyr::filter(measurement_type ==j) %>% dplyr::distinct(ID, rate, diff, dose_type)
      if ((nrow(high_m) <3 | nrow(high_m) >5000) | (nrow(low_m) <3 |nrow(low_m) >5000)) {
        out<- data.frame(j,NA, NA,NA, NA,NA,NA, NA, NA, NA, NA, NA, NA,NA,NA )
        names(out)<-names(results)
        results<- rbind(results, out)
          } else {      
        r_normality_h <- shapiro.test(high_m$rate)
        r_normality_l <- shapiro.test(low_m$rate)
        r_variance <- levene.test(total_m$rate, total_m$dose_type)
        r_ttest <- t.test(high_m$rate, low_m$rate)
        r_wilcox <- wilcox.test(high_m$rate, low_m$rate)
    
        d_normality_h <- shapiro.test(high_m$diff)
        d_normality_l <- shapiro.test(low_m$diff)
        d_variance <- levene.test(total_m$diff, total_m$dose_type)
        d_ttest <- t.test(high_m$diff, low_m$diff)
        d_wilcox <- wilcox.test(high_m$diff, low_m$diff)
        ## 소수점 2자리에서 반올림
        r_normality_h = round(r_normality_h$p.value , digits = 2)
        r_normality_l = round(r_normality_l$p.value , digits = 2)
        r_variance = round(r_variance$p.value , digits = 2)
        r_ttest_p<- round(r_ttest$p.value, digits = 2)
        r_wilcox <-round(r_wilcox$p.value, digits = 2) 
        r_means <- as.numeric(gsub("^[a-z]","", r_ttest$estimate) )
        r_high_mean <- round(r_means[1], digits=2)
        r_low_mean <- round(r_means[2], digits=2)

        d_normality_h = round(d_normality_h$p.value , digits = 2)
        d_normality_l = round(d_normality_l$p.value , digits = 2)
        d_variance = round(d_variance$p.value , digits = 2)
        d_ttest_p<- round(d_ttest$p.value, digits = 2)
        d_wilcox <-round(d_wilcox$p.value, digits = 2) 
        d_means <- as.numeric(gsub("^[a-z]","", d_ttest$estimate) )
        d_high_mean <- round(d_means[1], digits=2)
        d_low_mean <- round(d_means[2], digits=2)

        ## 합치기 
        outs<- data.frame(j, r_normality_h, r_normality_l, r_variance, r_ttest_p, r_wilcox, r_high_mean, r_low_mean, d_normality_h, d_normality_l, d_variance, d_ttest_p, d_wilcox, d_high_mean, d_low_mean)
        names(outs)<-names(results)
        results<- rbind(results, outs)
        ## 그림 
        r_bars <- tapply(total_m$rate, total_m$dose_type, function(x) mean(x, na.rm=TRUE))
        r_lower<- tapply(total_m$rate, total_m$dose_type, function(x) t.test(x)$conf.int[1])
        r_upper<- tapply(total_m$rate, total_m$dose_type, function(x) t.test(x)$conf.int[2])
        png(file=paste0("/data/results/dose_fig/dose_",j,"_rate.png"))
        barplot2(r_bars, space=0.4, xlim=c(0,3.0), plot.ci=TRUE, ci.l = r_lower, ci.u= r_upper, ci.color ="maroon", ci.lwd=4, names.arg=c("high","low"), col=c("coral", "darkkhaki"), xlab ="dose type", ylab= "rate", 
        main=paste0(j," rate by dose type with Confidence Interval" ) )
        dev.off()
        d_bars <- tapply(total_m$diff, total_m$dose_type, function(x) mean(x, na.rm=TRUE))
        d_lower<- tapply(total_m$diff, total_m$dose_type, function(x) t.test(x)$conf.int[1])
        d_upper<- tapply(total_m$diff, total_m$dose_type, function(x) t.test(x)$conf.int[2])
        png(file=paste0("/data/results/dose_fig/dose_",j,"_diff.png"))
        barplot2(d_bars, space=0.4, xlim=c(0,3.0), plot.ci=TRUE, ci.l = d_lower, ci.u= d_upper, ci.color ="maroon", ci.lwd=4, names.arg=c("high","low"), col=c("coral", "darkkhaki"), xlab ="dose type", ylab= "diff", 
        main=paste0(j," diff by dose type with Confidence Interval" ) )
        dev.off()
      }
    }
      results <- na.omit(results)
      ###################### delete intermediate file 
      rm(high)
      rm(low)
      rm(outs)
      ######################
      return(results)
  }
# paired ttest rate test: 등분산성, 정규성, t-test, wilcox test 
## target + 용량군별 
dose_ptest <- function(total){
  dose_list  <- c("high", "low")
  results<- data.frame(dose_type =NA,m_type= NA, normality_pvalue_pre= NA, normality_pvalue_post= NA, var_pvalue= NA, ttest_pvalue = NA,  wilcoxon_pvalue = NA,pre_mean = NA, post_mean = NA, mean_diff= NA)
  m_list <- unique(total$measurement_type)
  for (j in m_list){  
        total_m <- total %>% dplyr::filter(measurement_type == j ) %>% dplyr::distinct(ID, value_as_number.before, value_as_number.after, dose_type, measurement_type)
       if (nrow(total_m) < 7) {
        out<- data.frame("total",j,NA, NA, NA, NA, NA, NA, NA, NA )
        names(out)<-names(results)
        results<- rbind(results, out)
          } else {      
        normality_pre <- ad.test(total_m$value_as_number.before)
        normality_post <- ad.test(total_m$value_as_number.after)
        variance <- var.test(total_m$value_as_number.before , total_m$value_as_number.after)
        ptest <- t.test(total_m$value_as_number.after , total_m$value_as_number.before, paired =TRUE)
        wilcox <- wilcox.test(total_m$value_as_number.before , total_m$value_as_number.after,exact = FALSE)
        ## 소수점 2자리에서 반올림
        normality_pre = round(normality_pre$p.value , digits = 2)
        normality_post = round(normality_post$p.value , digits = 2)
        variance = round(variance$p.value , digits = 2)
        ptest_p<- round(ptest$p.value, digits = 2)
        wilcox <-round(wilcox$p.value, digits = 2) 
        mean_diff <- round(as.numeric(gsub("^[a-z]","", ptest$estimate)), digits=2 )
        pre_mean <- round(mean(total_m$value_as_number.before, na.rm=TRUE), digits=2)
        post_mean <- round(mean(total_m$value_as_number.after, na.rm=TRUE), digits=2)
        ## 합치기 
        outs<- data.frame('total' ,j, normality_pre, normality_post, variance, ptest_p, wilcox, pre_mean, post_mean, mean_diff)
        names(outs)<-names(results)
        results<- rbind(results, outs)
        ## 그림 
        sub <- subset(total_m, select =c("value_as_number.before", "value_as_number.after"))
        bars<- sapply( sub , function(x) mean(x, na.rm=TRUE))
        lower <- sapply(sub , function(x) t.test(x)$conf.int[1])
        upper<- sapply(sub, function(x) t.test(x)$conf.int[2])
        png(file=paste0("/data/results/dose_fig/dose_total_",j,"_value_ptest.png"))
        barplot2(bars, space=0.4, plot.ci=TRUE, ci.l= lower, ci.u= upper, ci.color="maroon", ci.lwd=4, names.arg=c("pre","post"), col=c("coral","darkkhaki"), xlab="pre vs post", ylab = "value", 
        main =paste0("total_dose : ",j," with Confidence Interval"))
        dev.off()
       }} ##dose_Type 
      for( i in dose_list){
    target_dose <-total[grep(i, total$dose_type), ]
    for (j in m_list){  
        target_ <- target_dose %>% dplyr::filter(measurement_type == j)
        target_ <- as.data.frame(target_)
      
        if (nrow(target_) <3 | nrow(target_) >5000) {
          out<- data.frame(i,j, NA, NA,NA, NA,NA, NA, NA, NA )
          names(out)<-names(results)
          results<- rbind(results, out)
            } else {      
          normality_pre <- shapiro.test(target_$value_as_number.before)
          normality_post <- shapiro.test(target_$value_as_number.after)
          variance <- var.test(target_$value_as_number.before , target_$value_as_number.after)
          ptest <- t.test(target_$value_as_number.after , target_$value_as_number.before, paired =TRUE)
          wilcox <- wilcox.test(target_$value_as_number.before , target_$value_as_number.after, exact = FALSE)

          ## 소수점 2자리에서 반올림
          normality_pre = round(normality_pre$p.value , digits = 2)
          normality_post = round(normality_post$p.value , digits = 2)
          variance = round(variance$p.value , digits = 2)
          ptest_p<- round(ptest$p.value, digits = 2)
          wilcox <-round(wilcox$p.value, digits = 2) 
          mean_diff <- round(as.numeric(gsub("^[a-z]","", ptest$estimate)), digits=2 )
          pre_mean <- round(mean(target_$value_as_number.before, na.rm=TRUE), digits=2)
          post_mean <- round(mean(target_$value_as_number.after, na.rm=TRUE), digits=2)

          ## 합치기 
          outs<- data.frame(i,j, normality_pre, normality_post, variance, ptest_p, wilcox, pre_mean, post_mean, mean_diff)
          names(outs) <- names(results)
          results<- rbind(results, outs)
    
          ## 그림 
          sub <- subset(target_, select = c("value_as_number.before", "value_as_number.after"))
          bars<- sapply( sub , function(x) mean(x, na.rm=TRUE))
          lower <- sapply(sub , function(x) t.test(x)$conf.int[1])
          upper<- sapply(sub, function(x) t.test(x)$conf.int[2])
          png(file=paste0("/data/results/dose_fig/does_total_",i,"_", j,"_value_ptest.png"))
          barplot2(bars, space=0.4, plot.ci=TRUE, ci.l= lower, ci.u= upper, ci.color="maroon", ci.lwd=4, names.arg=c("pre","post"), col=c("coral","darkkhaki"), xlab="pre vs post", ylab = "value", 
          main =paste0(i," : ",j," value with Confidence Interval"))
          dev.off()
      }}}

            results <- na.omit(results)
            ###################### delete intermediate file 
            rm(total_m)
            rm(total)
            rm(outs)
            ######################
            return(results)
          }
        
        
        })


# ## ps매칭에 필요한 변수 정의 : drug, disease history, Renal 수치  
ps <- module({
  import("strex")
  import("reshape")
  import("dplyr")
  import("tidyr")
  import('data.table')
  import('lubridate')
  import('utils')
  import('base')
  import('stats')
  import('MatchIt')
# drug history # ps_1st
##파일 합치기. 
drug_history <- function(data, t1){
  dc <- left_join(data, t1[,c("drug_concept_id","Name","type1","type2")], by = c("drug_concept_id") ) 
  dc<- unique(dc)
## get dummies 
  dc2 <- reshape2::melt(dc[,c("ID", "type1","type2")], id.vars =c("ID"), measure.vars=c("type1", "type2"))
  dc2 <- dc2 %>% group_by(ID) %>% summarise(type = list(unique(value)))
  columns = c("SU", "alpha", "dpp4i", "gnd", "metformin", "sglt2", "tzd")
  dc3 <- sapply(dc2$type, function(x) table(factor(x, levels= columns)))
  dc3 <-t(dc3)
  dc4<-cbind(dc2, dc3)  #id, type(all drug list ), dummmies variable SU", "alpha", "dpp4i", "gnd", "metformin", "sglt2", "tzd

  columns = c("ID","SU", "alpha", "dpp4i", "gnd", "metformin", "sglt2", "tzd")
  dc4 <- dplyr::select(dc4, all_of(columns))

  ###################### delete intermediate file 
  rm(dc)
  rm(dc2)
  rm(dc3)
  ###################### delete intermediate file 
  return(dc4)
}

disease_history <- function(df, data2, data3){
# df$condition_type  <-  case_when(df$ancestor_concept_id == 4329847 ~ "MI",
#                                                 df$ancestor_concept_id == 316139  ~ "HF",
#                                                 df$ancestor_concept_id == 321052  ~ "PV",
#                                                 df$ancestor_concept_id %in% c(381591, 434056)  ~ "CV",
#                                                 df$ancestor_concept_id == 4182210 ~ 'Dementia',
#                                                 df$ancestor_concept_id == 4063381 ~ 'CPD',
#                                                 df$ancestor_concept_id %in% c(257628, 134442, 80800, 80809, 256197, 255348) ~ 'Rheuma',
#                                                 df$ancestor_concept_id == 4247120 ~ 'PUD',
#                                                 df$ancestor_concept_id %in% c(4064161, 4212540) ~ 'MLD',
#                                                 df$ancestor_concept_id == 201820 ~ 'D',
#                                                 df$ancestor_concept_id %in% c(443767,442793) ~ 'DCC',
#                                                 df$ancestor_concept_id %in% c(192606, 374022) ~ 'HP',
#                                                 df$ancestor_concept_id %in% c(4030518,	4239233, 4245042) ~ 'Renal',
#                                                 df$ancestor_concept_id == 443392  ~ 'M',
#                                                 df$ancestor_concept_id %in% c(4245975, 4029488, 192680, 24966) ~ 'MSLD',
#                                                 df$ancestor_concept_id == 432851  ~ 'MST',
#                                                 df$ancestor_concept_id == 439727  ~ 'AIDS',
#                                                 df$ancestor_concept_id == 316866  ~ 'HT',
#                                                 df$ancestor_concept_id == 432867  ~ 'HL',
#                                                 df$ancestor_concept_id == 132797  ~ 'Sepsis',
#                                                 df$ancestor_concept_id == 4254542 ~ 'HTT',
#                                                 TRUE ~ 'error')
# # disease history 
  dd <- reshape2::melt(df[,c("ID", "condition_type")], id.vars = "ID" , measure.vars="condition_type") 

  #l <- unique(dd$value)
  dd2 <-dd %>% group_by(ID) %>% summarise(dtype = list(unique(value)))
  print("renal::dd2")
  cols = c("MI", "HF", "PV", "CV" , "Dementia","CPD", "Rheuma", "PUD", "MLD", "D", "DCC", "HP", 'Renal', "M", "MSLD","MST", "AIDS",'HT','HL','Sepsis', 'HTT')
  dd3 <- sapply(dd2$dtype ,function(x) table(factor(x, levels = cols)) )
  dd4 <- t(dd3)
  dd4 <- cbind(dd2, dd4)
  dd4 <-dd4 %>% select(-dtype)
  print("renal::dd4")
## 고혈압 데이터랑 합치기 
  d_hp <- left_join(dd4, data2, by = 'ID')
  d_hp$hypertension_drug[is.na(d_hp$hypertension_drug)] <-0
## 합친 데이터가 0이상이면 1로 변경. 
  d_hp <- d_hp %>% mutate( HT2 = HT + hypertension_drug )
  d_hp$HT2[d_hp$HT2 > 0] <- 1

## 고지혈증 데이터랑 합치기 
  dd5 <- left_join(d_hp, data3, by = 'ID')
  dd5$hyperlipidemia_drug[is.na(dd5$hyperlipidemia_drug)] <-0 
## 합친 데이터가 0이상이면 1로 변경. 
  dd5 <- dd5 %>% mutate( HL2 = HL + hyperlipidemia_drug )
  dd5$HL2[dd5$HL2 > 0] <- 1
  cols <- c("ID", "MI", "HF", "PV", "CV" , "Dementia","CPD", "Rheuma", "PUD", "MLD", "D", "DCC", "HP", 'Renal', "M", "MSLD","MST", "AIDS",'HT2','HL2','Sepsis', 'HTT')  
  dd5 <- select(dd5, all_of(cols) )
  ###################### delete intermediate file 
  rm(df)
  rm(dd)
  rm(dd2)
  rm(dd3)
  rm(dd4)
  rm(d_hp)
  ###################### delete intermediate file 
  return(dd5)
} 
# id, type(all disease list), dummies variabel: codition_types
# #신기능 수치 추출  #ID, 날짜, 신기능 수치들..

renal <- function(data){
# cleaning 
data$value_as_number[data$value_as_number == 'None'] <-1

# Before, Cr, BUN 추출 
renal1 <- data %>% dplyr::filter(measurement_type %in% c("BUN","Creatinine") & measurement_date < cohort_start_date) %>% dplyr::distinct(ID, cohort_start_date, measurement_type, value_as_number, measurement_date, gender, age)
setDT(renal1)
#check n 수 
print('renal 1: filter data')
N1=length(unique(renal1$ID))
print(N1)
## pick latest data
print("renal2 :pick latest data")
renal2 <- renal1[,.SD[which.max(measurement_date)], by=.(ID, measurement_type)]

# renal2 <- renal1 %>% arrange(ID, measurement_type, desc(measurement_date)) %>% group_by(ID, measurement_type) %>% mutate(row = row_number() )
# renal2<- as.data.frame(renal2)
# print('renal 2; new column row ')
# str(renal2)   
N2=length(unique(renal2$ID))
print(N2)
# print("renal 3: select row ==1")
# renal3 <- renal2 %>% dplyr::filter(row ==1)
# str(renal3)   
# N3=length(unique(renal3$ID))
# print(N3)
print("start renal3; pivot ")
renal3 <- renal2 %>% distinct(ID, measurement_type, value_as_number, age, gender) %>% tidyr::pivot_wider(names_from = measurement_type, values_from = value_as_number)
renal3 <- as.data.frame(renal3)
N3= length(unique(renal3$ID))
print(N3)
print('renal3; pivot_wider')

print("extract before bun, cr done")

#null, na 값은 1, 10 로 대체. 
renal3$Creatinine[is.na(renal3$Creatinine)] <- 1
renal3$BUN[is.na(renal3$BUN)] <- 10
# eGFR 계산
renal3$gender <- ifelse(renal3$gender == 'M', 0.742, 1)
renal3$egfr <- round(175* (renal3$Creatinine^(-1.154))* (renal3$age^(-0.203))* renal3$gender, 2)
renal3 <- unique(renal3[, c("ID", "BUN", "Creatinine",'egfr') ])

###################### delete intermediate file 
rm(renal1)
rm(renal2)
###################### delete intermediate file 
return(renal3)
}
## CCI 계산
## disease_history 결과값을 INPUT 값으로 넣기. 
cci<- function(data){
  data <- data %>% dplyr::distinct(ID, MI, HF, PV, CV, Dementia, CPD, Rheuma, PUD, MLD, D, DCC, HP, Renal, M, MSLD, MST, AIDS)
  data <- data %>% dplyr::mutate(cci = MI + HF + PV + CV + Dementia + CPD + Rheuma + PUD  + MLD + D + DCC*2 + HP*2 + Renal*2 + M*2 + MSLD*3 + MST*6 +AIDS*6)
  data <- data[,c("ID", "cci")]

  return(data)
}

ps <- function(data, formula){
            ps <-  unique(data[, c("cohort_type", "age",  "gender", "SU", "alpha"   ,"dpp4i", "gnd", "sglt2" ,  "tzd" ,  
                  "BUN",  "Creatinine" ,  "MI"   , "HF"   ,   "PV"     ,   "CV"    ,   "CPD" ,   "Rheuma" , 
                  "PUD",    "MLD"      ,   "DCC" ,  "HP"  ,  "Renal",  "MSLD"  ,  "AIDS"   ,   "HT2" ,   "HL2",   
                  "Sepsis" ,  "HTT"    ,      "ID"      ,    "egfr",  'cci', 'year', 'hospital')])   
            ps$egfr[is.na(ps$egfr)] <- 90
            ps[is.na(ps)] <- 0
            # ## convert cohort_Type, age; chr to numeric
            ps$cohort_type <- ifelse(ps$cohort_type =='C', 1, 0)
            ps$gender <- ifelse(ps$gender =='M', 0, 1)
            print("test:: matchit :::: formla version")
            m.out <- matchit(formula , data = ps, method ='nearest', distance ='logit',  ratio =2)
            summary(m.out)
            m.data <- match.data(m.out, data = ps, distance = 'prop.score')
            id <- m.data %>% distinct(ID)
            data2 <- left_join(id, data, by = 'ID')
            rm(ps)
            rm(m.out)
            rm(m.data)
            return(data2)
            }


})


## labeling so9
##파일 합치기. 
simplify <- module({
  import("strex")
  import("reshape")
  import("dplyr")
  import("tidyr")
  import('data.table')
  import('stats')
  import('base')
  import('utils')
  import('lubridate')
pair<- function(data){
                    # pair 
                    before <- data %>% dplyr::filter(measurement_date < cohort_start_date) %>% dplyr::distinct(ID, measurement_type, value_as_number, measurement_date, cohort_start_date)
                    # before <- data %>% dplyr::filter(measurement_date < cohort_start_date) %>% dplyr::distinct(ID, measurement_type, value_as_number, measurement_date, cohort_type, cohort_start_date)  
                    before <- as.data.frame(before)
                    before2 <- before %>% dplyr::arrange(ID, measurement_type, desc(measurement_date)) %>% group_by(ID, measurement_type) %>% mutate( row = row_number())
                    before2<- as.data.frame(before2)
                    before3 <- before2 %>% dplyr::filter(row ==1)
                    before <- before3 %>% dplyr::distinct(ID, measurement_type, value_as_number, measurement_date) 
                    #before <- before3 %>% dplyr::distinct(ID, measurement_type, value_as_number, cohort_type, measurement_date) 
                    print("check before:::")
                    str(before)
                    after <- data %>% dplyr::filter(measurement_date >= cohort_start_date) 
                    after<- as.data.frame(after)
                    print("unique after")
                    after <- unique(after)
                    print("join start")
#                    pair = inner_join(before, after, by= c("ID","measurement_type","cohort_type"), suffix =c(".before", ".after"))
                    pair = inner_join(before, after, by= c("ID","measurement_type"), suffix =c(".before", ".after"))
                    print("unique pair")
                    pair = unique(pair)
                    print("check pair")
                    str(pair)
                    ###################### delete intermediate file 
                    rm(before)
                    rm(before2)
                    rm(before3)
                    rm(after)
                    ###################### delete intermediate file 
                    return(pair)
                    }
exposure <- function(pair){
                    #exposure
                    print("start exposure, filter latest") 
                    exposure <- pair %>% dplyr::filter(drug_exposure_start_date <= measurement_date.after & measurement_date.after <= drug_exposure_end_date) 
                    exposure <- as.data.frame(exposure)      
#                    exposure2 <- exposure %>% arrange(ID, measurement_type, desc(measurement_date.after)) %>% group_by(ID, measurement_type) %>% mutate(row= row_number())
#                   exposure <- as.data.frame(exposure2)               
                    print("start unique exposure")
                    exposure <- unique(exposure)
                    print("check exposure")
                    str(exposure)
                    ###################### delete intermediate file 
#                    rm(exposure2)
                    ###################### delete intermediate file 
                    return(exposure)
                    }
# # # rule out 
ruleout <- function(exposure, t1){
         ##### 0) 측정 날짜에 복용한 약물 중. 첫 복용 시작 날짜가 max 인것을 추출 --> 잔류기간으로 중복되어 ruleout 되는 경우 줄이고, max dose over 되는 경우 줄이고자함.  
                  print("choose max drug_start date")
                  d <- exposure %>% dplyr::distinct(ID, measurement_date.after, drug_exposure_start_date)
                  d1 <- d %>% group_by(ID, measurement_date.after) %>% summarise(drug_list = list(unique(drug_exposure_start_date))) 
                  d1 <- d1 %>% group_by(ID, measurement_date.after) %>% mutate(drug_exposure_start_date2 = max(unlist(drug_list))) 
                  d1$drug_exposure_start_date <- as_date( d1$drug_exposure_start_date2,  origin = lubridate::origin)
                  d1 <- as.data.frame(d1)
                  print("change join to merge !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                  d2 <- merge(x= d1[,c('ID', 'measurement_date.after', 'drug_exposure_start_date')], y= exposure, by = c("ID", "measurement_date.after", "drug_exposure_start_date"))
                  print("show me d2")
                  str(d2)
                  rm(d)
                  rm(d1)
                  rm(exposure)
         # # # 1) ingredient 3 out 
                  print("start join exposure, and t1")
                  dc <- left_join(d2, t1[,c("drug_concept_id","Name","type1","type2","dose")], by = c("drug_concept_id") ) 
                  print("delete error drug")
                  dc <- dc[which(dc$type1 != 'error' & dc$type2 != 'error'), ]
                  print("start unique join data")
                  dc<- unique(dc) 
                  print("check dc ")
                  str(dc)
                  rm(d2)
          # 2) 3제 요법 제외, target 임에도 불구하고 metformin 없는 경우 제외 
                  ## 성분 만 추출
                  print("start:::extract ingredient")
#                  dc2 <- as.data.frame(dc) # error 방지용 
                  print("start reshpae2::melt")
                  dc2 <- reshape2::melt(data= dc[,c("ID", "measurement_date.after","type1","type2")], id.vars =c("ID", "measurement_date.after"), measure.vars = c("type1", "type2"))
                  print("start making drug_list")
                  dc2 <- dc2 %>% group_by(ID, measurement_date.after) %>% summarise( drug_list = list(unique(value)))
                  #성분 / metformin 갯수/ 병용 약물군 정의  
                  print("count ingredient")
                  fun <- function(x){
                    x1 = x[x !='']
                    n= length(x1)
                    return(n)
                  }
                  count_ <- sapply(dc2$drug_list, fun)
                  print("put ingredient_count columns in df")
                  dc2$ingredient_count <- count_
                  print("put metformin_count")
                  dc2$metformin_count <- sapply(  dc2$drug_list,  function(x) length(grep( "metformin", x)))
                  print("put drug_group")
                  dc2$drug_group <- sapply(dc2$drug_list, function(x) paste(x, collapse="/"))
                  #합치기. 
                  #dc3 <- left_join(dc, dcc[,c("ID", "measurement_date.after", 'dose_type')], by =c("ID", "measurement_date.after"))
                  dc3 <- left_join(dc, dc2[,c("ID","measurement_date.after","ingredient_count","metformin_count","drug_group")], by=c("ID", "measurement_date.after"))
                  rm(dc2)
                  rm(dc)
                  ## 필터링.
                  dc4 <- dc3 %>% dplyr::filter(ingredient_count < 3 ) %>% dplyr::filter((cohort_type=='T' & metformin_count !=0) | cohort_type=='C') 
                  rm(dc3)        
                  print("how about dc4?????")
                  str(dc4)
           # 3) 용량군 정의  
                  # drug concep id 별 Total dose 계산  
                  print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! calculate total dose by drug concept id !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                  dc4$id_dose <- ( dc4$dose * (as.numeric(dc4$quantity)/ as.numeric(dc4$days_supply)))
                  dc4$usage <- dc4$quantity / dc4$days_supply 
                  # 측정 날짜를 기준으로 용량 list 생성하기.  (metformin 갯수에 상관없이 sum 적용)
                  #3 dose_list1: name 추출한 용량 그대로 list, dose_lsit2: id_dose를 계산해서 list

                  print(" make dose_list, dose columns")
                  dc5 <- dc4 %>% group_by(ID, measurement_date.after) %>% summarise( dose_list2 = list(unique(id_dose))) 
                  dc5 <- dc5 %>% group_by(ID, measurement_date.after) %>% mutate(total_dose = sum(unlist(dose_list2)))
                  dc51 <- dc4 %>% group_by(ID, measurement_date.after) %>% summarise( dose_list1 = list(unique(dose)))
 #                 dc52 <- dc4 %>% group_by(ID, measurement_date.after) %>% summarise(usage_list = list(unique(dc4$usage))) 
#                  dc53 <- dc4 %>% group_by(ID, measurement_date.after) %>% summarise( quantity_list  = list(unique(dc4$quantity)))  
#                  dc54 <- dc4 %>% group_by(ID, measurement_date.after) %>% summarise(days_list = list(unique(dc4$days_supply))) 
                  print("how about dc5???")
                  str(dc5)
                  print("how about dc51????")
                  str(dc51)
                  print("put dose columns in data frame ")
                  #합치기 
                  dc6 <- left_join(dc4, dc5[,c("ID", "measurement_date.after",  "dose_list2", "total_dose")], by=c("ID", "measurement_date.after"))
                  dc6 <- left_join(dc6, dc51[,c("ID", "measurement_date.after",  "dose_list1")], by=c("ID", "measurement_date.after"))
                  #dc6 <- left_join(dc6, dc52[,c("ID", "measurement_date.after",  "usage_list")], by=c("ID", "measurement_date.after"))
                  #dc6 <- left_join(dc6, dc53[,c("ID", "measurement_date.after",  "quantity_list")], by=c("ID", "measurement_date.after"))
                  #dc6 <- left_join(dc6, dc54[,c("ID", "measurement_date.after",  "days_list")], by=c("ID", "measurement_date.after"))
                  print("how about dc6?")
                  str(dc6)
                  rm(dc5)       
                  rm(dc4)
                  rm(dc51)
                  # rm(dc52)
                  # rm(dc53)
                  # rm(dc54)
            #4) 계산       
                  # #   ##계산
                  # dc6$total_dose <- (dc6$dose2 * as.numeric(dc6$quantity)) / as.numeric(dc6$days_supply)
                  # print("complete put total_dose column  s in data frame")
                  #   ## 용량군 정의 
                  dc6$dose_type <- ifelse(dc6$total_dose > 1500, "high", "low")
                  print("complete put dose_type columns in data frame")
                  print("!!!!!!!!!!!!!!! show me dose_Type 1 column !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                  str(dc6)
                    ## select only one dose_Type 
                    # print("select only one dose_Type")
                    # dcc <- reshape2::melt(data= dc[,c("ID", "measurement_date.after","dose_type1")], id.vars =c("ID", "measurement_date.after"), measure.vars = "dose_type1")
                    # dcc <-dcc %>% group_by(ID, measurement_date.after) %>% summarise( dose_list = list(unique(value))) %>% mutate(dose_type = case_when(
                    #       any(dose_list =='high') ~'high',
                    #       any(dose_list == 'middle') ~'middle', 
                    #       TRUE ~'low'))
                  # print("show me dcc ")
                  # str(dcc)

                  #                  unique(dc3[which(dc3$ingredient_count<3) & ((dc3$cohort_type=='T')& (dc3$metformin_count !=0 )) ],)
                  # id_list 추가 
                  # print("start3:: add id_list")
                  # dc7 <- reshape2::melt(data = dc6[,c("ID", "measurement_date.after", "drug_concept_id")], id.vars =c("ID",  "measurement_date.after"), measure.vars= c("drug_concept_id"))
                  # print("start3:: melt id list")
                  # dc7 <- dc7%>% group_by(ID, measurement_date.after) %>% summarise( id_list = list(unique(value)) )
                  # ## 합치기                   
                  # dc8 <- left_join(dc6, dc7[,c("ID", "measurement_date.after", "id_list")], by=c("ID",  "measurement_date.after"))
                  # rm(dc7)
                  # rm(dc6)
                  # print("show me dc8")
                  # str(dc8)
                  #ruleout <- dc8 %>% dplyr::distinct(ID, cohort_type, year,  measurement_type, value_as_number.before, value_as_number.after, measurement_date.before, measurement_date.after,
                  # gender, age, id_list , drug_group, dose_list1, dose_list2, total_dose, dose_type, usage_list, quantity_list ,days_list)
                  #rm(dc8)

                  ruleout <- dc6 %>% dplyr::distinct(ID, cohort_type, year,  measurement_type, value_as_number.before, value_as_number.after, measurement_date.before, measurement_date.after,
                   gender, age, drug_group, dose_list1, dose_list2, total_dose, dose_type)
                  ## select latest measurement_data
                  print("ruleout:: select latest data")
                  # ruleout2 <- ruleout %>% dplyr::arrange(ID, measurement_type, desc(measurement_date.after)) %>% group_by(ID, measurement_type) %>% dplyr::mutate( row = row_number())
                  # ruleout2<- as.data.frame(ruleout2)
                  # ruleout3 <- ruleout2 %>% dplyr::filter(row ==1)
                  # ruleout3 <- ruleout3 %>% select(-row)
                  setDT(ruleout)
                  ruleout <- ruleout[,.SD[which.max(measurement_date.after)], by=.(ID, measurement_type)]
                  #ruleout_min <- ruleout[,.SD[which.min(measurement_date.after)], by=.(ID, measurement_type)]
                  #print("ruleout_max, min:::")
                  #ruleout <- list( earliest = ruleout_min, latest = ruleout_max)
                  print("flatten list column ")
#                  ruleout$id_list <- vapply(ruleout$id_list, paste, collapse = ", ", character(1L)) 
                  ruleout$dose_list1 <- vapply(ruleout$dose_list1, paste, collapse = ", ", character(1L)) 
                  ruleout$dose_list2 <- vapply(ruleout$dose_list2, paste, collapse = ", ", character(1L)) 
 #                 ruleout$usage_list <- vapply(ruleout$usage_list, paste, collapse = ", ", character(1L)) 
  #                ruleout$quantity_list <- vapply(ruleout$quantity_list, paste, collapse = ", ", character(1L)) 
  #                ruleout$days_list <- vapply(ruleout$days_list, paste, collapse = ", ", character(1L)) 
 
                  str(ruleout)
                  return(ruleout) #min, max 
                  }
})






