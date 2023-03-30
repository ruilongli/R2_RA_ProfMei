###there are problem with find horizon and the relationship between fpi
###I am also not clear that what inactive mean
###then the way I wrote in select latest forecast is too slow
###I also did not do the method to filter shrcd
library(tidyverse)
library(lubridate)
library(RSQLite)
library(dbplyr)
library(RPostgres)
library(RPostgreSQL)
library(DatabaseConnector)
library(data.table)

start_date <- ymd("1998-01-01")
end_date <- ymd("2000-12-31")
drv <- Postgres()
wrds <- dbConnect(
  drv,
  host = "wrds-pgdata.wharton.upenn.edu",
  dbname = "wrds",
  port = 9737,
  sslmode = "require",
  user = "ruilongli",
  password = "13183707189Lrl"
)

##in this table, the direct use of predict in earning is missing

ibes_withactual_adjusted<-tbl(wrds,in_schema("ibes", "det_epsus"))
crsp_dse <- tbl(wrds, in_schema("crsp", "dsf"))
compustat_funda <- tbl(wrds, in_schema("comp", "funda"))
ibes_crsp_link<-tbl(wrds,in_schema("wrdsapps", "ibcrsphist"))

fundamental<-compustat_funda|>
  filter(datadate>= start_date&datadate<=end_date)|>
  select(datadate,cusip,at,gvkey)|>
  mutate(cusip=substr(cusip,0,8))|>
  mutate(yr=ifelse(month(datadate)>6,year(datadate),year(datadate)-1))|>
  
  filter(!is.na(at))|>
  mutate(at=at*1000000)|>
  collect()

##but what is this revdates
ibes <- ibes_withactual_adjusted  |>
  filter(measure == "EPS") |>
  filter(!is.na(actual)&!is.na(anndats))|>
  filter(anndats>start_date& anndats_act<end_date)|>
  filter(fpi %in% c("1","2","3","4","5"))|>
  filter(anndats<anndats_act & anndats_act>fpedats)|>
  collect()

ibes_crsp_lk<-ibes_crsp_link|>
  collect()

ibes_with_permno<-inner_join(ibes,ibes_crsp_lk,by=c("ticker"))|>
  filter(!is.na(permno)&anndats >= sdate & anndats <= edate)

crsp<-crsp_dse|>
  filter(date>=start_date&date<=end_date)|>
  collect()

ibes_crsp<-inner_join(ibes_with_permno,crsp,by=c("permno","anndats"="date"))|>
  select(anndats,actual,analys,anndats_act,cusip.x,fpedats,fpi,value,shrout,permno)|>
  mutate(earning_pred=value*shrout*1000)|>
  mutate(earning_act=actual*shrout*1000)|>
  mutate(len=anndats_act-anndats)

ccmxpf_linktable_db <- tbl(
  wrds,
  in_schema("crsp", "ccmxpf_linktable")
)

ccmxpf_linktable <- ccmxpf_linktable_db |>
  filter(linktype %in% c("LU", "LC") &
           linkprim %in% c("P", "C") &
           usedflag == 1) |>
  select(permno = lpermno, gvkey, linkdt, linkenddt) |>
  collect() |>
  mutate(linkenddt = replace_na(linkenddt, today()))

ccm_links <- crsp |>
  inner_join(ccmxpf_linktable, by = "permno") |>
  filter(!is.na(gvkey) & (date >= linkdt & date <= linkenddt)) |>
  select(permno, gvkey, date)

#here still need check on replicate and take the unique!!!!!!!!!!!!!!
#######################################################################
final_table<- ibes_crsp|>
  inner_join(ccm_links,by=c("permno","anndats_act"="date"))|>
  mutate(yr=ifelse(month(anndats_act)>6,year(anndats_act),year(anndats_act)-1))|>
  inner_join(fundamental,by=c("gvkey","yr"))|>
  mutate(n_pred_earning= earning_pred/at)|>
  mutate(n_act_earning=earning_act/at)


final_table<-final_table|>
  filter(at>earning_pred*10)


############################
###########################
regression_results <- final_table |>
  mutate(month_len=len/30)|>
  group_by(fpedats,fpi,analys,cusip) |>
  slice_max(anndats)|>
  ungroup()|>
  unique()



regression_results<-regression_results|>
  ########what to group by here? year of fpedates, or fpedates direcely?
  group_by(fpi,analys,fpedats)|>
  nest()|>
  mutate(sample_size=map(data,nrow))|>
  filter(sample_size>3&sample_size<30)


model<-list()
####set negative pred R2 into 0
for(i in 1:length(regression_results$analys)){
  model[[i]]<-lm(n_act_earning ~ n_pred_earning, data = regression_results$data[i][[1]])
  if(coef(model[[i]])[2]<=0){
    regression_results$r2[i]<-0
  }
  if(coef(model[[i]])[2]>0){
    regression_results$r2[i]<-summary(model[[i]])$r.square
  }
  regression_results$len_avg[i]<- mean(regression_results$data[i][[1]]$month_len)
}


simplified_res<-regression_results|>
  mutate(len_avg=round(len_avg,digits=1))

simplified_res<-as.data.table(simplified_res)
a<-simplified_res[,mean(r2),by=.(len_avg)]
ggplot(a)+
  geom_smooth(aes(x=len_avg,y=V1))

####################################################
####debug part
###to see which part of regression gives NA as its rsquare


problem<-vector()
for(i in 1:length(regression_results$analys)){
  if(summary(model[[i]])$r.square==0&!is.na(summary(model[[i]])$r.square)){
    problem<-append(problem,i)
  }
}

df<-regression_results[problem,]


###select the latest report

a<-regression_results[[4]][[1]]
b<-a|>
  group_by(cusip,fpedats)|>
  slice_max(anndats)|>
  ungroup()

#how many data the ibes have
ibes <- ibes_withactual_adjusted  |>
  filter(measure == "EPS") |>
  filter(!is.na(actual)&!is.na(anndats))|>
  filter(fpi %in% c("1","2","3","4","5"))|>
  collect()
########the whole IBES database only have 10915147 data about eps, why the paper would have so many?