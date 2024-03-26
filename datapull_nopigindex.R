library(tidyverse)
library(readr)
library(here)
library(lubridate)

############## THIS SCRIPT SHOULD BE USED FOR MID-WEEK DATA PULL, NO PIG OR INDEX!!!##########

studs<-c('MB 7081',
         'MB 7082',
         'MB 7092',
         'MB 7093',
         'MB 7094',
         'MBW Cimarron',
         'MBW Cyclone',
         'MBW Yuma',
         'Skyline Boar Stud',
         'SPGNC',
         'SPGVA')

source('C:/Users/vance/Documents/myR/functions/getSQL.r')



distrib <- "SELECT [StudID] as 'Boar Stud'
,[Dest]
,[Date_Shipped]
,[Breed]
,[Doses]
,[BatchNum]
,[BatchID]
,[Accounting]
FROM [Intranet].[dbo].[Boar_Distrib]
WHERE [Date_Shipped] > '2020-01-01 00:00:00'"
distraw <- getSQL('Intranet',query=distrib)

split <- "SELECT [StudID] as 'Boar Stud'
,[BoarID]
,[Collnum]
,[BatchNum]
FROM [Intranet].[dbo].[Boar_Split]"
splitraw <- getSQL('Intranet',query=split)


coll <- "SELECT [StudID] as 'Boar Stud'
,[Collnum]
,[BoarID]
,[Col_Date]
,[Tech]
,[Ext_Name]
,[Tot_Ext_Vol]
,[Used_Doses]
,[Dose_Actual]
,[Status] AS 'Collection Status'
,[Sperm_Conc]
,[Tot_Sperm]
,[Sem_Vol_Used]
,[TRCODE]
,[VAL1] AS 'Motility'
,[Ext_Batch_Num]
,[EBV]
FROM [Intranet].[dbo].[Boar_Collection]
WHERE  [Col_Date]>'2020-01-01 00:00:00'"
collraw <- getSQL('Intranet',query=coll)

look <- "SELECT [StudID] as 'Boar Stud'
      ,[ID]
,[DESCR]
FROM [Intranet].[dbo].[Boar_LookUp]"
lookraw <- getSQL('Intranet',query=look)

trt <- "SELECT [StudID] AS 'Boar Stud'
      ,[BoarID]
,[TCode]
,[TDate]
,[TDays]
,[TPrev]
,[TDesc]
FROM [Intranet].[dbo].[Boar_APPTMT]
WHERE  [TDate]>'2020-01-01 00:00:00'"
trtraw <- getSQL('Intranet',query=trt)

batch <- "SELECT [StudID] AS 'Boar Stud'
      ,[BATCHNUM]
,[COL_DATE]
,LEFT([EXT_NAME],2) AS TYPE
,[POST_MOT_VAL_1]
,[POST_MOT_VAL_2]
,[POST_MOT_VAL_3]
,[POST_MOT_VAL_4]
,[POST_MOT_VAL_5]
,[POST_MOT_VAL_6]
FROM [Intranet].[dbo].[Boar_Batch]"
batchraw <- getSQL('Intranet',query=batch)

################################################################

coll<-collraw %>% 
  filter(`Boar Stud`%in%studs) %>% 
  mutate(Col_Date=as.Date(Col_Date))

dist<-distraw %>% 
  filter(`Boar Stud`%in%studs) %>% 
  mutate(Date_Shipped=as.Date(Date_Shipped))

split<-splitraw %>% 
  filter(`Boar Stud`%in%studs)


look<-lookraw %>% 
  filter(`Boar Stud`%in%studs)

trt<-trtraw %>% 
  filter(`Boar Stud`%in%studs) %>% 
  mutate(TDate=as.Date(TDate))

batch<-batchraw %>% 
  filter(`Boar Stud`%in%studs)

data1<-coll %>% 
  group_by(`Boar Stud`) %>% 
  summarise('Collection Date'=max(Col_Date))

data2<-dist %>%
  group_by(`Boar Stud`) %>% 
  summarise('Distribution Date'=max(Date_Shipped))

data3<-left_join(x = data1,y = data2,by=c("Boar Stud"="Boar Stud"))

write_csv(x = coll,file =here::here('coll.csv'),append = FALSE)
write_csv(x = dist,file = here::here('dist.csv'), append = FALSE)
write_csv(x = split,file = here::here('split.csv'), append = FALSE)
write_csv(x = look,file = here::here('look.csv'), append = FALSE)
write_csv(x = trt,file = here::here('trt.csv'), append = FALSE)
write_csv(x = data3,file = here::here('dates.csv'), append = FALSE)
write_csv(x = batch,file = here::here('batch.csv'), append = FALSE)


############## SAVE FILES FOR NETWORK FOLDER ###################


write_csv(x = coll,path='//spgfs1/SemenQATesting/PRISM Files/coll.csv',append = FALSE)
write_csv(x = dist,path ='//spgfs1/SemenQATesting/PRISM Files/dist.csv', append = FALSE)
write_csv(x = split,path ='//spgfs1/SemenQATesting/PRISM Files/split.csv', append = FALSE)
write_csv(x = look,path = '//spgfs1/SemenQATesting/PRISM Files/look.csv', append = FALSE)
write_csv(x = trt,path = '//spgfs1/SemenQATesting/PRISM Files/trt.csv', append = FALSE)
write_csv(x = data3,path = '//spgfs1/SemenQATesting/PRISM Files/dates.csv', append = FALSE)
write_csv(x = batch,path = '//spgfs1/SemenQATesting/PRISM Files/batch.csv', append = FALSE)
