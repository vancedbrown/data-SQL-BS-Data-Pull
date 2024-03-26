library(tidyverse)
library(readr)
library(here)
library(lubridate)

# weekindex<-read_csv('indexarchive.csv',col_types = cols(BoarID = col_character())) %>%
#   filter(WeekBeginning=='2022-06-20')

############## MAKE SURE TO CHECK CORRECT INDEX FILE IS BEING PULLED!!!!!##########

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

pig <- "SELECT [StudID] AS 'Boar Stud'
,[BoarID]
,[Breed]
,[Status] AS 'Boar Status'
,[Date_Arrival]
,[Date_Studout]
,[Dispose_Code]
FROM [Intranet].[dbo].[Boar_Pig]
WHERE [BoarID] not like ('PIC%')"
pigraw <- getSQL('Intranet',query=pig)

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

index<-read_csv('index.csv',col_types = cols(Tattoo = col_character()))

# index<-index[c(2,4)]

pigraw1<-left_join(x = pigraw, y = weekindex, by=c("BoarID"="BoarID"))

pigraw1<-pigraw1[c(-9)]

pigraw1<-left_join(x = pigraw, y=index,by=c("BoarID"="Tattoo"))

pig<-pigraw1 %>%
  filter(`Boar Stud`%in%studs) %>%
  mutate(Date_Arrival=as.Date(Date_Arrival),
         Date_Studout=as.Date(Date_Studout))

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

write_csv(x = coll,path=here::here('coll.csv'),append = FALSE)
write_csv(x = dist,path = here::here('dist.csv'), append = FALSE)
write_csv(x = split,path = here::here('split.csv'), append = FALSE)
write_csv(x = pig,path = here::here('pig.csv'), append = FALSE)
write_csv(x = look,path = here::here('look.csv'), append = FALSE)
write_csv(x = trt,path = here::here('trt.csv'), append = FALSE)
write_csv(x = data3,path = here::here('dates.csv'), append = FALSE)
write_csv(x = batch,path = here::here('batch.csv'), append = FALSE)

############### PULL INDEXES FOR NEXT WEEK #######################


oldindex<-read_csv('index.csv',col_types = cols(Tattoo = col_character()))
write_csv(x = oldindex,path = here::here('oldindex.csv'),append = FALSE)

ind <- "SELECT
[SPGid] as 'Tattoo'
,[DV_IDX] as 'Index'
FROM [Intranet].[dbo].[ProductIdx]
WHERE [Product] in ('D1_Heat','LW_SPG','LR_GN')"
indexraw <- getSQL('Intranet',query=ind)

ind2 <- "SELECT a.[BoarID] AS 'Tattoo'
	  ,b.[idx] AS 'Index'
  FROM [Intranet].[dbo].[Boar_Pig] a
  inner join [OADB].[reports].[idxCurrent] b on a.[Name] = b.[spg_id]
  WHERE [Breed] in ('PICL02', 'PICL03','PIC800', 'DNA200','DNA400','DNA600','TNLR')"
indexraw2 <- getSQL('Intranet',query=ind2)

indexraw3<-rbind(indexraw,indexraw2)


write_csv(x = indexraw3,path = here::here('index.csv'), append = FALSE)

######## BUILD INDEX ARCHIVE ############

pig1<-pig %>%
  mutate("WeekBeginning"=floor_date(x = today(),unit = "week",week_start = 1)-7) %>%
  filter(Date_Arrival>='2016-01-01') %>%
  select(c(2,8,9))

write_csv(x = pig1,path = here::here("indexarchive.csv"),append = TRUE)


############## SAVE FILES FOR NETWORK FOLDER ###################


write_csv(x = coll,path='//spgfs1/SemenQATesting/PRISM Files/coll.csv',append = FALSE)
write_csv(x = dist,path ='//spgfs1/SemenQATesting/PRISM Files/dist.csv', append = FALSE)
write_csv(x = split,path ='//spgfs1/SemenQATesting/PRISM Files/split.csv', append = FALSE)
write_csv(x = pig,path = '//spgfs1/SemenQATesting/PRISM Files/pig.csv', append = FALSE)
write_csv(x = look,path = '//spgfs1/SemenQATesting/PRISM Files/look.csv', append = FALSE)
write_csv(x = trt,path = '//spgfs1/SemenQATesting/PRISM Files/trt.csv', append = FALSE)
write_csv(x = data3,path = '//spgfs1/SemenQATesting/PRISM Files/dates.csv', append = FALSE)
write_csv(x = batch,path = '//spgfs1/SemenQATesting/PRISM Files/batch.csv', append = FALSE)


