SELECT * FROM `dex-initial-load-aws`.`2019-full-data` ;

insert into `dex-initial-load-aws`.`2018_delete`
select * from `dex-initial-load-aws`.`2018-full-data` limit 10;

insert into  `dex-initial-load-aws`.`alldata`
(select * from `dex-initial-load-aws`.`2018-full-data` )
union
(select * from `dex-initial-load-aws`.`2019-full-data` )
union
(select * from `dex-initial-load-aws`.`2020-full-data` )
union
(select * from `dex-initial-load-aws`.`2021-full-data` );

select `Transaction Date` from `dex-initial-load-aws`.`alldata` ;


SELECT * FROM `dex-initial-load-aws`.lastContacted;

load data local infile '/Users/janakiram/Downloads/lastContacted.xlsx - report1626236878036.csv'  
into table `dex-initial-load-aws`.`lastContacted` fields  
terminated by ',' lines terminated by '\n' 
(@`Agency: Last Activity Date`) 
set `Agency: Last Activity Date` = date_format(@`Agency: Last Activity Date`,'%d/%m/%Y');

show global variables like 'local_infile';

-- Error Code: 2068. LOAD DATA LOCAL INFILE file request rejected due to restrictions on access.
-- Error Code: 2068. LOAD DATA LOCAL INFILE file request rejected due to restrictions on access.



SET SQL_SAFE_UPDATES = 0;

UPDATE `dex-initial-load-aws`.lastContacted SET `Agency: Last Activity Date`=STR_TO_DATE(`Agency: Last Activity Date`,'%d/%m/%Y');


	load data local infile '/usr/tmp/2018-full-data.xlsx - Travel Agency Report.csv'
	 into table `2018-full-data` fields 
	 terminated by ',' lines terminated by '\n'   
	 IGNORE 1 LINES 
	 (`@Agency: Last Activity Date`)
	 set `Agency: Last Activity Date` = str_to_date(`@Agency: Last Activity Date`, '%d/%m/%Y');
 
 
 load data local infile '/usr/tmp/2018-full-data.xlsx - Travel Agency Report.csv'
 into table `2018-full-data` fields 
 terminated by ',' lines terminated by '\n'   
 IGNORE 1 LINES 
 set `Agency: Last Activity Date` = str_to_date(`@Agency: Last Activity Date`, '%d/%m/%Y');
 
 
 SELECT sum(`Gross Transaction`), month(STR_TO_DATE(`Transaction Date`,'%d/%m/%Y')) as rmonth, year(STR_TO_DATE(`Transaction Date`,'%d/%m/%Y')) as ryear
, Tier
FROM `dex-initial-load-aws`.`alldata`
group by Tier, month(STR_TO_DATE(`Transaction Date`,'%d/%m/%Y')), year(STR_TO_DATE(`Transaction Date`,'%d/%m/%Y'))
order by rmonth, ryear;

SELECT month(STR_TO_DATE(`Transaction Date`,'%d/%m/%Y')), year(STR_TO_DATE(`Transaction Date`,'%d/%m/%Y'))
FROM `dex-initial-load-aws`.`alldata`;
SELECT `Transaction Date`,'%d/%m/%y'
FROM `dex-initial-load-aws`.`alldata`;

SELECT * FROM `dex-initial-load-aws`.lastContacted;
TRUNCATE `dex-initial-load-aws`.`lastContacted`;
load data local infile '/Users/janakiram/Downloads/lastContacted.xlsx - report1626236878036.csv'  
into table `dex-initial-load-aws`.`lastContacted` fields  
terminated by ',' lines terminated by '\n' 
(`Agency: Tracking Code`, `Agency: Agency Name`,`First Name`,`Last Name`,`Assigned`,`Type`
,@`Agency: Last Activity Date`)
set `Agency: Last Activity Date` = date_format(@`Agency: Last Activity Date`,'%d/%m/%Y');
SELECT * FROM `dex-initial-load-aws`.lastContacted;


show global variables like 'local_infile';

-- Error Code: 2068. LOAD DATA LOCAL INFILE file request rejected due to restrictions on access.
-- Error Code: 2068. LOAD DATA LOCAL INFILE file request rejected due to restrictions on access.



SET SQL_SAFE_UPDATES = 0;

UPDATE `dex-initial-load-aws`.lastContacted SET `Agency: Last Activity Date`=STR_TO_DATE(`Agency: Last Activity Date`,'%d/%m/%Y');


	load data local infile '/usr/tmp/2018-full-data.xlsx - Travel Agency Report.csv'
	 into table `2018-full-data` fields 
	 terminated by ',' lines terminated by '\n'   
	 IGNORE 1 LINES 
	 (`@Agency: Last Activity Date`)
	 set `Agency: Last Activity Date` = str_to_date(`@Agency: Last Activity Date`, '%d/%m/%Y');
 
 
 load data local infile '/usr/tmp/2018-full-data.xlsx - Travel Agency Report.csv'
 into table `2018-full-data` fields 
 terminated by ',' lines terminated by '\n'   
 IGNORE 1 LINES 
 set `Agency: Last Activity Date` = str_to_date(`@Agency: Last Activity Date`, '%d/%m/%Y');
 
 
 SELECT sum(`Gross Transaction`), month(STR_TO_DATE(`Transaction Date`,'%d/%m/%Y')) as rmonth, year(STR_TO_DATE(`Transaction Date`,'%d/%m/%Y')) as ryear
, Tier
FROM `dex-initial-load-aws`.`alldata`
group by Tier, month(STR_TO_DATE(`Transaction Date`,'%d/%m/%Y')), year(STR_TO_DATE(`Transaction Date`,'%d/%m/%Y'))
order by rmonth, ryear;

-- QA needs to be done, all metrics for 2021 YTD
-- created sub queries, create more sub queries -> like join momperf with master and yoyperf with master
select 
-- direct metrics
a.`Tracking Code`, a.`Travel Agency`, count(a.`Gross Transaction`) as TxnCount, 
round(sum(a.`Gross Transaction`),2) as GrossTxn,
round(avg(a.`Gross Transaction`),2) as AvgTxn, 
 round(sum(a.`Commission`),2) as TotalCommission,
 round(sum(a.`Gross Transaction`),2) as PerAgentGBV, a.`Tier`, 
-- cancellation
(count(a.`Tracking Code`)/inter_totals.total * 100) as `cancellation %`,
-- last booking
inter_salesRank.`Gross Transaction` as LastBookingDateGBV,
inter_salesRank.`Transaction Date` as LastBookingDate,
lastContacted.`Agency: Last Activity Date` as lastContactDate
-- yoy 
 ,inter_yoyperf.YearGBV, inter_yoyperf.BookingYear 
-- 
FROM -- only selecting 2021 data for reducing time load, full data not working high time load
(select * 
from `dex-initial-load-aws`.`alldata` 
where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021' ) a
left join -- to know "cancellation %"
inter_totals
on a.`Tracking Code` = inter_totals.`Tracking Code`
left join -- to know "LastBookingDate"
inter_salesRank
on a.`Tracking Code` = inter_salesRank.`Tracking Code`
left join -- to know "LastContactDate"
lastContacted 
on a.`Tracking Code` = lastContacted.`Agency: Tracking Code`
left join -- to know "YOY GBV"
inter_yoyperf
on a.`Tracking Code` = inter_yoyperf.`Tracking Code`
left join -- to know mom perf
inter_momperf
on a.`Tracking Code` = inter_momperf.`Tracking Code`
-- where a.`Transaction Status` = 'Canceled'  and a.`Notes` = 'Package Rate'
group by a.`Tracking Code`, inter_yoyperf.`BookingYear`;

-- mom perf
create table inter_momperf as
select 
`Tracking Code`, 
round(sum(`Gross Transaction`),2) as MonthGBV, 
MONTHNAME(str_to_date(`Transaction Date`,'%d/%m/%Y')) as BookingMonth
FROM `dex-initial-load-aws`.`alldata` 
where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021' 
group by `Tracking Code`, month(str_to_date(`Transaction Date`,'%d/%m/%Y'));

-- yoy perf
create table inter_yoyperf as
select `Tracking Code`, round(sum(`Gross Transaction`),2) as YearGBV, 
year(str_to_date(`Transaction Date`,'%d/%m/%Y')) as BookingYear
FROM `dex-initial-load-aws`.`alldata` 
group by `Tracking Code`, year(str_to_date(`Transaction Date`,'%d/%m/%Y'));

-- sales rank
create table inter_salesRank as
select * from 
(SELECT `Transaction Date`,`Gross Transaction` , `Tracking Code`,
RANK() OVER ( partition by `Tracking Code` ORDER BY str_to_date(`Transaction Date`,'%d/%m/%Y') DESC ) sales_rank
FROM `dex-initial-load-aws`.`alldata`) tb where tb.`sales_rank` = 1;

-- totals 
create table inter_totals as
select `Tracking Code` , count(`Tracking Code`) as total 
from `dex-initial-load-aws`.`alldata`
where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021' --  and `Tracking Code` ='WI01944'
 group by `Tracking Code`;

-- mom perf with all data 2021
-- create table inter2_mom_all as
select a.*,b.`MonthGBV`, b.`BookingMonth` from 
(select * 
from `dex-initial-load-aws`.`alldata` 
where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021') a
left join 
(select * from inter_momperf) b
on a.`Tracking Code` = b.`Tracking Code`
group by a.`Tracking Code`, b.`Tracking Code`, b.`MonthGBV`, b.`BookingMonth`;

-- yoy perf with all data 2021
create table inter2_yoy_all as
select a.*, b.`YearGBV`, b.`BookingYear` from 
(select * 
from `dex-initial-load-aws`.`alldata` 
where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021') a
left join 
(select * from inter_yoyperf) b
on a.`Tracking Code` = b.`Tracking Code`
group by a.`Tracking Code`, b.`Tracking Code`, b.`YearGBV`, b.`BookingYear`;

-- lastContacted 
create table inter3_lastContacted as
select inter2_yoy_all.*, b.`Agency: Last Activity Date`
from inter2_yoy_all
left join 
(select * from lastContacted -- where `Agency: Tracking Code` = 'WI06472'
group by `Agency: Last Activity Date`, `Agency: Tracking Code`) b
on b.`Agency: Tracking Code` = inter2_yoy_all.`Tracking Code`;

-- sales rank, LastBookingDate, LastBookingGBV
create table inter4_LastBookingDate as
select a.*, inter_salesRank.`Gross Transaction` as LastBookedGBV,
inter_salesRank.`Transaction Date` as LastBookingDate from 
(select *
from inter3_lastContacted) a
left join 
`dex-initial-load-aws`.inter_salesRank
on a.`Tracking Code` = inter_salesRank.`Tracking Code`;
 
 
-- cancellation % pre join
create table inter_prejoin_cancellationDate as 
select a.`Tracking Code`,
(count(a.`Tracking Code`)/inter_totals.total * 100) as `cancellation %`
from 
(select * 
 from `dex-initial-load-aws`.`alldata` 
 where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021'
 and `Transaction Status` = 'Canceled') a
left join 
inter_totals 
on a.`Tracking Code` = inter_totals.`Tracking Code`
group by a.`Tracking Code`, inter_totals.`Tracking Code` ;

-- booking % pre join
create table inter_prejoin_bookingDate as
select a.`Tracking Code`,
(count(a.`Tracking Code`)/inter_totals.total * 100 ) as `booking %`
from 
(select * 
 from `dex-initial-load-aws`.`alldata` 
 where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021'
 and `Transaction Status` = 'Booked') a
left join 
inter_totals 
on a.`Tracking Code` = inter_totals.`Tracking Code`
group by a.`Tracking Code`, inter_totals.`Tracking Code` ;

-- cancellation % 
create table inter5_cancellationDate as
select inter4_LastBookingDate.* , 
round(inter_prejoin_cancellationDate.`cancellation %`,2) as `cancellation%`
from inter4_LastBookingDate
left join 
inter_prejoin_cancellationDate 
on inter4_LastBookingDate.`Tracking Code` = inter_prejoin_cancellationDate.`Tracking Code`;

-- booking % , write query for this and join that for the table below.
create table inter6_bookingDate as 
select inter5_cancellationDate.* ,round(inter_prejoin_bookingDate.`booking %`,2) as `booking%`
from
inter5_cancellationDate
left join 
inter_prejoin_bookingDate
on inter5_cancellationDate.`Tracking Code`= inter_prejoin_bookingDate.`Tracking Code`;

-- commission payout previous year
create table inter7_commissionPrevYear as
select inter6_bookingDate.*, b.cmsnPrvYear
from inter6_bookingDate 
left join
(select `Tracking Code` , round(sum(`Commission`),2) as cmsnPrvYear
from `dex-initial-load-aws`.`alldata` 
where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2020'
group by `Tracking Code`) b 
on b.`Tracking Code` = inter6_bookingDate.`Tracking Code`;

-- commission tier 
-- Premium, Basic, Basic Plus
create table inter_prejoin_commissionTier as
select  
a.`Tracking Code`, a.`Commission Tier`,
(count(a.`Commission Tier`)/inter_totals.total * 100) as 'tier%'
from 
(select * from alldata where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021' -- and `Tracking Code`= 'WI06472'
) a
left join inter_totals
on a.`Tracking Code` = inter_totals.`Tracking Code`
group by a.`Tracking Code`, a.`Commission Tier`
order by a.`Tracking Code` desc;

-- > join commission tier, execute it
create table inter8_hotelCommissionTier as
select inter7_commissionPrevYear.`Point of Sale`,
inter7_commissionPrevYear.`Travel Agency`,
inter7_commissionPrevYear.`Tracking Code`,
inter7_commissionPrevYear.`Tier`,
inter7_commissionPrevYear.`Status`,
inter7_commissionPrevYear.`Travel Agent`,
inter7_commissionPrevYear.`Expedia Account`,
inter7_commissionPrevYear.`Itinerary Number`,
inter7_commissionPrevYear.`Transaction Status`,
inter7_commissionPrevYear.`Transaction Date`,
inter7_commissionPrevYear.`Deferred Payment Date`,
inter7_commissionPrevYear.`StartDate`,
inter7_commissionPrevYear.`Traveler Name`,
inter7_commissionPrevYear.`Line of Business`,
inter7_commissionPrevYear.`Gross Transaction`,
inter7_commissionPrevYear.`Commission`,
inter7_commissionPrevYear.`Commission Rate`,
inter7_commissionPrevYear.`Payment With`,
inter7_commissionPrevYear.`Commission Month`,
inter7_commissionPrevYear.`Notes`,
inter7_commissionPrevYear.`Voucher Eligible`,
inter7_commissionPrevYear.`YearGBV`,
inter7_commissionPrevYear.`BookingYear`,
inter7_commissionPrevYear.`Agency: Last Activity Date`,
inter7_commissionPrevYear.`LastBookedGBV`,
inter7_commissionPrevYear.`LastBookingDate`,
inter7_commissionPrevYear.`cancellation%`,
inter7_commissionPrevYear.`booking%`,
inter7_commissionPrevYear.`cmsnPrvYear`, 
inter_prejoin_commissionTier.`Commission Tier`,
inter_prejoin_commissionTier.`tier%`
from inter7_commissionPrevYear
left join 
inter_prejoin_commissionTier
on inter7_commissionPrevYear.`Tracking Code` = inter_prejoin_commissionTier.`Tracking Code`
group by  
inter_prejoin_commissionTier.`Tracking Code`,
inter_prejoin_commissionTier.`Commission Tier`,
inter_prejoin_commissionTier.`tier%`,
inter7_commissionPrevYear.`YearGBV`,
inter7_commissionPrevYear.`BookingYear` ;

-- payment with expedia or supplier
create table inter_prejoin_paymentwith as
select a.`Tracking Code`,`Payment With` as `PayWithFlag`, (count(a.`Payment With`)/inter_totals.total * 100) as 'Payment%'
from
(select * from `alldata` where year(str_to_date(`Transaction Date`,'%d/%m/%Y'))='2021'  ) a
left join 
inter_totals
on a.`Tracking Code` = inter_totals.`Tracking Code`
group by a.`Tracking Code`, a.`Payment with`;

-- create inter9_paymentwith
create table inter9_paymentwith as
select inter8_hotelCommissionTier.*,
inter_prejoin_paymentwith.`PayWithFlag`, inter_prejoin_paymentwith.`Payment%`
 from inter8_hotelCommissionTier
left join 
inter_prejoin_paymentwith
on inter8_hotelCommissionTier.`Tracking Code` = inter_prejoin_paymentwith.`Tracking Code`;

SET SQL_SAFE_UPDATES = 0;

update `per-agent-trial` set `Agency: Last Activity Date` = replace(`Agency: Last Activity Date`,'\r\n','');

-- create inter10_momperf
create table inter10_momperf as
select inter9_paymentwith.*, inter_momperf.BookingMonth, inter_momperf.MonthGBV
from inter9_paymentwith 
left join 
inter_momperf
on inter9_paymentwith.`Tracking Code` = inter_momperf.`Tracking Code`;

select replace(`Agency: Last Activity Date`,'\r','') from `per-agent-trial`
into outfile '/Users/Downloads/per-agent-3.csv';

-- total final query , previous one -> use this, as we are not including 
-- find if there is any pattern in requirements, otherwise implement it independently
select * from `per-agent-trial` limit 100;
create table `per-agent-trial` as
select 
b.*,
round(inter10_momperf.`MonthGBV`) as `MonthGBV`,
inter10_momperf.`BookingMonth`,
round(inter10_momperf.`Payment%`) as `Payment%`,
inter10_momperf.`PayWithFlag` ,
inter10_momperf.`Travel Agency`,
round(inter10_momperf.`tier%`) as `tier%`,
inter10_momperf.`Commission Tier`,
round(inter10_momperf.`cmsnPrvYear`) as `cmsnPrvYear`,
round(inter10_momperf.`booking%`) as `booking%`,
round(inter10_momperf.`cancellation%`) as `cancellation%`,
inter10_momperf.`LastBookingDate`,
round(inter10_momperf.`LastBookedGBV`) as `LastBookedGBV`,
replace(inter10_momperf.`Agency: Last Activity Date`,'\r','') as 'AgencyLastContacted',
inter10_momperf.`BookingYear`,
round(inter10_momperf.`YearGBV`) as `YearGBV`
 from
(select 
 a.`Tracking Code`, 
count(a.`Gross Transaction`) as TxnCount, 
round(sum(a.`Gross Transaction`)) as GrossTxn,
round(avg(a.`Gross Transaction`)) as AvgTxn, 
 round(sum(a.`Commission`)) as TotalCommission,
 round(sum(a.`Gross Transaction`)) as PerAgentGBV,
 count(a.`Tracking Code`) as NumberOfTxn
 from
 (select * from alldata 
where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021' 
) a
group by a.`Tracking Code`) b
left join 
inter10_momperf 
on b.`Tracking Code` = inter10_momperf.`Tracking Code`;

select * from 
(select `Tracking Code`, count(*) -- *
 from `2021-full-data` -- limit 100
  where `Notes` = ''
 group by `Tracking Code`, `Notes`) a
 left join
(select `Tracking Code`, count(*) -- *
 from `2021-full-data` -- limit 100
  where `Notes` = 'Package Rate'
 group by `Tracking Code`, `Notes`) b
 on a.`Tracking Code` = b.`Tracking Code`;
 
-- total final query , recent one, rewrite query
create table `per-agent-trial` as
select 
b.*,
inter11_dormancy.`MonthGBV`,
inter11_dormancy.`BookingMonth`,
inter11_dormancy.`Payment%`,
inter11_dormancy.`PayWithFlag`,
inter11_dormancy.`Travel Agency`,
inter11_dormancy.`tier%`,
inter11_dormancy.`Commission Tier`,
inter11_dormancy.`cmsnPrvYear`,
inter11_dormancy.`booking%`,
inter11_dormancy.`cancellation%`,
inter11_dormancy.`LastBookingDate`,
inter11_dormancy.`LastBookedGBV`,
replace(inter11_dormancy.`Agency: Last Activity Date`,'\r','') as 'AgencyLastContacted',
inter11_dormancy.`BookingYear`,
inter11_dormancy.`YearGBV`,
inter11_dormancy.`30DormancyPeriod`,
inter11_dormancy.`60DormancyPeriod`,
inter11_dormancy.`90DormancyPeriod`
 from
(select 
 a.`Tracking Code`, 
count(a.`Gross Transaction`) as TxnCount, 
round(sum(a.`Gross Transaction`),2) as GrossTxn,
round(avg(a.`Gross Transaction`),2) as AvgTxn, 
 round(sum(a.`Commission`),2) as TotalCommission,
 round(sum(a.`Gross Transaction`),2) as PerAgentGBV

 from
 (select * from alldata 
where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021' 
) a
group by a.`Tracking Code`) b
left join 
inter11_dormancy
on b.`Tracking Code` = inter11_dormancy.`Tracking Code`;

SELECT * FROM `per-agent-trial` WHERE `Tracking Code` = 'WI06487';


SELECT *
FROM tableName INTO OUTFILE '/Users/Downloads/per-agent-2.csv'
FIELDS ENCLOSED BY '"'
TERMINATED BY ','
ESCAPED BY '"' 
LINES TERMINATED BY 'n';

-- where is WI01944 ? inter_totals  or alldata 2021 p? 
select distinct `Payment With` from alldata where `Tracking Code` ='WI06968';

-- totals 
create table inter_totals as
select `Tracking Code` , count(`Tracking Code`) as total 
from `dex-initial-load-aws`.`alldata`
where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021' --  and `Tracking Code` ='WI01944'
 group by `Tracking Code`;
 
select * from alldata where -- year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021' and 
`Tracking Code` = 'WI01944';

select * from inter_totals;
-- inter_totals
select * from (
select `Tracking Code`, count(`Tracking Code`), 
year(str_to_date(`Transaction Date`,'%d/%m/%Y'))  as total 
from `dex-initial-load-aws`.`alldata`
where `Tracking Code`='WI01944'
and year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021' -- and `Tracking Code` ='WI01944'
 group by `Tracking Code`) a where `Tracking Code` ='WI01944';

select * -- count(`Tracking Code`) 
from `alldata` 
where year(str_to_date(`Transaction Date`,'%Y'))='2021'
and `Tracking Code` = 'WI01944' -- and `Tracking Code` = 'WI06900'
group by `Tracking Code`-- , `Payment With`
-- having count(*) > 5
order by `Tracking Code` desc;

SET SQL_SAFE_UPDATES = 0;

UPDATE `per-agent-trial`
SET `BookingYear` = str_to_date( `BookingYear`, '%Y' );

select `BookingYear`, str_to_date( `BookingYear`, '%Y' ) from `per-agent-trial` limit 100;

SHOW VARIABLES LIKE 'tmpdir';
show global variables like '%file_path%';
select table_schema, sum((data_length+index_length)/1024/1024) AS MB 
from information_schema.tables group by 1;

-- poc for having 4 rows didnt work in PBI, as its showing sum of average with 4 rows in 2021, 20, 19, 18
-- we should have single row for each agency

-- just to know count of 2021 rows in table, can be deleted remove later.
select count(distinct `Tracking Code` )
from `dex-initial-load-aws`.`alldata` 
where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021';

-- yoy GBV with year, 
select `Tracking Code`, round(sum(`Gross Transaction`),2) as YearGBV, 
year(str_to_date(`Transaction Date`,'%d/%m/%Y')) as BookingYear
FROM `dex-initial-load-aws`.`alldata` 
where `Tracking Code` = 'WI03415'
group by `Tracking Code`, year(str_to_date(`Transaction Date`,'%d/%m/%Y')) ;

-- mom performance query, when months are not present, check how power BI responds? 
-- show null for no months but left join not working :(
select * 
from 
(select -- mom perf
`Tracking Code`, 
round(sum(`Gross Transaction`),2) as MonthGBV, 
MONTHNAME(str_to_date(`Transaction Date`,'%d/%m/%Y')) as BookingMonth
FROM `dex-initial-load-aws`.`alldata` 
where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021' -- and `Tracking Code` = 'WI03415'
group by `Tracking Code`, month(str_to_date(`Transaction Date`,'%d/%m/%Y'))) b 
left join
(select 'January' as  monthList union select 'February' union select  'March' union -- list of months
select 'April' union 
select 'May' union select 'June' union select 'July' union select 'August' union select 'September' 
union select 'October' union select 'November' union select 'December') a 
on a.`monthList` = b.`BookingMonth` ;


select * from 
(select 'January' as  monthList union select 'February' union select  'March' union select 'April' union 
select 'May' union select 'June' union select 'July' union select 'August' union select 'September' 
union select 'October' union select 'November' union select 'December') a 
right join 
(select 'January' as  monthList union select 'February' union select  'March' union select 'April' union 
select 'May' union select 'June' union select 'July' union select 'August' union select 'September' ) b
on a.monthList = b.monthList;


-- left join testing query , can be deleted later
select * from 
(select * from `dex-initial-load-aws`.`alldata` 
where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = 2021 and `Tracking Code` = 'WI03415') a
left join 
(select `Tracking Code`, round(sum(`Gross Transaction`),2) as YearGBV, 
year(str_to_date(`Transaction Date`,'%d/%m/%Y')) as BookingYear
FROM `dex-initial-load-aws`.`alldata` 
where `Tracking Code` = 'WI03415'
group by `Tracking Code`, year(str_to_date(`Transaction Date`,'%d/%m/%Y')) ) b
on a.`Tracking Code` = b.`Tracking Code`
group by a.`Tracking Code`, b.`BookingYear`;

select count(distinct `Tracking Code`) from `dex-initial-load-aws`.`alldata` group by `Tracking Code`;

SELECT `Transaction Date`,`Gross Transaction` , 
RANK() OVER ( partition by `Tracking Code` ORDER BY str_to_date(`Transaction Date`,'%d/%m/%Y') DESC ) sales_rank
FROM `dex-initial-load-aws`.`alldata` where `Tracking Code` = 'WI03415' and sales_rank = 1;

select count(distinct `Tracking Code`) from `dex-initial-load-aws`.`alldata`; 

select * FROM `dex-initial-load-aws`.`alldata` where `Notes` != '' limit 10;

select (count(a.`Tracking Code`)/totals.total * 100)
FROM `dex-initial-load-aws`.`alldata` a  ,
 (select `Tracking Code`, count(`Tracking Code`) as total from `dex-initial-load-aws`.`alldata` group by `Tracking Code`) as totals
where a.`Travel Agency` = "Kore Voyages LLP" and 
a.`Transaction Status` = 'Canceled' and a.`Tracking Code` = totals.`Tracking Code`
group by a.`Tracking Code`;

-- create table `dex-initial-load`.lastBookingDate
select `Tracking Code`, max(STR_TO_DATE(`Transaction Date`, '%d/%m/%Y')) as lastBookingDate, `Gross Transaction`
from `dex-initial-load-aws`.`alldata`
group by `Tracking Code`;

select * --  a.`Tracking Code`, a.`Gross Transaction` , b.`maxTxnDate`, a.`Transaction Date`
from `dex-initial-load-aws`.`alldata` a join
(select `Tracking Code`,date_format(max(STR_TO_DATE(`Transaction Date`, '%d/%m/%Y')),'%d/%m/%Y') as maxTxnDate 
from `dex-initial-load-aws`.`alldata`
group by `Tracking Code`) b
on b.`Tracking Code` =  a.`Tracking Code` and a.`Transaction Date` = b.maxTxnDate and a.`Tracking Code` = 'WI03415';

select count(*) from 
`dex-initial-load-aws`.`alldata` a
join `dex-initial-load-aws`.`alldata` b
on a.`Tracking Code` = b.`Tracking Code`
where a.`Tracking Code` = 'WI03415';


SELECT *, RANK() OVER ( partition by `Tracking Code`
					ORDER BY str_to_date(`Transaction Date`,'%d/%m/%Y') DESC ) sales_rank
FROM `dex-initial-load-aws`.`alldata` where `Tracking Code` = 'WI03415';

select count(*) from `dex-initial-load-aws`.`alldata` where `Tracking Code` = 'WI03415';
-- Agency name, TXN,GBV, ABV,Commission, Tier, PR bookings,Cancellation %,
-- x-> YOY & MOM GBV graph,
-- x-> YOY Prepaid and postpaid booking, 
-- x-> Last booking date with GBV, 
-- x-> Address
-- x-> Last contacted date

-- dormant agent
-- who didnt book in the last 90 days from today -> list of tracking codes where today-txdate > 90
-- not a valid query. dont use it for analytics
select sum(cmsnPrvYear) from inter10_momperf where `Tracking Code` = 'WI00005';
create table inter11_dormancy as
select inter10_momperf.*, f.`30DormancyPeriod` ,
f.`60DormancyPeriod`, f.`90DormancyPeriod`
 from 
inter10_momperf
left join
(
select * from 
(select a.`Tracking Code`, `30DormancyPeriod` ,
`60DormancyPeriod`, `90DormancyPeriod`
from 
(select `Tracking Code` from `dex-initial-load-aws`.`alldata` 
 where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021') a
 left join
(select a.`Tracking Code` as DormantAgent, datediff( curdate(), max(str_to_date(a.`Transaction Date`,'%d/%m/%Y'))) as `30DormancyPeriod`
from (select * from 
`dex-initial-load-aws`.`alldata` 
 where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021') a
group by `Tracking Code`
having datediff( curdate(), max(str_to_date(`Transaction Date`,'%d/%m/%Y'))) > 30
order by `30DormancyPeriod` asc) d
 on 
 a.`Tracking Code` = d.`DormantAgent`
left join 
(select a.`Tracking Code` as DormantAgent, datediff( curdate(), max(str_to_date(a.`Transaction Date`,'%d/%m/%Y'))) as `60DormancyPeriod`
from (select * from 
`dex-initial-load-aws`.`alldata` 
 where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021') a
group by `Tracking Code`
having datediff( curdate(), max(str_to_date(`Transaction Date`,'%d/%m/%Y'))) > 60
order by `60DormancyPeriod` asc) b 
on a.`Tracking Code` = b.`DormantAgent`
left join 
(select a.`Tracking Code` as DormantAgent, datediff( curdate(), max(str_to_date(a.`Transaction Date`,'%d/%m/%Y'))) as `90DormancyPeriod`
from (select * from 
`dex-initial-load-aws`.`alldata` 
 where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021') a
group by `Tracking Code`
having datediff( curdate(), max(str_to_date(`Transaction Date`,'%d/%m/%Y'))) > 90
order by `90DormancyPeriod` asc) c
on 
a.`Tracking Code` = c.`DormantAgent`) e
where `30DormancyPeriod` is not null or 
`60DormancyPeriod` is not null or `90DormancyPeriod` is not null )f 
on f.`Tracking Code` = inter10_momperf.`Tracking Code`
group by  `Tracking Code`, `30DormancyPeriod` ,
`60DormancyPeriod`, `90DormancyPeriod`;

select * from inter11_nojoin_dormancy where `Tracking Code` = 'WI00005';
-- group by required for this table  

-- revenue per week 
select * from inter10_momperf where `Tracking Code` = 'WI00005';

-- report-2 
-- Wow negative 5 agents are red lets call them now 
-- green=positive [white= stagnant up and down 5%] and red=negative
select * 
from alldata 
where year(str_to_date(`Transaction Date`,'%d/%m/%Y'))='2021'


-- dormancy 
select * from inter11_nojoin_dormancy where `Tracking Code` = 'WI00005';
create table inter11_nojoin_dormancy as
select * from 
(select a.`Tracking Code`, `30DormancyPeriod` ,
`60DormancyPeriod`, `90DormancyPeriod`
from 
(select `Tracking Code` from `dex-initial-load-aws`.`alldata` 
 where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021') a
 left join
(select a.`Tracking Code` as DormantAgent, datediff( curdate(), max(str_to_date(a.`Transaction Date`,'%d/%m/%Y'))) as `30DormancyPeriod`
from (select * from 
`dex-initial-load-aws`.`alldata` 
 where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021') a
group by `Tracking Code`
having datediff( curdate(), max(str_to_date(`Transaction Date`,'%d/%m/%Y'))) > 30
order by `30DormancyPeriod` asc) d
 on 
 a.`Tracking Code` = d.`DormantAgent`
left join 
(select a.`Tracking Code` as DormantAgent, datediff( curdate(), max(str_to_date(a.`Transaction Date`,'%d/%m/%Y'))) as `60DormancyPeriod`
from (select * from 
`dex-initial-load-aws`.`alldata` 
 where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021') a
group by `Tracking Code`
having datediff( curdate(), max(str_to_date(`Transaction Date`,'%d/%m/%Y'))) > 60
order by `60DormancyPeriod` asc) b 
on a.`Tracking Code` = b.`DormantAgent`
left join 
(select a.`Tracking Code` as DormantAgent, datediff( curdate(), max(str_to_date(a.`Transaction Date`,'%d/%m/%Y'))) as `90DormancyPeriod`
from (select * from 
`dex-initial-load-aws`.`alldata` 
 where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021') a
group by `Tracking Code`
having datediff( curdate(), max(str_to_date(`Transaction Date`,'%d/%m/%Y'))) > 90
order by `90DormancyPeriod` asc) c
on 
a.`Tracking Code` = c.`DormantAgent`) e
where `30DormancyPeriod` is not null or 
`60DormancyPeriod` is not null or `90DormancyPeriod` is not null
group by `Tracking Code`, `30DormancyPeriod` ,
`60DormancyPeriod`, `90DormancyPeriod`;

-- region wise GBV, join tracking code with -> lost this query when computer restarted
select * from `dex-initial-load-aws`.`alldata` limit 10;

-- active agents week wise, agents who booked each week , for last 5 weeks , current week - 5 week number 
-- as contains in mysql , get their tracking code and names
-- need a table that contains revenue aggregated by weeks 
-- get week number in sub query 
-- week1, gbv, 
select a.`Tracking Code`, b.`TxnWeek`
from `dex-initial-load-aws`.`alldata` a
join (SELECT `Tracking Code`, week(str_to_date(`Transaction Date`,'%d/%m/%Y')) as `TxnWeek`, 
`Transaction Date`
 FROM `dex-initial-load-aws`.`alldata` ) b
 on a.`Tracking Code` = b.`Tracking Code`
 group by b.TxnWeek;
 
 -- active agents per week 
 select a.`Tracking Code`, a.`Transaction Date`,b.`Transaction Date`, b.`weekTxn` from 
 (select *
 from `dex-initial-load-aws`.`alldata` 
 where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021') a
 join
 (select `Tracking Code`, 
 `Transaction Date`,
 week(str_to_date(`Transaction Date`,'%d/%m/%Y')) as weekTxn
 from `dex-initial-load-aws`.`alldata` 
 where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021') b
 on a.`Tracking Code` = b.`Tracking Code` and a.`Transaction Date` = b.`Transaction Date`
 group by b.`weekTxn`, b.`Tracking Code`, a.`Tracking Code`
 order by b.`weekTxn`;
 
 -- region wise WOW GBV 52.53 region -> in email , some has cities and some has east 
 select c.`Cities`,sum(c.`sumGT`) as finalSum, c.`weekTxn`, c.`Tracking Code` from 
 (select a.`Tracking Code`, week(str_to_date(a.`Transaction Date`,'%d/%m/%Y')) as weekTxn
 , b.`Cities`, -- , b.`weekTxn` ,
 sum(a.`Gross Transaction`) as sumGT
 from 
 (select *
 from `dex-initial-load-aws`.`alldata` 
 where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021') a
 join
 (select distinct `Tracking Code`, `Cities`
 from `dex-initial-load-aws`.`ta-region-city-tier` ) b
 on a.`Tracking Code` = b.`Tracking Code`
 group by b.`Cities`, b.`Tracking Code`, a.`Tracking Code`) c
 group by `weekTxn`, `Cities`
 order by `weekTxn` desc;
 
 -- tier wise yoy gbv growth 50,51
 select c.`Tier`,c.`weekTxn`, sum(c.`sumT`) from 
 (select a.`Tier`, b.`weekTxn`,sum(a.`Gross Transaction`) as sumT from 
 (select *
 from `dex-initial-load-aws`.`alldata` 
 where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021') a
 join
 (select `Tracking Code`,`Transaction Date`, week(str_to_date(`Transaction Date`,'%d/%m/%Y')) as weekTxn
 from `dex-initial-load-aws`.`alldata` 
 where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021') b
 on b.`Tracking Code` = a.`Tracking Code` and b.`Transaction Date` = a.`Transaction Date`
 group by b.`Tracking Code`, a.`Tracking Code`, b.`Transaction Date`) c
 group by c.`Tier`, c.`weekTxn`
 order by weekTxn desc;
 
 
 select * from `dex-initial-load-aws`.`ta-region-city-tier` limit 10;
 -- count of active agents come from the # of records of the above excel 
 select count(*) from
 (select a.`Tracking Code`, a.`Transaction Date`,b.`Transaction Date`, b.`weekTxn` from 
 (select *
 from `dex-initial-load-aws`.`alldata` 
 where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021') a
 join
 (select `Tracking Code`, 
 `Transaction Date`,
 week(str_to_date(`Transaction Date`,'%d/%m/%Y')) as weekTxn
 from `dex-initial-load-aws`.`alldata` 
 where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021') b
 on a.`Tracking Code` = b.`Tracking Code` and a.`Transaction Date` = b.`Transaction Date`
 group by b.`weekTxn`, b.`Tracking Code`, a.`Tracking Code`
 order by b.`weekTxn`) c;
 
 -- city tier wise GBV , wrong
 select round(sum(a.`Gross Transaction`),2) as sumGT, b.`Tier`, b.`Cities`   from 
 (select * 
 from `dex-initial-load-aws`.`alldata`  
 where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021') a
 join
(SELECT distinct `Tracking Code`, `Tier`, `Cities`
 FROM `dex-initial-load-aws`.`ta-region-city-tier`) b
 on b.`Tracking Code` = a.`Tracking Code`
 group by b.`Tier`, b.`Cities`;
 
 -- city tier wise GBV - correct 
  select c.`Tier` as CityTier  , b.`Tier` as AgentTier, round(sum(a.`Gross Transaction`),2) as GBV
  from 
 (select * 
 from `dex-initial-load-aws`.`alldata`  
 where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021') a
 join
(SELECT distinct `Tracking Code`, `Tier`, `Cities`
 FROM `dex-initial-load-aws`.`ta-region-city-tier`) b
 on b.`Tracking Code` = a.`Tracking Code`
 join 
 `dex-initial-load-aws`.`indian-city-tier-list` c
 on trim(trailing '\r' from b.`Cities`) = c.`City`
 group by b.`Tier`, c.`Tier`
 order by c.`Tier`, b.`Tier`;
 
 
 SELECT * -- b.`Tracking Code`, b.`Tier`, b.`Cities`, c.`Tier`, c.`City`
 FROM `dex-initial-load-aws`.`ta-region-city-tier` 
where trim(trailing '\r' from `Cities`)  = 'Mumbai';

SELECT * -- b.`Tracking Code`, b.`Tier`, b.`Cities`, c.`Tier`, c.`City`
 FROM -- `dex-initial-load-aws`.`ta-region-city-tier` 
 `dex-initial-load-aws`.`indian-city-tier-list` c;
 -- on b.`Cities` = c.`City`;
 
 SELECT * FROM `dex-initial-load-aws`.`ta-region-city-tier` WHERE `Cities` REGEXP "\r\n";

 
 select * FROM `dex-initial-load-aws`.`ta-region-city-tier` 
 from `dex-initial-load-aws`.`indian-city-tier-list` on 


-- frequency = txn/reach

-- transaction = num of booking+number of cancellation

-- Achieved Activation, 
-- First time booked = new activation =  no business in the last 5 years. 
-- registered date, first booking date, 
select * from (
select *,
rank() over(partition by `Tracking Code` order by `Transaction Date` ) as rankDate
 from 
(select date_format(b.`Registration Date`,'%d/%m/%Y')  as `Reg Date`, 
a.`Transaction Date`, a.`Tracking Code`
from
(select `Tracking Code`, `Transaction Date`
from `dex-initial-load-aws`.`alldata`
where year(str_to_date(`Transaction Date`,'%d/%m/%Y')) = '2021') a
join 
(select distinct *
FROM `dex-initial-load-aws`.`ta-region-city-tier`
where year(str_to_date(`Registration Date`,'%Y/%m/%d')) = '2021' ) b
on a.`Tracking Code` = b.`Tracking Code`
group by a.`Transaction Date`, a.`Tracking Code`) c) d
where rankDate = 1;

-- agency by site -> TA , tier, city, name , address -> per agent
select * from `dex-initial-load-aws`.`ta-region-city-tier`;

CREATE TABLE `dex-initial-load-aws`.`indian-city-tier-list` (
  `Country` text,
  `State` text,
  `City` text,
  `Tier` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
