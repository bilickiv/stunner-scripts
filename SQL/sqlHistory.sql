LOAD DATA LOCAL INFILE '/Users/bilickiv/developer/stunner-scripts/data/dbip-city.csv'  IGNORE INTO TABLE `dbip_lookup` FIELDS TERMINATED BY ','


delete from `dbip_lookup`

drop table `dbip_lookup`

CREATE TABLE `dbip_lookup` (
  `ip_start`  text CHARACTER SET utf8 COLLATE utf8_general_ci,
  `ip_end`  text CHARACTER SET utf8 COLLATE utf8_general_ci,
  `country` char(2) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `stateprov` varchar(80) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `city` varchar(80) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  PRIMARY KEY (`ip_start`)
);

alter table `dbip_lookup` add ipEnd INTEGER
alter table DATA add pubIP INTEGER

update `dbip_lookup` set `ipStart` = INET_ATON(REPLACE(ip_start,'"','')), `ipEnd` = INET_ATON(REPLACE(ip_end,'"',''))
update `DATA` set `pubIP` = INET_ATON(REPLACE(publicIP,'"',''))


select * from (select * from DATA where pubIP is not null limit 10) d JOIN dbip_lookup l on d.pubIP BETWEEN l.ipStart AND l.ipEnd 
SELECT `hashId`, CONCAT(EXTRACT(YEAR_MONTH from CAST(mdate as DATETIME)),EXTRACT(DAY from CAST(mdate as DATETIME))), `publicIP`, localIP,  count(*) from DATA group by hashId, CONCAT(EXTRACT(YEAR_MONTH from CAST(mdate as DATETIME)),EXTRACT(DAY from CAST(mdate as DATETIME))), publicIP order by hashId, CONCAT(EXTRACT(YEAR_MONTH from CAST(mdate as DATETIME)),EXTRACT(DAY from CAST(mdate as DATETIME))), publicIP

SELECT DISTINCT localIP from DATA where  IS_IPV4(localIP) and (left(localIP,3) not like "10.") and (left(localIP,7) not like '192.168') and localIP not in (SELECT DISTINCT ipAddress from IP2ISPDATA) order by localIP