LOAD DATA LOCAL INFILE '/Users/bilickiv/developer/stunner-scripts/data/dbip-city.csv'  IGNORE INTO TABLE `dbip_lookup` FIELDS TERMINATED BY ','


delete from `dbip_lookup`

drop table `dbip_lookup`

CREATE TABLE `dbip_lookup` (
  `ip_start` varbinary(16) NOT NULL,
  `ip_end` varbinary(16) NOT NULL,
  `country` char(2) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `stateprov` varchar(80) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `city` varchar(80) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  PRIMARY KEY (`ip_start`)
);