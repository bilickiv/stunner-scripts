CREATE TABLE `DATAPART` (
  `hashId` text CHARACTER SET utf8 COLLATE utf8_general_ci,
  `publicIP` text CHARACTER SET utf8 COLLATE utf8_general_ci,
  `localIP` text CHARACTER SET utf8 COLLATE utf8_general_ci,
  `mdate` text CHARACTER SET utf8 COLLATE utf8_general_ci,
  `latitude` text CHARACTER SET utf8 COLLATE utf8_general_ci,
  `longitude` text CHARACTER SET utf8 COLLATE utf8_general_ci,
  `discoveryResultCode` text CHARACTER SET utf8 COLLATE utf8_general_ci,
  `connectionMode` text CHARACTER SET utf8 COLLATE utf8_general_ci,
  `bandwidth` text CHARACTER SET utf8 COLLATE utf8_general_ci,
  `ssid` text CHARACTER SET utf8 COLLATE utf8_general_ci,
  `macAddress` text CHARACTER SET utf8 COLLATE utf8_general_ci,
  `rssi` text CHARACTER SET utf8 COLLATE utf8_general_ci,
  `carrier` text CHARACTER SET utf8 COLLATE utf8_general_ci,
  `simCountryIso` text CHARACTER SET utf8 COLLATE utf8_general_ci,
  `networkType` text CHARACTER SET utf8 COLLATE utf8_general_ci,
  `roaming` text CHARACTER SET utf8 COLLATE utf8_general_ci,
  `timeZone` text CHARACTER SET utf8 COLLATE utf8_general_ci
  /*!90618 , SHARD KEY () */ 
);