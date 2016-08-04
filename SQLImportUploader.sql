INSERT INTO RAWDATA (source_file,upload_date,hashId,platform,publicIP,localIP,mdate,`latitude`,`longitude`,`androidVersion`,
  `discoveryResultCode`,
  `connectionMode`,
  `technology` ,
  `present` ,
  `pluggedState`,
  `voltage`,
  `temperature`,
  `percentage`,
  `health`,
  `chargingState`,
  `bandwidth`,
  `ssid`,
  `macAddress`,
  `rssi`,
  `carrier`,
  `networkType`,
  `roaming`,
  `triggerCode`,
  `appVersion`,
  `timeZone`
) 
SELECT 'SQLImport',TMPDEVLIST.uploadTime, TMPDEVLIST.deviceHash, TMPRAWDATA.platform, 
TMPRAWDATA.publicIP,TMPRAWDATA.localIP,TMPRAWDATA.mdate,TMPRAWDATA.latitude,TMPRAWDATA.longitude,TMPRAWDATA.androidVersion,
  TMPRAWDATA.discoveryResultCode,
  TMPRAWDATA.connectionMode,
  TMPRAWDATA.technology,
  TMPRAWDATA.present,
  TMPRAWDATA.pluggedState,
  TMPRAWDATA.voltage,
  TMPRAWDATA.temperature,
  TMPRAWDATA.percentage,
  TMPRAWDATA.health,
  TMPRAWDATA.chargingState,
  TMPRAWDATA.bandwidth,
  TMPRAWDATA.ssid,
  TMPRAWDATA.macAddress,
  TMPRAWDATA.rssi,
  TMPRAWDATA.carrier,
  TMPRAWDATA.networkType,
  TMPRAWDATA.roaming,
  TMPRAWDATA.triggerCode,
  TMPRAWDATA.appVersion,
  TMPRAWDATA.timeZone FROM TMPRAWDATA INNER JOIN TMPDEVLIST on TMPRAWDATA.hashId = TMPDEVLIST.deviceHash