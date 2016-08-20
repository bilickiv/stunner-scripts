use test;
#select d.*, l.`country`, l.`stateprov`, l.`city` from  DATA  d  LEFT JOIN dbip_lookup l on d.pubIP BETWEEN l.ipStart AND l.ipEnd
select d.* from  DATA d