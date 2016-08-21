#
# 0:-1-2,1 
#	ERROR(-1, R.string.error),
#	/** Unknown result. */
#	UNKNOWN(-2, R.string.unknown);
#	FIREWALL_BLOCKS(1, R.string.firewall_blocks),
#	/** Symmetric firewall is present. */
update DATA
set `aggDiscResult` = '0'
WHERE discoveryResultCode = '1' OR discoveryResultCode = '-1' OR discoveryResultCode = '-2';

update DATA
set `aggDiscResult` = '0'
WHERE discoveryResultCode = '0' AND `connectionMode` = '-1';

#10:2
#	/** Symmetric firewall is present. */
#	SYMMETRIC_FIREWALL(2, R.string.symmetric_firewall),update DATA
update DATA
set `aggDiscResult` = '10'
WHERE discoveryResultCode = '2';

#20:6
#	/** Symmetric cone NAT handles connections. */
#	SYMMETRIC_CONE(6, R.string.symmetric),
update DATA
set `aggDiscResult` = '20'
WHERE discoveryResultCode = '6';

#	40:5
#		/** Port restricted cone NAT handles connections. */
#		PORT_RESTRICTED_CONE(5, R.string.port_restricted),
update DATA
set `aggDiscResult` = '40'
WHERE discoveryResultCode = '5';

#	50:4
#		/** Restricted cone NAT handles connections. */
#		RESTRICTED_CONE(4, R.string.restricted),
update DATA
set `aggDiscResult` = '50'
WHERE discoveryResultCode = '4';

#80:3
#	/** Full cone NAT handles connections. */
#	FULL_CONE(3, R.string.full),
update DATA
set `aggDiscResult` = '80'
WHERE discoveryResultCode = '3';


#	100:0
#		/** No NAT is present, open access to the internet. */
#		OPEN_ACCESS(0, R.string.open_access),
update DATA
set `aggDiscResult` = '100'
WHERE discoveryResultCode = '0';