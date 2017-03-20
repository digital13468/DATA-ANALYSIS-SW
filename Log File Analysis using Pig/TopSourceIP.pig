%declare output ''
%declare input ''

network_trace = LOAD '$input' Using PigStorage(' ');

-- only interested in communcation that used the TCP protocol
TCP_network_trace = FILTER network_trace BY $5 MATCHES 'tcp';

-- process source and destination IP addresses and get rid of the extra information including and after te fourth "."
source_destination = FOREACH TCP_network_trace GENERATE SUBSTRING($2,0, LAST_INDEX_OF($2,'.')),SUBSTRING($4,0, LAST_INDEX_OF($4,'.'));

-- create a group for each source IP
group_source_destination = GROUP source_destination BY $0;

-- count the distinct destination IPs in each group
destination_count = FOREACH group_source_destination {distinct_destination = DISTINCT source_destination.$1; GENERATE group, COUNT(distinct_destination);};

-- order the source IPs by the number of destinations
order_destination_count = ORDER destination_count BY $1 DESC;
top_10_source = LIMIT order_destination_count 10;
STORE top_10_source INTO '$output';