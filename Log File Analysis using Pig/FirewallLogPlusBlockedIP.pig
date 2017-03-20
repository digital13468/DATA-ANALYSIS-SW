%declare output_blocked ''
%declare output_firewall ''
%declare input_trace ''
%declare input_block ''

A = LOAD '$input_trace' USING PigStorage(' ');
B = LOAD '$input_block' USING PigStorage(' ');

-- take time, connection id, source ip and destination ip from trace
C = FOREACH A GENERATE $0, $1, $2, $4;

-- join by connection id
D = JOIN C BY $1, B BY $0;

-- firewall log contains time, connection id, source ip, destination ip and allowed/blocked
E = FOREACH D GENERATE $0, $1, $2, $3, $5;

-- only concern about blocked sources
F = FILTER E BY $4 MATCHES 'Blocked';
STORE F INTO '$output_firewall';

-- take out blocked source ip addresses
G = FOREACH F GENERATE $2, $4;

-- group by source ip
H = GROUP G BY $0;

-- count the number of times an ip was blocked
I = FOREACH H GENERATE group, COUNT(G);

J = ORDER I BY $1 DESC;
STORE J INTO '$output_blocked';