%declare input ''
%declare output1 ''
%declare output2 ''


REGISTER ratio.jar
data = LOAD '$input' Using PigStorage(',');

-- Ticker, Date, Opening Price
tdp = FOREACH data GENERATE $0, (int)$1, (double)$2;

-- Jan 1st, 1990 - Jan 3rd, 2000
period1 = FILTER tdp BY $1 >= 19900101 AND $1 <= 20000103;

-- Jan 2nd, 2005 - Jan 31st, 2014
period2 = FILTER tdp BY $1 >= 20050102 AND $1 <= 20140131;

-- Group by Ticker
group1_ticker = GROUP period1 BY $0;
group2_ticker = GROUP period2 BY $0;

-- Ignore aompanies that doesn't exist for more than 1 day
multidays1 = FILTER group1_ticker BY COUNT(period1) > 1;
multidays2 = FILTER group2_ticker BY COUNT(period2) > 1;

-- Find the start data and end date based on the ordered dates; oldest date first for period_min, newest date first for period_max
start_end_1 = FOREACH multidays1 {period1_min = ORDER period1 BY $1; period1_max = ORDER period1 BY $1 DESC; period1_m = LIMIT period1_min 1; period1_M = LIMIT period1_max 1; GENERATE group, period1_m, period1_M, FLATTEN(period1_m.$2), FLATTEN(period1_M.$2);};
start_end_2 = FOREACH multidays2 {period2_min = ORDER period2 BY $1; period2_max = ORDER period2 BY $1 DESC; period2_m = LIMIT period2_min 1; period2_M = LIMIT period2_max 1; GENERATE group, period2_m, period2_M, FLATTEN(period2_m.$2), FLATTEN(period2_M.$2);};

-- Calculate the growth factor; ending price divided by starting price
growth1 = FOREACH start_end_1 GENERATE $0, $1, $2, Ratio($3, $4);
growth2 = FOREACH start_end_2 GENERATE $0, $1, $2, Ratio($3, $4);

-- Find the companies with highest stock growth
highest1 = ORDER growth1 BY $3 DESC;
highest2 = ORDER growth2 BY $3 DESC;
STORE highest1 INTO '$output1';
STORE highest2 INTO '$output2';