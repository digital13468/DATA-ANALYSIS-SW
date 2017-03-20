%declare input ''
%declare output ''




REGISTER MovingAverage.jar
data = LOAD '$input' Using PigStorage(',');

-- Ticker, Date, Opening Price
tdp = FOREACH data GENERATE $0, (int)$1, (double)$2;

-- Group by ticker
group_ticker = GROUP tdp BY $0;

-- Remove redundant ticker field
t_d_p = FOREACH group_ticker {date_price = FOREACH tdp GENERATE $1, $2; GENERATE group, date_price;};

-- Apply the UDF to compute the average given the information in bag
average = FOREACH t_d_p GENERATE group, CustomMovingAverage($1);

-- Remove the bag level
moving_average = FOREACH average GENERATE $0, FLATTEN($1);

-- Combine ticker and date as ID
average_ID = FOREACH moving_average GENERATE *, CONCAT((chararray)$0, (chararray)$1);

-- Focus on dates between 20131001 and 20131031
select = FILTER tdp BY $1 >= 20131001 AND $1 <= 20131031;

-- Combine ticker and date as ID
select_ID = FOREACH select GENERATE *, CONCAT((chararray)$0, (chararray)$1);

-- Join average relation and input data relation
by_id = JOIN select_ID BY $3, average_ID BY $3;

-- Remobe redundant fields
final = FOREACH by_id GENERATE $0, $1, $2, $6;
STORE final INTO '$output';
