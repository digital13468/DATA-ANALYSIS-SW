%declare output ''
%declare input ''

-- Clean the output folder
rmf $output;

-- Extract each line as a record
US_demographic_data = LOAD '$input' USING PigStorage('\n');

-- Replace spaces with ',' as separator so that the values can be split easily
formated_US_demographic_data = FOREACH US_demographic_data GENERATE REPLACE($0, '[\\s]+', ',');

-- Split values by ','
values = FOREACH formated_US_demographic_data GENERATE FLATTEN(STRSPLIT($0, ','));

-- USPS and ALAND
state_area = FOREACH values GENERATE $0, $4;

-- To remove the first row that is the column name, assign row number
state_area_with_column_name = RANK state_area;

-- Remove the column-name row
state_area_wo_column_name = FILTER state_area_with_column_name BY $0 > 1;

-- Convert the ALAND values to double before SUM
formated_state_area = FOREACH state_area_wo_column_name GENERATE $1, (double)$2;

-- Create a group for each state
state_groups = GROUP formated_state_area BY $0;

-- Sum the total land area for each USPS
total_area = FOREACH state_groups GENERATE group, SUM(formated_state_area.$1);

-- Order by total land
ordered_total_area = ORDER total_area BY $1 DESC;

-- Top 10 states
top_10_total_area = LIMIT ordered_total_area 10;
STORE top_10_total_area INTO '$output';