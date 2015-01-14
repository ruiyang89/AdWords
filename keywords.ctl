LOAD DATA
INFILE Keywords.dat
INTO TABLE Keywords
FIELDS TERMINATED BY '	'
(advertiserid,keyword,bid)