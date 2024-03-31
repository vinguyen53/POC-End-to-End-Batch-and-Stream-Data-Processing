load data local infile 'C:/Users/ntanvi/Downloads/data-test.csv'
into table de_db.people
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows (id, user_id, first_name, last_name, sex, phone, date_of_birth, job_title)