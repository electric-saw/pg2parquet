create user repl with replication password '123';


CREATE TABLE "users" (
  "id" bigserial PRIMARY KEY,
  "name" varchar,
  "pass" varchar,
  "desciption" text
);


CREATE TABLE "users_arr" (
  "id" bigserial PRIMARY KEY,
  "name" varchar,
  "pass" varchar[],
  "desciption" text
);

insert into users (name, pass, desciption) 
values 
    ('juca', '123', 'aaaaaaaa'),
    ('bala', '123', 'aaaaaaaa'),
    ('beto', '123', 'aaaaaaaa');

insert into users_arr (name, pass, desciption) 
values 
    ('juca', '{"123", "345"}', 'aaaaaaaa'),
    ('bala', '{"123", "345"}', 'aaaaaaaa'),
    ('beto', '{"123", "345"}', 'aaaaaaaa');