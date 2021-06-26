connect system/123456@jdbc:oracle:thin://192.168.1.72:1521/xe
CREATE TYPE Struct_Test AS OBJECT(s VARCHAR(30), i NUMBER);
CREATE TABLE test(i NUMBER, obj Struct_Test);
Insert into test values(1, struct_test('hello',5));
select * from test;

create table timetest
(
    tme date,
    tmestp timestamp(3),
    tmestp_tz timestamp(3) with time zone,
    tmpstp_tzl timestamp(3) with local time zone
);
insert into timetest values(sysdate,sysdate,sysdate,sysdate);
select * from timetest;
SET DATETIME-TZ_FORMAT %Y-%m-%d %H:%M:%S %Z%z
select tmpstp_tzl from timetest;

connect mem;
create table aaa (id TIMESTAMP WITH TIME ZONE);
insert into aaa values(NOW());
select * from aaa;
