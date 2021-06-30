connect admin/123456@jdbc:linkoopdb:tcp://192.168.1.154:9105/ldb
--connect system/123456@jdbc:oracle:thin://192.168.1.72:1521/xe
drop table T_TYPE_CLOB;
create table T_TYPE_CLOB (a1 clob);
insert into T_TYPE_CLOB values ('abcdefg');
--SET LOB_LENGTH 5
select * from T_TYPE_CLOB;
SET LOB_LENGTH 5
select * from T_TYPE_CLOB;
SET LOB_LENGTH 20

drop table T_TYPE_BLOB;
create table T_TYPE_BLOB (a1 blob);
insert into T_TYPE_BLOB values ('abcdefg');
SET LOB_LENGTH 5
select * from T_TYPE_BLOB;
SET LOB_LENGTH 20
select * from T_TYPE_BLOB;
