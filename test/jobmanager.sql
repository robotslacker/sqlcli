set jobmanager on
set
__internal__ job create mytest loop 2 parallel 2 script localtest\cc.sql tag group1;
__internal__ job create mytest2 loop 2 parallel 2 script localtest\cc.sql tag group1;
__internal__ job create mytest3 loop 2 parallel 2 script localtest\cc.sql tag group1;
__internal__ job start mytest;
--sleep 10
__internal__ job start mytest2;
sleep 10
__internal__ job start mytest3;
sleep 3
__internal__ job show all;
__internal__ job wait all;
exit