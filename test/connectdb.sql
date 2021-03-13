-- 测试数据库连接
connect mem
connect "工具人1号"/123456@jdbc:oracle:thin://192.168.1.72:1521:xe
connect "U_DBlink@!x$%#(*特朗普"/123456@jdbc:oracle:thin://192.168.1.72:1521:xe
connect testdblink/testdblink@jdbc:teradata://192.168.1.136/testbase
connect system/123456@jdbc:oracle:thin://192.168.1.72:1521/xe
connect admin/123456@jdbc:linkoopdb:cluster://node61:9105|node62:9105|node63:9105/ldb
connect admin/123456@jdbc:linkoopdb:tcp://192.168.1.84:9105/ldb
connect admin/123456@jdbc:linkoopdb:tcp://192.168.1.154:9105/ldb
connect LINKOOPDB/123456789@jdbc:dm://192.168.1.138:5236/DAMENG
connect testuser/testuser@jdbc:oracle:thin://192.168.1.80:1521/ldb
connect test/123456@jdbc:mysql:tcp://192.168.1.72:3306/mysql
connect postgres/123456@jdbc:postgresql:tcp://192.168.1.72:5432/test
connect postgres/123456@jdbc:postgresql:tcp://192.168.1.98:5432/nyc
connect sa/LinkoopDB123@jdbc:sqlserver:tcp://192.168.1.72:1433/dev
connect default/""@jdbc:clickhouse:tcp://192.168.1.81:8123/default
connect testdblink/testdblink@jdbc:teradata://192.168.1.136/testbase
connect hive/hive@jdbc:hive2://192.168.1.151:10000/default


select '${LastSQLResult(.rows)}' from dual;
select '${LastSQLResult(.result.100.0)}' from dual;
select '${LastSQLResult(.result.fadfdsa[0])}' from dual;


set debug on

__internal__ test set  CompareEnableMask  True;
__internal__ test set  CompareMask  .*aa.*;
__internal__ test set  CompareMask  .*bb.*;
__internal__ test set  CompareReportDetailMode  true;
__internal__ test set  CompareWorkEncoding gbk;
__internal__ test set  CompareRefEncoding gbk;
__internal__ test set  GenerateReport true;
__internal__ test Compare aa.txt bb.txt

