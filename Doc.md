# SQLCli 快速说明

SQLCli 是一个命令行的测试工具，设计目的主要是为了满足数据库方面的相关功能测试、压力测试。  
通过SQLCli，你可以执行通用的SQL数据库脚本，记录执行结果。  
也可以把SQLCli当作一个命令行的日常使用工具，来完成你数据库的相关维护工作。  

SQLCli是一个Python程序，其中大部分代码都是用Python完成的。  
可以通过jaydebeapi连接数据库的JDBC驱动，这个时候需要安装相关的包，Windows平台上可能还需要进行相关的编译工作。  
也可以通过ODBC标准连接数据库的ODBC驱动，但是这部分并没有经过严谨的测试。  


SQLCli 目前可以支持的数据库有：  
   * Oracle,MySQL,PostgreSQL,SQLServer,TeraData, Hive等通用数据库  
   * 达梦，神通， 金仓， 南大通用，LinkoopDB等国产数据库  
   * ClickHouse    
   * 其他符合标准JDBC规范的数据库  
***
### 谁需要用这个文档

需要通过SQL语句来查看、维护数据库的。  
需要进行各种数据库功能测试、压力测试的。  
需要用统一的方式来连接各种不同数据库的。    

***
### 安装
安装的前提有：
   * 有一个Python 3.6以上的环境
   * 能够连接到互联网上， 便于下载必要的包
   * 安装JDK8
   * 对于Windows平台，还需要提前安装微软的C++编译器（Jaydebeapi安装过程中需要动态编译jpype）  
   * 对于Linux平台，也需要提前安装gcc编译器（Jaydebeapi安装过程中需要动态编译jpype）  
     yum install -y gcc-c++ gcc python3-devel
   * 对于MAC平台，直接安装


依赖的第三方安装包：  
   * 这些安装包会在robotslacker-sqlcli安装的时候自动随带安装
   * jaydebeapi               : Python通过jaydebeapi来调用JDBC，从而执行SQL语句
   * setproctitle             : Python通过setproctitle来设置进程名称，从而在多进程并发时候给以帮助
   * click                    : Python的命令行参数处理
   * prompt_toolkit           : 用于提供包括提示符功能的控制台终端的显示样式
   * cli_helpers              : 用于提供将数据库返回结果进行表单格式化显示
   * hdfs                     : HDFS类库，支持对HDFS文件操作
   * confluent_kafka          : Kafka类库，支持对Kafka操作, 这个包和kafka-python冲突，需要提前移除kafka-python。 目前不支持WIndows
   * wget                     : JDBC驱动更新和下载  
   * pyodbc                   : 用于连接ODBC驱动

安装命令：
```
   pip install -U robotslacker-sqlcli
```
***
### 第一次使用
安装后直接在命令下执行sqlcli命令即可。  
如果你的$PYTHON_HOME/Scripts没有被添加到当前环境的$PATH中，你可能需要输入全路径名  
```
(base) >sqlcli
SQL*Cli Release 0.0.32
SQL> 
```
如果你这里看到了版本信息，那祝贺你，你的安装成功了
***
### 驱动程序的下载和配置
sqlcli是一个基于JDBC的数据库工具，能够操作数据库的前提当前环境下有对应的数据库连接jar包  
#### 驱动程序的配置
配置文件位于SQLCli的安装目录下的conf目录中，配置文件名为:sqlcli.conf  
配置例子:
```
[driver]
oracle=oracle_driver
mysql=mysql_driver
.... 

[oracle_driver]
filename=ojdbc8.jar
downloadurl=http://xxxxxx/driver/ojdbc8.jar
md5=1aa96cecf04433bc929fca57e417fd06
driver=oracle.jdbc.driver.OracleDriver
jdbcurl=jdbc:oracle:thin:@${host}:${port}/${service}

[mysql_driver]
filename=mysql-connector-java-8.0.20.jar
downloadurl=http://xxxxx/driver/mysql-connector-java-8.0.20.jar
md5=48d69b9a82cbe275af9e45cb80f6b15f
driver=com.mysql.cj.jdbc.Driver
jdbcurl=jdbc:mysql://${host}:${port}/${service}
jdbcprop=socket_timeout:360000000
odbcurl=DRIVER={driver_name};SERVER=${host};PORT=${port};DATABASE=${service};UID=${username};PWD=${password};
```

如果数据库要新增其他数据库的连接，则应仿效上述配置例子。  
其中：  
* 所有的数据库名称及配置项名称均应该出现在[driver]的配置项中  
  如果某一种数据库连接需要不止一个jar包，则这里应该配置多个配置项  
  例如：   mydb=mydb1_driver1, mydb1_driver2
* 数据库的具体配置应该在具体的配置项中  
  filename：      可选配置项，jar包具体的名字  
  driver:         可选配置项，数据库连接的主类  
  jdbcurl:        可选配置项，jdbc连接字符串，其中${host}${port}${service}分别表示数据库连接主机，端口，数据库名称  
  downloadurl：   可选配置项，若本地不存在该文件，则文件下载需要的路径  
  md5:            可选配置项，文件下载校验MD5    
  jdbcprop:       可选配置项，若该数据库连接需要相应的额外参数，则在此处配置
  odbcurl:        可选配置项，若该数据库连需要通过ODBC连接数据库，则在此处配置      
  jdbcurl和odbcurl两个参数，至少要配置一个。若配置jdbcurl，则jdbc对应的filename,driver也需要配置  
* 基于这个原则，最简化的运行配置应该为：  
```
[driver]
oracle=oracle_driver
mysql=mysql_driver

[oracle_driver]
filename=ojdbc8.jar
driver=oracle.jdbc.driver.OracleDriver
jdbcurl=jdbc:oracle:thin:@${host}:${port}/${service}

[mysql_driver]
filename=mysql-connector-java-8.0.20.jar
driver=com.mysql.cj.jdbc.Driver
jdbcurl=jdbc:mysql://${host}:${port}/${service}
```
#### 驱动程序的下载
基于上述参数文件的正确配置，可以使用--syncdriver的方式从服务器上来更新数据库连接需要的jar包  
例子：    
```
(base) C:\Work\linkoop\sqlcli>sqlcli --syncdriver
Checking driver [oracle] ...
File=[ojdbc8.jar], MD5=[1aa96cecf04433bc929fca57e417fd06]
Driver [oracle_driver] is up-to-date.
Checking driver [mysql] ...
File=[mysql-connector-java-8.0.20.jar], MD5=[48d69b9a82cbe275af9e45cb80f6b15f]
Driver [mysql_driver] is up-to-date.
```
#### 程序的命令行参数
```
(base) sqlcli --help
Usage: sqlcli [OPTIONS]

Options:
  --version       Output sqlcli's version.
  --logon TEXT    logon user name and password. user/pass
  --logfile TEXT  Log every query and its results to a file.
  --execute TEXT  Execute SQL script.
  --nologo        Execute with silent mode.
  --sqlperf TEXT  SQL performance Log.
  --syncdriver    Download jdbc jar from file server.
  --clientcharset TEXT  Set client charset. Default is UTF-8.
  --resultcharset TEXT  Set result charset. Default is same to clientcharset.
  --help          Show this message and exit.
```  
--version 用来显示当前工具的版本号
```
(base) sqlcli --version
Version: 0.0.32
```
--logon  用来输入连接数据的的用户名和口令
```
(base) sqlcli --logon user/pass
Version: 0.0.32
Driver loaded.
Database connected.
SQL>
如果用户、口令正确，且相关环境配置正常，你应该看到如上信息。

user/pass : 数据库连接的用户名和口令  

成功执行这个命令的前提是你已经在环境变量中设置了数据库连接的必要信息。  
这里的必要信息包括：
环境变量：  SQLCLI_CONNECTION_URL  
    参数格式是： jdbc:[数据库类型]:[数据库通讯协议]://[数据库主机地址]:[数据库端口号]/[数据库服务名]  
```
--logfile   用来记录本次命令行操作的所有过程信息  
这里的操作过程信息包含你随后在命令行里头的所有输出信息。  
如果你在随后的命令行里头设置了SET ECHO ON，那么记录里头还包括了你所有执行的SQL语句原信息  
文件输出的字符集将根据参数resultcharset中的设置来确定    
```
(base) sqlcli --logon user/pass --logfile test.log
Version: 0.0.32
Driver loaded.
Database connected.
set echo on
select * from test_tab;
+----+----------+
| ID | COL2     |
+----+----------+
| 1  | XYXYXYXY |
| 1  | XYXYXYXY |
+----+----------+
SQL> exit
Disconnected.

(base) type test.log
Driver loaded.
Database connected.
SQL> select * from test_tab;

+----+----------+
| ID | COL2     |
+----+----------+
| 1  | XYXYXYXY |
| 1  | XYXYXYXY |
+----+----------+
2 rows selected.
SQL> exit
Disconnected.
```
--execute 在SQLCli启动后执行特定的SQL脚本  
为了能够批处理一些SQL语句，我们通常将这批SQL语句保存在一个特定的文件中，这个文件的习惯后缀是.sql  
通过execute参数，可以让SQLCli来执行这个脚本，而不再需要我们一行一行的在控制台输入  
脚本字符集将根据参数clientcharset中的设置来确定
```
(base) type test.sql
select * from test_tab;

(base) sqlcli --logon user/pass --execute test.sql
Version: 0.0.32
Driver loaded.
Database connected.
set echo on
select * from test_tab;
+----+----------+
| ID | COL2     |
+----+----------+
| 1  | XYXYXYXY |
| 1  | XYXYXYXY |
+----+----------+
SQL> exit
Disconnected.
注意： 即使你的SQL脚本中不包含Exit语句，在sqlcli执行完当前脚本后，他也会自动执行exit语句
如果SQL脚本中不包含数据库驱动加载或者数据库连接语句，请在执行这个命令前设置好相应的环境变量信息
```
--nologo    这是一个选项，用来控制sqlcli是否会在连接的时候显示当前的程序版本
```
(base) sqlcli 
SQL*Cli Release 0.0.32
SQL>   
区别：
(base) sqlcli --nologo
SQL>
```
--sqlperf  输出SQL运行日志  
这里的运行日志不是指程序的logfile，而是用CSV文件记录的SQL日志，  
这些日志将作为后续对SQL运行行为的一种分析  
运行日志共包括如下信息：  
1、Script       运行的脚本名称
2、StartedTime  SQL运行开始时间，格式是：%Y-%m-%d %H:%M:%S  
3、elapsed      SQL运行的消耗时间，这里的单位是秒，精确两位小数    
4、SQL          运行的SQL    
5、SQLStatus    SQL运行结果，0表示运行正常结束，1表示运行错误  
6、ErrorMessage 错误日志，在SQLStatus为1的时候才有意义  
7、thread_name  工作线程名，对于主程序，这里显示的是MAIN  
说明：上述信息都有TAB分隔，其中字符信息用单引号包括，如下是一个例子：  
```
Script  Started elapsed SQLPrefix       SQLStatus       ErrorMessage    thread_name
'sub_1.sql' '2020-05-25 17:46:23'       0.00        'loaddriver localtest\linkoopdb-jdbc-2.3.'      0       ''      'MAIN'
'sub_1.sql' '2020-05-25 17:46:23'       0.28        'connect admin/123456'  0       ''      'MAIN'
'sub_1.sql' '2020-05-25 17:46:24'       0.00        'SET ECHO ON'   0       ''      'MAIN'
'sub_1.sql' '2020-05-25 17:46:24'       0.00        'SET TIMING ON' 0       ''      'MAIN'
'sub_1.sql' '2020-05-25 17:46:24'       0.01        'LOADSQLMAP stresstest' 0       ''      'MAIN'
'sub_1.sql' '2020-05-25 17:46:24'       0.92        'ANALYZE TRUNCATE STATISTICS'   0       ''      'MAIN'
'sub_1.sql' '2020-05-25 17:46:25'       0.02        'SELECT count(SESSION_ID)  FROM INFORMATI'      0       ''      'MAIN'
'sub_1.sql' '2020-05-25 17:46:25'       1.37        'drop user testuser if exists cascade'  0       ''      'MAIN'
'sub_1.sql' '2020-05-25 17:46:26'       0.54        'CREATE USER testuser PASSWORD '123456''        0       ''      'MAIN'
```
***
#### 在SQLCli里面查看当前支持的命令
```
(base) sqlcli 
SQL*Cli Release 0.0.32
SQL> help
+--------------+-----------------------------+
| Command      | Description                 |
+--------------+-----------------------------+
| __internal__ | execute internal command.   |
| abortjob     | Abort Jobs                  |
| connect      | Connect to database .       |
| disconnect   | Disconnect database .       |
| exit         | Exit program.               |
| help         | Show this help.             |
| loadsqlmap   | load SQL Mapping file .     |
| quit         | Quit.                       |
| set          | set options .               |
| showjob      | show informations           |
| shutdownjob  | Shutdown Jobs               |
| sleep        | Sleep some time (seconds)   |
| start        | Execute commands from file. |
| StartJob     | Start Jobs                  |
| Submitjob    | Submit Jobs                 |
| waitjob      | Wait Job complete           |
+--------------+-----------------------------+
这里显示的是所有除了标准SQL语句外，可以被执行的各种命令开头。
标准的SQL语句并没有在这里显示出来，你可以直接在控制行内或者脚本里头执行SQL脚本。
```

***
#### 连接数据库
在sqlcli命令行里头，可以通过connect命令来连接到具体的数据库
```
(base) sqlcli 
SQL*Cli Release 0.0.32
SQL> connect user/pass@jdbc:[数据库类型]:[数据库通讯协议]://[数据库主机地址]:[数据库端口号]/[数据库服务名] 
Database connected.
SQL> 
能够成功执行connect的前提是： 数据库驱动已经放置到jlib下，并且在conf中正确配置

如果已经在环境变量中指定了SQLCLI_CONNECTION_URL，连接可以简化为
(base) sqlcli 
SQL*Cli Release 0.0.32
SQL> connect user/pass
Database connected.
SQL> 

在数据库第一次连接后，第二次以及以后的连接可以不再输入连接字符串，程序会默认使用上一次已经使用过的连接字符串信息，比如：
(base) sqlcli 
SQL*Cli Release 0.0.32
SQL> connect user/pass@jdbc:[数据库类型]:[数据库通讯协议]://[数据库主机地址]:[数据库端口号]/[数据库服务名] 
Database connected.
SQL> connect user2/pass2
Database connected.
SQL> 

常见数据库的连接方式示例：
ORACLE:
    connect username/password@jdbc:oracle:tcp://IP:Port/Service_Name
MYSQL:
    connect username/password@jdbc:mysql:tcp://IP:Port/Service_Name
PostgreSQL：
    connect username/password@jdbc:postgresql:tcp://IP:Port/Service_Name
SQLServer：
    connect username/password@jdbc:sqlserver:tcp://IP:Port/DatabaseName
TeraData：
    connect username/password@jdbc:teradata:tcp://IP:0/DatabaseName
Hive:
    connect hive/hive@jdbc:hive2://IP:Port/DatabaseName
ClickHouse:
	connect default/""@jdbc:clickhouse:tcp://IP:Port/DatabaseName
LinkoopDB:
    connect username/password@jdbc:linkoopdb:tcp://IP:Port/Service_Name
```

#### 断开数据库连接
```
(base) sqlcli 
SQL*Cli Release 0.0.32
SQL> connect user/pass@jdbc:[数据库类型]:[数据库通讯协议]://[数据库主机地址]:[数据库端口号]/[数据库服务名] 
Database connected.
SQL> disconnect
Database disconnected.
```
***

#### 会话的切换和保存
```
(base) sqlcli 
SQL*Cli Release 0.0.32
SQL> connect user/pass@jdbc:[数据库类型]:[数据库通讯协议]://[数据库主机地址]:[数据库端口号]/[数据库服务名] 
Database connected.
SQL> session save sesssion1
Session saved.
# 这里会把当前会话信息保存到名字为session1的上下文中，session1为用户自定义的名字
# 注意：这里并不会断开程序的Session1连接，当Restore的时候也不会重新连接
SQL> connect user/pass@jdbc:[数据库类型]:[数据库通讯协议]://[数据库主机地址]:[数据库端口号]/[数据库服务名]
Database connected.
# 连接到第一个会话
SQL> session save sesssion2
Session saved.
# 这里会把当前会话信息保存到名字为session2的上下文中，session2为用户自定义的名字
# 注意：这里并不会断开程序的Session2连接，当Restore的时候也不会重新连接
SQL> session show
+---------------+-----------+-----------------------------------------------+
| Sesssion Name | User Name | URL                                           |
+---------------+-----------+-----------------------------------------------+
| session1      | xxxxx     | jdbc:xxxxx:xxx://xxx.xxx.xxx.xxx/xxxx         |
| session2      | yyyyy     | jdbc:yyyy:xxx://xxx.xxx.xxx.xxx/yyyyy         |         
+---------------+-----------+-----------------------------------------------+
# 显示当前保存的所有会话信息

SQL> session restore sesssion1
Session stored.
# 这里将恢复当前数据库连接为之前的会话1

SQL> session restore sesssion2
Session stored.
# 这里将恢复当前数据库连接为之前的会话2

SQL> session saveurl sesssion3
Session saved.
# 这里会把当前会话信息的URL保存到名字为session3的上下文中，session3为用户自定义的名字
# 注意：这里并不会保持程序的Session3连接，仅仅记录了URL信息，当Restore的时候程序会自动重新连接

SQL> session release sesssion3
Session released.
# 这里将释放之前保存的数据库连接，和针对具体一个连接的DisConnect类似
```
***

#### 让程序休息一会
```
(base) sqlcli 
SQL*Cli Release 0.0.32
SQL> sleep 10
SQL> disconnect
Database disconnected.
这里的10指的是10秒，通过这个命令可以让程序暂停10秒钟。
Sleep的做法主要用在一些定期循环反复脚本的执行上
```
#### 执行主机的操作命令
```
(base) sqlcli 
SQL*Cli Release 0.0.32
SQL> host date
2020年 10月 29日 星期四 11:24:34 CST
SQL> disconnect
Database disconnected.
这里的date是主机的命令，需要注意的是：在Windows和Linux上命令的不同，脚本可能因此无法跨平台执行
```
#### 回显指定的文件
```
(base) sqlcli 
SQL*Cli Release 0.0.32
SQL> echo subtest.sql
-- 这里是subtest.sql
connect admin/123456
select * from cat
SQL> echo off
这里从echo开始，到echo off结束的中间内容并不会被数据库执行，而且会记录在subtest.sql中
同样的操作，这里也可以用来生成一些简单的配置文件，简单的报告信息等
```

#### 加载数据库驱动
SQLCli会默认加载所有配置在conf/sqlcli.ini中的JDBC驱动
```
(base) sqlcli 
SQL*Cli Release 0.0.32
SQL> loaddriver;
没有任何参数的loaddriver命令将显示出所有系统已经加载的驱动程序
SQL> loaddriver [database_name]  [jdbc_jarfile]
将用参数中jdbc_jarfile指定的文件替换掉配置文件中的文件信息
```

#### 加载SQL重写配置文件
在sqlcli命令行里头，可以通过loadsqlmap命令来加载SQL重写配置文件
```
(base) sqlcli 
SQL*Cli Release 0.0.31
SQL> loadsqlmap map1
Mapping file loaded.
这里的map1表示一个重写配置文件，这里可以写多个配置文件的名字，比如loadsqlmap map1,map2,map3，多个文件名之间用逗号分隔

重写文件的查找顺序：
   1.  以map1为例子，如果map1是一个全路径或者基于当前目录的相对目录，则以全路径或相对目录为准。这时候参数应该带有后缀
   2：  如果依据全路径没有找到，则会尝试在脚本所在的目录进行相对目录的查找。这时候参数不能够带后缀，查找的后缀文件名是.map
   3：  如果依然没有找到，则会在SQLCLI_HOME\mapping下查找。这时候参数不能带后缀，查找的后缀文件名是.map

启用SQL重写的方法：
   1.   在命令行里面，通过sqlcli --sqlmap map1的方式来执行重写文件的位置
   2.   定义在系统环境变量SQLCLI_SQLMAPPING中指定。 如果定义了命令行参数，则系统环境变量不生效
   3.   通过在SQL输入窗口，输入loadsqlmap的方式来指定

重写文件的写法要求：
   以下是一个重写文件的典型格式， 1，2，3，4，5 是文件的行号，不是真实内容：
    1 #..*:                                      
    2 ((?i)CREATE TABLE .*\))=>\1 engine xxxxx
    3 WEBURL=>{ENV(ROOTURL)}
    4 MYID=>{RANDOM('random_ascii_letters_and_digits',10)}
    5 #.

    行1：
        这里定义的是参与匹配的文件名，以#.开头，以:结束，如果需要匹配全部文件，则可以在中间用.*来表示
    行2：
        这里定义的是一个正则替换规则, 在符合CREATE TABLE的语句后面增加engine xxxxx字样
    行3：
        这里定义的是一个正则替换规则, 用环境变量ROOTURL的值来替换文件中WEBURL字样
    行4：
        这里定义的是一个正则替换规则, 用一个最大长度位10，可能包含字母和数字的内容来替换文件中的MYID字样
    行5：
        文件定义终止符

    每一个MAP文件里，可以循环反复多个这样的类似配置，每一个配置段都会生效

重写SQL的日志显示：
    被重写后的SQL在日志中有明确的标志信息，比如：
        SQL> CREATE TABLE T_TEST(
           > id int,
           > name varchar(30),
           > salary int
           > );
        REWROTED SQL> Your SQL has been changed to:
        REWROTED    > CREATE TABLE T_TEST(
        REWROTED    > id int,
        REWROTED    > name varchar(30),
        REWROTED    > salary int
        REWROTED    > ) engine xxxx;
     这里第一段是SQL文件中的原信息，带有REWROTED的信息是被改写后的信息

```

#### 执行数据库SQL语句
在数据库连接成功后，我们就可以执行我们需要的SQL语句了，对于不同的SQL语句我们有不同的语法格式要求。  
* 对于SQL语句块的格式要求：  
  SQL语句块是指用来创建存储过程、SQL函数等较长的SQL语句  
  SQL语句块的判断依据是：
```
     CREATE | REPLACE ******   FUNCTION|PROCEDURE **** | DECLARE ****
     这里并没有完整描述，具体的信息可以从代码文件中查阅
```
&emsp; SQL语句块的结束符为【/】，且【/】必须出现在具体一行语句的开头  比如：
```
    SQL> CREATE PROCEDURE PROC_TEST()
       > BEGIN
       >     bulabulabula....;
       > END;
       > /
    SQL> 
```
&emsp; 对于SQL语句块，SQLCli将被等待语句结束符后把全部的SQL一起送给SQL引擎（不包括语句结束符）。

* 对于多行SQL语句的格式要求：  
   多行SQL语句是指不能在一行内写完，需要分成多行来写的SQL语句。  
   多行SQL语句的判断依据是： 语句用如下内容作为关键字开头
```
    CREATE | SELECT | UPDATE | DELETE | INSERT | __INTERNAL__ | DROP | REPLACE | ALTER

```
&emsp; 多行SQL结束符为分号【 ；】 比如：
```
    SQL> CREATE TABLE TEST_TAB
       > (
       >    ID   CHAR(20),
       >    COL1 CHAR(20)
       > );
    SQL> 
    对于多行SQL语句，同样也可以使用行首的【/】作为多行语句的结束符
```
&emsp; 对于SQL多行语句，SQLCli将被等待语句结束符后把全部的SQL一起送给SQL引擎（包括语句结束符）。

* 其他SQL语句  
  不符合上述条件的，即不是语句块也不是多行语句的，则在输入或者脚本回车换行后结束当前语句。  
  结束后的语句会被立刻送到SQL引擎中执行。

* 语句中的注释  
  注释信息分为行注释和段落注释，这些注释信息不会被送入到SQL引擎中，而是会被SQLCli直接忽略。  
  
  行注释的写法是： 【 ...SQL...   -- Comment 】  
  即单行中任何【--】标识符后的内容都被理解为行注释信息。  
  
  段落注释的写法是： 【 ...SQL...  /* Comment .... */ 】  
  即单行中任何【/*】标识符和【*/】标识符中的内容都被理解为行注释信息。  
  比如：
```
    SQL> CREATE TABLE TEST_TAB
       > (
       >    ID   CHAR(20),          -- ID信息，这是是一个行注释
       >    COL1 CHAR(20)           -- 第一个CHAR字段，这也是一个行注释
       >  /*  这个表是做测试用的，有两个字段：
       >      ID和COL
       >      这上下的4行内容都是段落注释
       >   */
       > );
    SQL> 

```  
***
#### 设置程序的运行选项
通过SET命令，我们可以改变SQLCli的一些行为或者显示选项。
```
    SQL> set
    Current set options: 
    +-------------------+----------+----------+
    | Name              | Value    | Comments |
    +-------------------+----------+----------+
    | WHENEVER_SQLERROR | CONTINUE | ----     |
    | PAGE              | OFF      | ----     |
    | ECHO              | ON       | ----     |
    | TIMING            | OFF      | ----     |
    | TIME              | OFF      | ----     |
    | OUTPUT_FORMAT     | ASCII    | ----     |
    | CSV_HEADER        | OFF      | ----     |
    | CSV_DELIMITER     | ,        | ----     |
    | CSV_QUOTECHAR     |          | ----     |
    | FEEDBACK          | ON       | ----     |
    | TERMOUT           | ON       | ----     |
    | ARRAYSIZE         | 10000    | ----     |
    | SQLREWRITE        | OFF      | ----     |
    | DEBUG             | OFF      | ----     |
    | LOB_LENGTH        | 20       | ----     |
    | FLOAT_FORMAT      | %.7g     | ----     |
    | DOUBLE_FORMAT     | %.10g    | ----     |
    +-------------------+----------+----------+
  没有任何参数的set将会列出程序所有的配置情况。

```
   
&emsp; &emsp; 主要的控制参数解释：  
&emsp; &emsp; 1.ECHO    SQL回显标志， 默认为ON，即SQL内容在LOG中回显
```
        SQL> set ECHO ON      # 在LOG中将会回显SQL语句
        SQL> set ECHO OFF     # 在LOG中不会回显SQL语句

        例如：执行SELECT 3 + 5 COL1 FROM DUAL，

        在ECHO打开下，log文件内容如下:
        SQL> SELECT 3 + 5 COL1 FROM DUAL;
        SQL> ===========
        SQL> =  COL1 ===
        SQL> ===========
        SQL>           8
        SQL> 1 rows selected.

        在ECHO关闭下，log文件内容如下:
        SQL> ===========
        SQL> =  COL1 ===
        SQL> ===========
        SQL>           8
        SQL> 1 rows selected.
```

&emsp; &emsp; 2. WHENEVER_SQLERROR  SQL错误终端表示， 用来控制在执行SQL过程中遇到SQL错误，是否继续。 默认是CONTINUE，即继续。   
&emsp; 目前支持的选项有：    
```
       CONTINUE |     遇到SQL语句错误继续执行 
       EXIT     |     遇到SQL语句错误直接退出SQLCli程序
```
&emsp; &emsp; 3. PAGE        是否分页显示，当执行的SQL语句结果超过了屏幕显示的内容，是否会暂停显示，等待用户输入任意键后继续显示下一页，默认是OFF，即不中断。

&emsp; &emsp; 4. OUTPUT_FORMAT   显示格式， 默认是ASCII
&emsp; 目前支持的选项有：
```
      ASCII    |     显示格式为表格的格式 
      CSV      |     显示格式为CSV文件的格式
      VERTICAL |     分列显示
```
&emsp; 以下是一个例子：
```
       SQL> set output_format ascii
       SQL> select * from test_tab;
       +----+----------+
       | ID | COL2     |
       +----+----------+
       | 1  | XYXYXYXY |
       | 1  | XYXYXYXY |
       +----+----------+
       2 rows selected.
      
       SQL> set output_format csv
       SQL> select * from test_tab;
       "ID","COL2"
       "1","XYXYXYXY"
       "1","XYXYXYXY"
       2 rows selected.
       SQL>           

       SQL> set output_format VERTICAL
       SQL> select * from test_tab;
        ***************************[ 1. row ]***************************
        ID     | 1
        COL2   | XYXYXYXY
        ***************************[ 2. row ]***************************
        ID     | 2
        COL2   | XYXYXYXY
        2 rows selected.
```

&emsp; &emsp; 5. LOB_LENGTH      控制LOB字段的输出长度，默认是20  
&emsp; &emsp; 由于LOB字段中的文本长度可能会比较长，所以默认不会显示出所有的LOB内容到当前输出中，而是最大长度显示LOB_LENGTH值所代表的长度对于超过默认显示长度的，将在输出内容后面添加...省略号来表示   
&emsp; &emsp; 对于BLOB类型，输出默认为16进制格式。对于超过默认显示长度的，将在输出内容后面添加...省略号来表示 
```       
        SQL> set LOB_LENGTH 300     # CLOB将会显示前300个字符
        例子，执行一个CLOB字段查询,CLOB中的信息为ABCDEFGHIJKLMNOPQRSTUVWXYZ
        SQL> set LOB_LENGTH 5
        SQL> SELECT CLOB1 FROM TEST_TAB;
        SQL> ===========
        SQL> =  CLOB1 ==
        SQL> ===========
        SQL>       ABCDE
        SQL> 1 rows selected.
        SQL> set LOB_LENGTH 15
        SQL> =====================
        SQL> =        CLOB1      =
        SQL> =====================
        SQL>    ABCDEFGHIJKLMNO...
        SQL> 1 rows selected.
```

&emsp; &emsp; 6. FEEDBACK      控制是否回显执行影响的行数，默认是ON，显示  
```
       SQL> set feedback on
       SQL> select * from test_tab;
       +----+----------+
       | ID | COL2     |
       +----+----------+
       | 1  | XYXYXYXY |
       | 1  | XYXYXYXY |
       +----+----------+
       2 rows selected.
       SQL> set feedback off
       SQL> select * from test_tab;
       +----+----------+
       | ID | COL2     |
       +----+----------+
       | 1  | XYXYXYXY |
       | 1  | XYXYXYXY |
       +----+----------+
```
&emsp; &emsp; 7. TERMOUT       控制是否显示SQL查询的返回，默认是ON，显示  

```
       SQL> set termout on
       SQL> select * from test_tab;
       +----+----------+
       | ID | COL2     |
       +----+----------+
       | 1  | XYXYXYXY |
       | 1  | XYXYXYXY |
       +----+----------+
       2 rows selected.
       SQL> set termout off
       SQL> select * from test_tab;
       2 rows selected.

```
&emsp; &emsp; 8. FLOAT_FORMAT    控制浮点数字的显示格式，默认是%.7g
```
    SQL>  select abs(1.234567891234) from dual;
    +----------+
    | C1       |
    +----------+
    | 1.234568 |
    +----------+
    1 row selected.
    SQL> set FLOAT_FORMAT %0.10g
    SQL>  select abs(1.234567891234) from dual;
    +-------------+
    | C1          |
    +-------------+
    | 1.234567891 |
    +-------------+
    1 row selected.

    类似的参数还有DOUBLE_FORMAT
```
&emsp; &emsp; 9. CSV格式控制  
```
    CSV_HEADER        控制CSV输出中是否包括字段名称信息， 默认是OFF
    CSV_DELIMITER     CSV输出中字段的分隔符, 默认为逗号，即,  
    CSV_QUOTECHAR     CSV中字符类型字段的前后标记符号，默认为不标记

    SQL> select * from cat where rownum<10;
    ADATA_1000W,TABLE
    ADATA_100W,TABLE
    ADATA_10W,TABLE
    ADATA_1W,TABLE
    ADATA_2000W,TABLE
    ADATA_500W,TABLE
    BIN$p+HZrV/nKjTgU1ABqMCZxw==$0,TABLE
    BIN$p+HaveUdKjzgU1ABqMC4xg==$0,TABLE
    BIN$p+HbAAWeKlfgU1ABqMCSUg==$0,TABLE
    
    SQL> select 1.2+2.2 from dual
       > union
       > select 3+4 from dual;
    3.4
    7
```

#### 在SQL中使用变量信息
&emsp; &emsp; 在一些场景中，我们需要通过变量来变化SQL的运行  
&emsp; &emsp; 这里提供的解决办法是：   
&emsp; &emsp; * 用set的语句来定义一个变量
```
    SQL> set @var1 value1
```
&emsp; &emsp; * 用${}的方式来引用已经定义的变量
```
    SQL> select ${var1} from dual;
    REWROTED SQL> Your SQL has been changed to:
    REWROTED    > select value1 from dual
    -- 这里真实的表达意思是： select value1 from dual, 其中${var1}已经被其对应的变量替换
```
#### 在SQL中使用spool命令来将当前执行结果输出到文件中
```
    SQL> spool test.log
    SQL> select 1+2 from dual;
    3
    1 rows selected.
    SQL> spool off

    $> type test.log
    SQL> select 1+2 from dual;
    3
    1 rows selected.

    -- spool [file name] 表示将从此刻开始，随后的SQL语句输出到新的文件中
    -- spool off         表示从此刻开始，关于之前的SQL文件输出

    * 在已经进行spool的过程中，spool新的文件名将导致当前文件被关闭，随后的输出切换到新文件中
    * spool 结果的目录：
      1. 如果程序运行过程中提供了logfile信息，则spool结果的目录和logfile的目录相同
      2. 如果程序运行过程中没有提供logfile信息，则spool结果文件目录就是当前的文件
```


#### 在SQL中用内置变量来查看上一个SQL的执行结果
&emsp; &emsp; 有一些场景通常要求我们来下一个SQL指定的时候，将上一个SQL的结果做出参数来执行  
&emsp; &emsp; 这里提供的解决办法是： 在SQL中引入被特殊定义的标志  
&emsp; &emsp; 具体的标志有：  
&emsp; &emsp; &emsp; &emsp; 1：  %lastsqlresult.LastAffectedRows%
```
    SQL>  select abs(1.234567891234) from dual;
    +----------+
    | C1       |
    +----------+
    | 1.234568 |
    +----------+
    1 row selected.
    SQL> select %lastsqlresult.LastAffectedRows% From Dual;
    +----------+
    | C1       |
    +----------+
    | 1        |
    +----------+
   这里的1表示之前第一个SQL中1 row selected的1
   
```  
&emsp; &emsp; &emsp; &emsp; 2：  %lastsqlresult.LastSQLResult%  
```
    SQL>  select abs(1.234567891234) from dual;
    +----------+
    | C1       |
    +----------+
    | 1.234568 |
    +----------+
    1 row selected.
    SQL> select %lastsqlresult.LastSQLResult[0][0]% From Dual;
    +----------+
    | C1       |
    +----------+
    | 1.234568 |
    +----------+
   这里的1.234568表示之前第一个SQL返回结果记录集中的第0行，第零列
   在这里，LastSQLResult后面的数据前述SQL结果集的行、列。可取值范围为： 0 - (Rowsaffected - 1), 0 - (column number -1)

```  

#### 执行特殊的内部语句
   这里指的内部语句是说不需要后台SQL引擎完成，而是通过在SQLCli程序中扩展代码来支持的语句。  
   目前支持的扩展语句有：
```
   SQL> __internal__ CREATE [string|integer] SEEDDATAFILE [SeedName] LENGTH [row length] ROWS [row number]  
   SQL> WITH NULL ROWS [null rows number];
   这个程序将在$SQLCLI_HOME/data下创建若干seed文件，用来后续的随机函数

    [string|integer]          是字符型随机数据文件还是数字型随机数据文件
    [SeedName]                数据种子的名称，根据需要编写，应简单好记
    [row length]              数据种子中每行数据的最大长度
    [row number]              数据种子的行数
    [null rows number]        在该数据文件row number的行数中有多少行为空行

   __internal__ CREATE [MEM|FS|HDFS] FILE [xxx]
   (
     如果参数中提供了ROWS：
         这里将把括号内内容理解为一行内容，其中的换行符在处理过程中被去掉
         相关信息将被完成宏替换后，重复ROWS行数后构成一个文件
     如果参数没有提供了ROWS：
         这里将把括号内内容理解为多行内容
         相关信息将被完成宏替换后构成一个文件
   ) ROWS [number of rows];
   
   MEM:  表示文件将被创建在内存中，程序退出后，文件将不复存在
   FS:   表示文件将被创建在文件中，需要注意的是，这里的目录不是真实的OS目录，而是一个相对于工作目录的相对目录
   HDFS: 表示文件将被创建在HDFS上，这里需要远程的HDFS服务器开启WebFS的功能
         文件格式例子：  http://nodexx:port/dirx/diry/xx.dat
         其中port是webfs的port，不是rpc的port， SQLCli并不支持rpc协议
   这里语句的开头：  __internal__ 是必须的内容，固定写法
   宏代码的格式包括：
     {identity(start_number)}                  表示一个自增字段，起始数字为start_number
     {random_ascii_letters(length)}            表示一个随机的ascii字符串，可能大写，可能小写，最大长度为length
     {random_ascii_lowercase(length)}          表示一个随机的ascii字符串，只能是大写字母，最大长度为length
     {random_ascii_uppercase(length)}          表示一个随机的ascii字符串，只能是小写字母，最大长度为length
     {random_digits(length)}                   表示一个随机的数字，可能数字，最大长度为length
     {random_ascii_letters_and_digits(length)} 表示一个随机的ascii字符串，可能大写，可能小写，可能数字，最大长度为length
     {random_from_seed(seedname,length}        表示从seed文件中随机选取一个内容，并且最大长度限制在length, 此时seedname不要引号
     {random_date(start, end, frmt)}           表示一个随机的日期， 日期区间为 start到end，日期格式为frmt
                                               frmt可以不提供，默认为%Y-%M-%D
     {random_timestamp(start, end, frmt)}      表示一个随机的时间戳， 时间区间为 start到end，日期格式为frmt
                                               frmt可以不提供，默认为%Y-%M-%D %H:%M%S
     {random_boolean())                        表示一个随机的Boolean，可能为0，也可能为1

    __internal__ CREATE [MEM|FS|HDFS] FILE [From file] FROM [MEM|FS|HDFS] FILE [To file] 
    这里将完成一个文件复制。 

   注意：
     如果需要发送数据到kafka, 必须提前设置kafka服务器的地址, 设置的方法是：
     SQL>  set KAFKA_SERVERS [kafka Server地址]:[kafka 端口号]


   例子：
   SQL> __internal__ CREATE FS FILE abc.txt
      > (
      > {identity(10)},'{random_ascii_letters(5)}','{random_ascii_lowercase(3)}'
      > ) ROWS 3;
    会在当前的文件目录下创建一个名字为abc.txt的文本文件，其中的内容为：
    10,'vxbMd','jsr'
    11,'SSiAa','vtg'
    12,'SSdaa','cfg'

   SQL> __internal__ CREATE FS FILE abc.txt
      > (
      > {identity(10)},'{random_ascii_letters(5)}','{random_ascii_lowercase(3)}'
      > {identity(10)},'{random_ascii_letters(5)}','{random_ascii_lowercase(3)}'
      > );
    会在当前的文件目录下创建一个名字为abc.txt的文本文件，其中的内容为：
    10,'vxbMd','jsr'
    11,'SSiAa','vtg'

   SQL> __internal__ CREATE FS FILE abc.txt FROM HDFS FILE http://nodexx:port/def.txt
   会从HDFS上下载一个文件def.txt, 并保存到本地文件系统的abc.txt中


   SQL> __internal__ create kafka server [bootstrap_server];
   创建一个Kafka服务器的配置信息，这个配置信息将用于随后的操作，格式位nodex:port1,nodey:port2....

   SQL> __internal__ create kafka topic [topic name] Partitions [number of partitions] replication_factor [number of replication factor];
   创建一个kafka的Topic，Topic的具体参数参考[topic name]， [number of partitions]， [number of replication factor]
   例子： __internal__ create kafka topic mytopic Partitions 16 replication_factor 1;
   注意： 由于kafka的机制，创建topic总是会立即成功的，但立即成功并不代表立即可用
         如果脚本中随后要立刻用到刚刚创建的topic，则需要休息一点时间后在进行尝试。比如sleep 3 

   SQL> __internal__ get kafka Offset topic [topic name] Partition [Partition id] group [gruop id];
   获得指定分区的高水位线和低水位线
    +-----------+-----------+
    | minOffset | maxOffset |
    +-----------+-----------+
    | 0         | 57        |
    +-----------+-----------+
   例子： __internal__ get kafka Offset topic mytopic Partition 0 group “”;

   SQL> __internal__ create kafka message from file [text file name] to topic [topic name];
   将指定文本文件中所有内容按行发送到Kafka指定的Topic中
   例子： __internal__ create kafka message from file Doc.md to topic Hello;

    SQL> __internal__ create kafka message topic [topic name]
       > (
       >    [message item1]
       >    [message item2] 
       >    [message item3]
       》 );
    将SQL中包含的Message item发送到kafka指定的topic中
    上述的例子，将发送3条消息到服务器。

    SQL> __internal__ drop kafka topic [topic name];
    删除指定的topic
```

***    
#### 退出
你可以使用exit来退出命令行程序，或者在脚本中使用Exit来退出正在执行的脚本
```
SQL> exit
Disconnected.
```
注意： 如果当前有后台任务正在运行，EXIT并不能立刻完成。  
1： 控制台应用：EXIT将不会退出，而是会提示你需要等待后台进程完成工作  
2： 脚本应用：  EXIT不会直接退出，而是会等待后台进程完成工作后再退出  
***
### 程序的并发和后台执行
SQLCli被设计为支持并发执行脚本，支持后台执行脚本。  
为了支持后台操作，我们这里有一系列的语句，他们是：  
1：  SubmitJob      提交后台任务    
2：  ShowJob        显示当前已经提交的后台任务，以及他们的运行状态  
3：  CloseJob       关闭正在运行JOB的后续循环执行，当前正在的脚本将被继续执行完成  
4：  ShutdownJob    终止正在运行的JOB，当前正在运行的SQL将被继续执行完成  
5：  AbortJob       放弃正在执行的JOB，当前正在运行的SQL将被强制中断  
6：  WaitJob        等待作业队列完成相关工作  
7：  StartJob       开始运行后台作业  
#### 如果提交脚本到后台任务
在很多时候，我们需要SQLCli来帮我们来运行数据库脚本，但是又不想等待脚本的运行结束。
我们可以通过SubmitJob的方式来将我们的脚本提交到后台  
SubmitJob有3个参数，他们是：  
1：  计划被执行的脚本  
2：  脚本将会启动的份数，默认为1， 即只启动1份  
3：  脚本将会循环执行的次数，默认为1，即执行完成1次后就会退出  
```
SQL> submitjob task_1.sql 3 5
3 Jobs Submittted.
task_1.sql：  计划后台执行的脚本。这里你可以输入全路径名，也可以输入当前脚本的相对文件名。
3         ：  同时运行的份数，这个参数可以被忽略，如果被忽略，那么同时运行的份数是1
5         ：  被循环执行的次数，这个参数可以被忽略，如果被忽略，那么循环运行的次数是1
注意： SubmitJob并不会真的开始运行你提交的脚本，他只是一个记录，如果要启动他，需要用后面提到的startjob
注意： 你当前SQL会话中的连接信息、SQL重写信息、版本显示的设置、运行日志的信息都被会子SQL继承。
      你可以选择在task_1.sql中重写覆盖这些信息，但起默认信息来自于主程序的设置。
SQLCli会为每一个后台任务编一个不重复的JOBID，具体的JOBID可以通过showjob来查看。
对于前面的这个例子，启动份数是3，则在ShowJOBS里头能看见3个不同的JOBID
```
#### 如何查看后台任务脚本的运行情况
通过showjob可以查看我们之前提交情况，脚本的运行情况，运行的开始时间，运行的结束时间，当前正在运行的SQL等。
```
SQL> showjob all
+------+--------------------+-------------+---------------------+---------------------+----------+-----------+
| JOB# | ScriptBaseName     | Status      | Started             | End                 | Finished | LoopCount |
+------+--------------------+-------------+---------------------+---------------------+----------+-----------+
| 1    | task_1.sql         | RUNNING     | 2020-05-25 17:46:28 | <null>              | 1        | 5         |
| 2    | task_1.sql         | RUNNING     | 2020-05-25 17:46:28 | <null>              | 2        | 5         |
| 3    | task_1.sql         | STOPPED     | 2020-05-25 17:46:28 | 2020-05-25 17:46:47 | 5        | 5         |
| 4    | task_2.sql         | Not Started | <null>              | <null>              | 0        | 5         |
| 5    | task_2.sql         | Not Started | <null>              | <null>              | 0        | 5         |
+------+--------------------+-------------+---------------------+---------------------+----------+-----------+
Total 5 Jobs.
这里可以看到目前5个脚本已经提交，其中：
1： 任务1，任务2 都在运行中，5次循环已经分别完成了1次和2次
2： 任务3已经完全运行完成
3： 任务4，任务5还完全没有开始

SQL> showjob 1                                                                                                                                               
Job Describe [1]
  ScriptBaseName = [xxx.sql]
  ScriptFullName = [/Users/xxxxx/task_1.sql]
  Status = [Not Started]
  StartedTime = [None]
  EndTime = [None]
  Current_SQL = [None]
这里可以看到具体对于JOB编号为1的任务的详细情况
```
  
#### 如何启动后台任务脚本
通过startJob的方式，我可以启动全部的后台任务或者只启动部分后台任务
```
SQL> startjob all
3 Jobs Started.
这里会将你之前提交的所有后台脚本都一次性的启动起来
SQL> startjob 1
1 Jobs Started.
这里只会启动JOBID为1的后台任务
随后，再次通过showjob来查看信息，可以注意到相关已经启动
```
#### 如何停止后台任务脚本
在脚本运行过程中，你可以用closejob来停止某个某个任务或者全部任务，
```
SQL> closejob all
3 Jobs will close.
这里会将当前运行的所有后台脚本都停止下来
SQL> closejob 1
Job 1 will close.
注意： closejob并不会真的终止你当前正在运行的SQL，甚至不会干扰当前脚本的运行。但是要求循环执行的脚本在本次执行结束后不再循环。
      只有在子任务完成当前脚本后，closejob才能完成。
      这个意思是说，如果你有一个比较大的长SQL作业，closejob并不能很快的终止任务运行。
```
在脚本运行过程中，你可以用shutdownjob来停止某个某个任务或者全部任务，
```
SQL> shutdownjob all
3 Jobs will shutdown.
这里会将当前运行的所有后台脚本都停止下来
SQL> shutdownjob 1
Job 1 will shutdown.
注意： shutdownjob并不会真的终止你当前正在运行的SQL，但是在这个SQL之后的所有SQL不再执行，要求循环执行的脚本也不再循环。
      如果shutdown的时候，程序正在执行sleep，则sleep会被打断
      只有在子任务完成当前SQL后，shutdownjob才能完成。
      这个意思是说，如果你有一个比较大的长SQL作业，shutdownjob并不能很快的终止任务运行。
```
#### 如何强行停止后台任务脚本
在脚本运行过程中，你可以用abortjob来强行停止某个某个任务或者全部任务，
```
SQL> abortjob all
3 Jobs will terminate.
这里会将当前运行的所有后台脚本都强制停止下来
SQL> abortjob 1
Job 1 will terminate.
注意： abortjob并不会真的杀掉你的子进程，但是会强制断开子进程的数据库连接，用户当前执行的SQL可能会出错退出
      这个意思是说，如果子进程正在运行一个SQL，SQL运行会报错退出
      如果abortjob的时候，程序正在执行sleep，则sleep会被打断
      如果你有一个比较大的长SQL作业，abortjob并不能很快的终止任务运行，但相对来说，比shutdownjob要快的多。
```
#### 等待后台任务脚本运行结束
在脚本运行过程中，你可以用waitjob来等待后台脚本的运行结束
```
SQL> waitjob all
3 Jobs completed
waitjob不会退出，而是会一直等待相关脚本结束后再退出
```

#### 后台执行脚本在主程序退出时候的影响
如果当前有后台程序运行：  
1： 控制台应用：EXIT将不会退出，而是会提示你需要等待后台进程完成工作  
2： 脚本应用：  EXIT不会直接退出，而是会等待后台进程完成工作后再退出  

### 程序员必读部分
```
---------- sqlcli
--------------- __init__.py            # 包标识文件，用来记录版本信息
--------------- commandanalyze.py      # 对用户或者脚本输入的命令进行判断，判断是否需要后续解析，或者执行内部命令
--------------- kafkawrapper.py        # 程序中对kafka操作的相关支持
--------------- sqlparse.py            # 用来解析SQL语句，判断注释部分，语句的分段、分行等
--------------- sqlinternal.py         # 执行internal命令
--------------- sqlexecute.py          # 程序主要逻辑文件，具体执行SQL语句
--------------- sqlcliexception.py     # 自定义程序异常类
--------------- sqloption.py           # 程序运行参数显示及控制实现
--------------- main.py                # 主程序
---------- setup.py                    # Python打包发布程序
---------- README.md                   # 应用程序说明，由于pypi限制，这里只放置简要信息
---------- Doc.md                      # 应用程序文档
---------- conf                        # 配置文件目录
--------------  sqlcli.conf            # 程序配置文件
---------- jlib                        # 应用程序连接数据库需要的各种jar包
--------------  xxxx1.jar              # 具体的jar包文件
--------------  xxxx2.jar              # 具体的jar包文件
---------- .gitignore                  # git控制文件
---------- uploadpypi.bat              # windows平台下用来向pypi更新安装包的相关命令
```
