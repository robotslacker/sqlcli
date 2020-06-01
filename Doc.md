# SQLCli 快速说明

SQLCli 是一个命令行工具， 用来连接数据库，通过交互或者批处理的方式来执行SQL语句。  
SQLCli 是一个Python程序，通过jaydebeapi连接数据库的JDBC驱动。

SQLCli 目前可以支持的数据库有：  
   Oracle  
   MySQL  
   PostgreSQL  
   SQLServer  
   TeraData  
   LinkoopDB  
   其他符合标准JDBC规范的数据库  
***
### 谁需要用这个文档

需要通过SQL语句来查看、维护数据内容。使用的前提是你熟悉基本的SQL操作。
***
### 安装
安装的前提有：
   * 有一个Python 3.6以上的环境
   * 能够连接到互联网上， 便于下载必要的包
   * 安装JDK8
   * 对于Windows平台，还需要提前安装微软的C++编译器（Jaydebeapi安装过程中需要动态编译jpype）  
     在jaydebeapi 1.1.1的版本下，发现jpype必须进行降级，否则无法使用  
     pip install --upgrade jpype1==0.6.3 --user
   * 对于Linux平台，也需要提前安装gcc编译器（Jaydebeapi安装过程中需要动态编译jpype）  
     yum install -y gcc-c++ gcc  
     在jaydebeapi 1.1.1的版本下，发现jpype必须进行降级，否则无法使用  
     pip install --upgrade jpype1==0.6.3 --user

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
### 使用
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
如果你在随后的命令行里头设置了set ECHO ON，那么记录里头还包括了你所有执行的SQL语句原信息  
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
1、StartedTime  SQL运行开始时间，格式是：%Y-%m-%d %H:%M:%S  
2、elapsed      SQL运行的消耗时间，这里的单位是毫秒  
3、SQL          运行的SQL，在这个运行日志中，并不会打印SQL全文，而是只打印SQL的前40个字符  
4、SQLStatus    SQL运行结果，0表示运行正常结束，1表示运行错误  
5、ErrorMessage 错误日志，在SQLStatus为1的时候才有意义  
6、thread_name  工作线程名，对于主程序，这里显示的是MAIN  
说明：上述信息都有TAB分隔，其中字符信息用单引号包括，如下是一个例子：  
```
Started elapsed SQLPrefix       SQLStatus       ErrorMessage    thread_name
'2020-05-25 17:46:23'       0.00        'loaddriver localtest\linkoopdb-jdbc-2.3.'      0       ''      'MAIN'
'2020-05-25 17:46:23'       0.28        'connect admin/123456'  0       ''      'MAIN'
'2020-05-25 17:46:24'       0.00        'SET ECHO ON'   0       ''      'MAIN'
'2020-05-25 17:46:24'       0.00        'SET TIMING ON' 0       ''      'MAIN'
'2020-05-25 17:46:24'       0.01        'LOADSQLMAP stresstest' 0       ''      'MAIN'
'2020-05-25 17:46:24'       0.92        'ANALYZE TRUNCATE STATISTICS'   0       ''      'MAIN'
'2020-05-25 17:46:25'       0.02        'SELECT count(SESSION_ID)  FROM INFORMATI'      0       ''      'MAIN'
'2020-05-25 17:46:25'       1.37        'drop user testuser if exists cascade'  0       ''      'MAIN'
'2020-05-25 17:46:26'       0.54        'CREATE USER testuser PASSWORD '123456''        0       ''      'MAIN'
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
| loaddriver   | load JDBC driver .          |
| loadsqlmap   | load SQL Mapping file .     |
| quit         | Quit.                       |
| set          | set options .               |
| showjob      | show informations           |
| shutdownjob  | Shutdown Jobs               |
| sleep        | Sleep some time (seconds)   |
| start        | Execute commands from file. |
| StartJob     | Start Jobs                  |
| Submitjob    | Submit Jobs                 |
+--------------+-----------------------------+
这里显示的是所有除了标准SQL语句外，可以被执行的各种命令开头。
标准的SQL语句并没有在这里显示出来，你可以直接在控制行内或者脚本里头执行SQL脚本。
```

#### 加载数据库驱动
仅在如下情况下，需要加载数据库驱动：  
* 你需要测试的数据库不是我们已经内嵌支持的数据库  
  内嵌支持的数据库有：SQLServer,Oracle,MySQL,PostgreSQL,TeraData  
* 你需要为你的程序使用更新的jdbc驱动程序  

在sqlcli命令行里头，可以通过load命令来加载数据库驱动文件。
```
(base) sqlcli 
SQL*Cli Release 0.0.32
SQL> loaddriver  xxxxx-jdbc-x.x.x.jar   com.xxxx.xxxxxxx.jdbc.JdbcDriver 
Driver loaded.
SQL> 

这里具体的写法应查询相应数据库产品的JDBC文档  
loaddrvier 命令的Jar包查找顺序：
  1. 如果给出的是绝对路径，或者相对用户当前目录的相对目录，那么以这个目录为准
  2. 如果在上述目录下没有找到文件，则会在脚本目录的相对目录开始查找

如果有多个Jar包需要加载，你可能需要多次执行loadriver  
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
能够成功执行connect的前提是： 数据库驱动已经被手工加载，或者通过环境变量的方式，在程序启动之前已经被默认加载

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
LinkoopDB:
    connect username/password@jdbc:linkoopdb:tcp://IP:Port/Service_Name
MYSQL:
    connect username/password@jdbc:mysql:tcp://IP:Port/Service_Name
PostgreSQL：
    connect username/password@jdbc:postgresql:tcp://IP:Port/Service_Name
SQLServer：
    connect username/password@jdbc:sqlserver:tcp://IP:Port/DatabaseName
TeraData：
    connect username/password@jdbc:teradata:tcp://IP:/DatabaseName
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
   以下是一个重写文件的典型格式， 1，2，3 是文件的行号，不是真实内容：
    1 #..*:                                      
    2 ((?i)CREATE TABLE .*\))=>\1 engine pallas
    3 #.

    行1：
        这里定义的是参与匹配的文件名，以#.开头，以:结束，如果需要匹配全部文件，则可以在中间用.*来表示
    行2：
        这里定义的是一个正则替换规则, =>前面的部分是查找表达式，=>后面的部分是需要被替换的内容
    行3：
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
        REWROTED    > ) engine pallas
     这里第一段是SQL文件中的原信息，带有REWROTED的信息是被改写后的信息

```

#### 执行数据库SQL语句
在数据库连接成功后，我们就可以执行我们需要的SQL语句了，对于不同的SQL语句我们有不同的语法格式要求。  
* 对于SQL语句块的格式要求：  
  SQL语句块是指用来创建存储过程、SQL函数等较长的SQL语句  
  SQL语句块的判断依据是：
```
     CREATE | REPLACE ******   FUNCTION|PROCEDURE ****
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
    CREATE | SELECT | UPDATE | DELETE | INSERT | __INTERNAL__ | DROP | REPLACE

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
    +-------------------+----------+
    | option            | value    |
    +-------------------+----------+
    | WHENEVER_SQLERROR | CONTINUE |
    | PAGE              | OFF      |
    | OUTPUT_FORMAT     | ASCII    |
    | ECHO              | ON       |
    | LONG              | 20       |
    | KAFKA_SERVERS     | None     |
    | TIMING            | OFF      |
    | TERMOUT           | ON       |
    | FEEDBACK          | ON       |
    | ARRAYSIZE         | 10000    |
    | SQLREWRITE        | OFF      |
    | DEBUG             | OFF      |
    +-------------------+----------+
  没有任何参数的set将会列出程序所有的配置情况。

```
   
   目前支持的控制参数有：  
1.&emsp; ECHO    SQL回显标志， 默认为OFF，即SQL内容在LOG中不回显
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

2.&emsp; WHENEVER_SQLERROR  SQL错误终端表示， 用来控制在执行SQL过程中遇到SQL错误，是否继续。 默认是CONTINUE，即继续。   
&emsp; 目前支持的选项有：    
```
       CONTINUE |     遇到SQL语句错误继续执行 
       EXIT     |     遇到SQL语句错误直接退出SQLCli程序
```
&emsp; PAGE        是否分页显示，当执行的SQL语句结果超过了屏幕显示的内容，是否会暂停显示，等待用户输入任意键后继续显示下一页，默认是OFF，即不中断。


3.&emsp; OUTPUT_FORMAT   显示格式， 默认是ASCII
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

4.&emsp; LONG      控制LOB字段的输出长度，默认是20  
&emsp; &emsp; 由于LOB字段中的文本长度可能会比较长，所以默认不会显示出所有的LOB内容到当前输出中，而是最大长度显示LONG值所代表的长度  
```       
        SQL> set long 300     # CLOB将会显示前300个字符
        例子，执行一个CLOB字段查询,CLOB中的信息为ABCDEFGHIJKLMNOPQRSTUVWXYZ
        SQL> set long 5
        SQL> SELECT CLOB1 FROM TEST_TAB;
        SQL> ===========
        SQL> =  CLOB1 ==
        SQL> ===========
        SQL>       ABCDE
        SQL> 1 rows selected.
        SQL> set long 15
        SQL> =====================
        SQL> =        CLOB1      =
        SQL> =====================
        SQL>       ABCDEFGHIJKLMNO
        SQL> 1 rows selected.
```

5.&emsp; FEEDBACK      控制是否回显执行影响的行数，默认是ON，显示  
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
6.&emsp; TERMOUT       控制是否显示SQL查询的返回，默认是ON，显示  

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
#### 执行特殊的内部语句
   这里指的内部语句是说不需要后台SQL引擎完成，而是通过在SQLCli程序中扩展代码来支持的语句。  
   目前支持的扩展语句有：
```
   __internal__ CREATE SEEDDATAFILE;
   这个程序将在$SQLCLI_HOME下创建若干seed文件，用来后续的随机函数
   若不创建这个文件，则random_from_seed会抛出异常
   对于一个环境只需要执行一次这样的操作，以后的每次随机数操作并不需要重复进行这个操作。

   __internal__ CREATE FILE '[mem://xxx]|[file://xxx]|[zip://xxx]|[tar://xxx]|[kafka://xxx]'
   (
     这里输入一个文本，文本中可以包含宏代码来表示特殊信息. 
     这里可以换行，但是换行符会在处理过程中被去掉
   ) ROWS [number of rows];
   
   这里语句的开头：  __internal__ 是必须的内容，固定写法
   宏代码的格式包括：
     {identity(start_number)}                  表示一个自增字段，起始数字为start_number
     {random_ascii_letters(length)}            表示一个随机的ascii字符串，可能大写，可能小写，最大长度为length
     {random_ascii_lowercase(length)}          表示一个随机的ascii字符串，只能是大写字母，最大长度为length
     {random_ascii_uppercase(length)}          表示一个随机的ascii字符串，只能是小写字母，最大长度为length
     {random_digits(length)}                   表示一个随机的数字，可能数字，最大长度为length
     {random_ascii_letters_and_digits(length)} 表示一个随机的ascii字符串，可能大写，可能小写，可能数字，最大长度为length
     {random_from_seed(seedname,length}        表示从seed文件中随机选取一个内容，并且最大长度限制在length, 写的时候seedname不要引号
                                               
                                                支持的字符串seedname有， 10s， 100s， 1Ks, 10Ks, 100Ks
                                                由于seed文件中的字符串最大为100，这里设置比100更高的length并没有实际意义
                                                                                              
                                                支持的数字seedname有， 10n， 100n， 1Kn, 10Kn
                                                由于seed文件中的字符串最大为10，这里设置比10更高的length并没有实际意义
     {random_date(start, end)}                  表示一个随机的日期， 日期区间为 start到end，日期格式为%Y-%M-%D
     {random_timestamp(start, end)}             表示一个随机的时间戳， 时间区间为 start到end，日期格式为%Y-%M-%D %H:%M%S
     {random_boolean())                         表示一个随机的Boolean，可能为0，也可能为1

   文件格式：
     mem://xxx            会在内存中创建这个文件，一旦退出sqlcli，相关信息将会丢失
     file://xxx           会在本地文件系统中创建这个文件，其中xxx为文件名称信息
     zip://xxx            会在本地文件系统中创建一个ZIP压缩文件，其中xxx为文件名称信息
     tar://xxx            会在本地文件系统中创建一个TAR压缩文件，其中xxx为文件名称信息
     kafka://xxx          会连接Kafka服务器，将生成的字符串发送过去，其中xxx为需要发送的TOPIC内容
   注意：
     如果需要发送数据到kafka, 必须提前设置kafka服务器的地址, 设置的方法是：
     SQL>  set KAFKA_SERVERS [kafka Server地址]:[kafka 端口号]


   例子：
   SQL> __internal__ CREATE FILE file://abc.txt
      > (
      > {identity(10)},'{random_ascii_letters(5)}','{random_ascii_lowercase(3)}'
      > ) ROWS 2;
    会在当前的文件目录下创建一个名字为abc.txt的文本文件，其中的内容为：
    10,'vxbMd','jsr'
    11,'SSiAa','vtg'
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
--------------- sqlparse.py            # 用来解析SQL语句，判断注释部分，语句的分段、分行等
--------------- sqlinternal.py         # 执行internal命令
--------------- sqlexecute.py          # 执行SQL语句
--------------- commandanalyze.py      # 对用户或者脚本输入的命令进行判断，判断是否需要后续解析，或者执行内部命令
--------------- main.py                # 主程序
---------- setup.py                    # 打包发布程序


```
