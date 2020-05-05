# SQLCli 快速说明

SQLCli 是一个命令行工具， 用来连接数据库，通过交互或者批处理的方式来执行SQL语句。  
SQLCli 是一个Python程序，通过jaydebeapi连接数据库的JDBC驱动。
***
### 谁需要用这个文档

需要通过SQL语句来查看、维护数据内容。使用的前提是你熟悉基本的SQL操作。
***
### 安装
安装的前提有：
   * 有一个Python 3.6以上的环境，并且安装了pip包管理器
   * 能够连接到互联网上， 便于下载必要的包
   * 对于Windows平台，还需要提前安装微软的C++编译器（Jaydebeapi安装过程中需要动态编译JType）

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
SQL*Cli Release 0.0.13
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
  --help          Show this message and exit.
```  
--version 用来显示当前工具的版本号
```
(base) sqlcli --version
Version: 0.0.13
```
--logon  用来输入连接数据的的用户名和口令
```
(base) sqlcli --logon user/pass
Version: 0.0.13
Driver loaded.
Database connected.
SQL>
如果用户、口令正确，且相关环境配置正常，你应该看到如上信息。

user/pass : 数据库连接的用户名和口令  

成功执行这个命令的前提是你已经在环境变量中设置了数据库连接的必要信息。  
这里的必要信息包括：
环境变量：  SQLCLI_CONNECTION_URL  
    参数格式是： jdbc:[数据库类型]:[数据库通讯协议]://[数据库主机地址]:[数据库端口号]/[数据库服务名]  
环境变量：  SQLCLI_CONNECTION_JAR_NAME  
    参数格式是： xxxxx-jdbc-x.x.x.jar
    这是程序连接数据库需要用到的Jar包，具体的写法应查询相应数据库产品的JDBC文档。
    对于一个特定的数据库有一个特定的写法，但是后面x.x.x代表的版本信息可能会有所不同。
    jar包必须放置在sqlcli程序能够访问到的目录下，如果不在当前目录下，这里需要写入相对或者绝对路径，比如: dir1\xxxxx-jdbc-x.x.x.jar
环境变量：  SQLCLI_CONNECTION_CLASS_NAME  
    参数格式是：  com.xxxx.xxxxxxx.jdbc.JdbcDriver  
    这是一个类名，具体的写法应查询相应数据库产品的JDBC文档。通常对于一种特定的数据库有一个特定的写法
```
--logfile   用来记录本次命令行操作的所有过程信息  
这里的操作过程信息包含你随后在命令行里头的所有输出信息。  
如果你在随后的命令行里头设置了set ECHO ON，那么记录里头还包括了你所有执行的SQL语句原信息  
```
(base) sqlcli --logon user/pass --logfile test.log
Version: 0.0.13
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
Version: 0.0.13
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
SQL*Cli Release 0.0.13
SQL>   
区别：
(base) sqlcli --nologo
SQL>
```
***
#### 加载数据库驱动
在sqlcli命令行里头，可以通过load命令来加载数据库驱动文件。
```
(base) sqlcli 
SQL*Cli Release 0.0.13
SQL> load  xxxxx-jdbc-x.x.x.jar   com.xxxx.xxxxxxx.jdbc.JdbcDriver 
Driver loaded.
SQL> 

这里具体的写法应查询相应数据库产品的JDBC文档  
jar包必须放置在sqlcli程序能够访问到的目录下，如果不在当前目录下，这里需要写入相对或者绝对路径，比如: dir1\xxxxx-jdbc-x.x.x.jar  
  
再次执行load命令，则会断开当前的数据库连接，并重新加载新的驱动。load后，下次执行的数据库操作将依赖新的加载包  
```
***
#### 连接数据库
在sqlcli命令行里头，可以通过connect命令来连接到具体的数据库
```
(base) sqlcli 
SQL*Cli Release 0.0.13
SQL> connect user/pass@jdbc:[数据库类型]:[数据库通讯协议]://[数据库主机地址]:[数据库端口号]/[数据库服务名] 
Database connected.
SQL> 
能够成功执行connect的前提是： 数据库驱动已经被手工加载，或者通过环境变量的方式，在程序启动之前已经被默认加载

如果已经在环境变量中指定了SQLCLI_CONNECTION_URL，连接可以简化为
(base) sqlcli 
SQL*Cli Release 0.0.13
SQL> connect user/pass
Database connected.
SQL> 

在数据库第一次连接后，第二次以及以后的连接可以不再输入连接字符串，程序会默认使用上一次已经使用过的连接字符串信息，比如：
(base) sqlcli 
SQL*Cli Release 0.0.13
SQL> connect user/pass@jdbc:[数据库类型]:[数据库通讯协议]://[数据库主机地址]:[数据库端口号]/[数据库服务名] 
Database connected.
SQL> connect user2/pass2
Database connected.
SQL> 

```
***
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
#### 执行特殊的内部语句
   SQLCli支持的特殊语句包括两种：
   * 显示或行为控制语句  
   通过在SQLCli命令行里头执行set命令可以控制显示的格式以及变化SQLCli执行的行为。
   
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
      ASCII |     显示格式为表格的格式 
      CSV   |     显示格式为CSV文件的格式
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

   * 内部的其他操作  
   这里指的内部语句是说不需要后台SQL引擎完成，而是通过在SQLCli程序中扩展代码来支持的语句。目前支持的扩展语句有：
```
   __internal__ CREATE FILE '[mem://xxx]|[file://xxx]|[zip://xxx]|[tar://xxx]|[kafka://xxx]'
   (
     这里输入一个文本，文本中可以包含宏代码来表示特殊信息. 
     这里可以换行，但是换行符会在处理过程中被去掉
   ) ROWS [number of rows];
   
   这里语句的开头：  __internal__ 是必须的内容，固定写法
   宏代码的格式包括：
     {identity(start_number)}         表示一个自增字段，起始数字为start_number
     {random_ascii_letters(length)}   表示一个随机的ascii字符串，可能大写，可能小写，可能数字，最大长度为length
     {random_ascii_lowercase(length)} 表示一个随机的ascii字符串，只能是大写字母，最大长度为length
     {random_ascii_uppercase(length)} 表示一个随机的ascii字符串，只能是小写字母，最大长度为length
     {random_digits(length)}          表示一个随机的数字，可能数字，最大长度为length
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
你可以使用exit或者quit来退出命令行程序，这里exit和quit的效果是完全一样的
```
SQL> exit
Disconnected.
或者
SQL> quit
Disconnected.
```
***
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
