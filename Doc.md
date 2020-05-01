# robotslacker-sqlcli

robotslacker-sqlcli 是一个SQL命令行工具，用来执行SQL脚本的工具，程序的开发中用到了jaydebeapi和prompt_kit.  

理论上来说这个工具和具体的数据库无关，但我并没有在更多的数据库上验证过。


## Install

Install robotslacker-sqlcli

    pip install -U robotslacker-sqlcli

## Usage
    # 直接登录SQLCli工具，登录后会进入到命令行窗口
    $> sqlcli
    SQL> 这里就进入这个工具了
    
    # 如何连接数据库
        # 连接数据库的第一步是加载驱动程序，这是jaydebeapi的要求
        SQL> load [jar filename] [driver class name]
        SQL> Driver loaded.
        # 例子
        SQL> load linkoopdb-jdbc-0.0.1.jar com.datapps.linkoopdb.jdbc.JdbcDriver
        
        # 连接数据库
        SQL> connect [User Name]/[Password]@jdbc:[driver type]:[protocol type]://[IP Address]:[Port]/[Service Name]
        SQL> Database Connected.
        # 例子
        SQL> connect admin/000000@jdbc:linkoopdb:tcp://127.0.0.1:9115/ldb
        
        # 重新连接数据库
        在数据库连接成功后，更换数据库用户，并不需要再次输入连接字符串，直接输入用户名和口令即可，比如：
        SQL> connect [User Name]/[Password]@jdbc:[driver type]:[protocol type]://[IP Address]:[Port]/[Service Name]
        SQL> Database Connected.
        SQL> connect [User Name]/[Password]
        SQL> Database Connected.
        
        # 环境变量的使用
        如果外部环境中定义了环境变量SQLCLI_CONNECTION_JAR_NAME和SQLCLI_CONNECTION_CLASS_NAME，则不再需要Load操作，比如
        setenv SQLCLI_CONNECTION_JAR_NAME [jar filename]
        setenv SQLCLI_CONNECTION_CLASS_NAME [driver class name]
        
        如果外部环境中定义了环境变量SQLCLI_CONNECTION_URL，则只需要输入用户名和口令，比如：
        setenv SQLCLI_CONNECTION_URL jdbc:[driver type]:[protocol type]://[IP Address]:[Port]/[Service Name]
        SQL> connect [User Name]/[Password]
        SQL> Database Connected.
     
    # 如何执行数据库脚本
    数据库脚本可以保存在sql文件中，通常sqlcli的参数来执行。
    $> sqlcli --execute <test.sql> --logfile <test.log>
    此时程序将会启动后自动读入test.sql文件，并将文件结果记录在test.log中
    
    # 显示格式的控制
        # SQL回显的控制
        在默认的情况下，执行的SQL并不会在LOG文件中体现，LOG文件中出现的只有SQL执行的结果
        通过set命令可以改变这个特性
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
                  
        # CLOB字段的输出
        在默认的情况下，CLOB字段的内容输出只有20个字符，通过set命令可以改变这个特性
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
    
    # SQL语句的解析规则
    一： 如果行内容为set，load，connect开头，则该语句为单行语句，不支持换行
    二： 如果行内容为create,drop,replace开头，且其后有function，procedure,则认为这一段为代码段
        代码段必须用/来结尾，比如:
        Create Procedure Proc_AAA As
        Begin
            bulabulabula ...
        End;
        /
    三： 如果行以create,drop,replace,select,update,delete,insert等开头，且不是存储过程的，则认为这一段为语句段
        语句段需要用；或者用/来结尾，比如：
         Create Table Test_Table 
         (
            COL1  CHAR(20),
            COL2  CHAR(30)
         );
         或者
         Create Table Test_Table 
         (
            COL1  CHAR(20),
            COL2  CHAR(30)
         )
         /
    三： 对于代码行中出现的--符号，本行内这个符号之后的内容将被认为是注释，不再处理
    四： 对于代码行中的/* fadfas */ 符号，认为这一段为注释内容，不再处理
        
