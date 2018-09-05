prompt PL/SQL Developer Export User Objects for user NCTEST@LINUX_ORCL
prompt Created by wangwei on 2018年9月3日
set define off
spool PKG_ETL_BASE.log


prompt
prompt Creating table ETL_TB_OBJECT_HIS
prompt ================================
prompt
create table ETL_TB_OBJECT_HIS
(
  object_name       VARCHAR2(30),
  object_type       VARCHAR2(30),
  create_time       VARCHAR2(30),
  change_type       VARCHAR2(30),
  object_script     CLOB,
  ts                CHAR(19) default to_char(sysdate,'yyyy-mm-dd hh24:mi:ss'),
  pk_etl_object_his CHAR(20) not null,
  oper_ip           VARCHAR2(30),
  oper_owner        VARCHAR2(30),
  obj_id            VARCHAR2(20),
  dr                NUMBER(10) default 0
)
tablespace NNC_DATA01
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );

prompt
prompt Creating table ETL_TB_PROCEDURE_LOG
prompt ===================================
prompt
create table ETL_TB_PROCEDURE_LOG
(
  object_id               VARCHAR2(200) not null,
  client_ip               VARCHAR2(100),
  client_proc             VARCHAR2(50),
  comments                VARCHAR2(1000),
  data_date               VARCHAR2(10),
  dr                      NUMBER(10) default 0,
  elapsed_time            NUMBER(38),
  end_time                CHAR(19),
  proc_name               VARCHAR2(200) not null,
  start_time              CHAR(19),
  status                  NUMBER(1),
  ts                      CHAR(19) default to_char(sysdate,'yyyy-mm-dd hh24:mi:ss'),
  tx_date                 CHAR(19),
  user_name               VARCHAR2(30),
  pk_etl_tb_procedure_log CHAR(20) not null
)
tablespace NNC_DATA01
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );
comment on table ETL_TB_PROCEDURE_LOG
  is '存储过程日志主表';
comment on column ETL_TB_PROCEDURE_LOG.object_id
  is '对象编码';
comment on column ETL_TB_PROCEDURE_LOG.client_ip
  is '客户端执行对象IP';
comment on column ETL_TB_PROCEDURE_LOG.comments
  is '备注';
comment on column ETL_TB_PROCEDURE_LOG.data_date
  is '数据日期';
comment on column ETL_TB_PROCEDURE_LOG.dr
  is '是否删除 1:删除；0:未删除';
comment on column ETL_TB_PROCEDURE_LOG.end_time
  is '结束时间';
comment on column ETL_TB_PROCEDURE_LOG.proc_name
  is '存储过程名称';
comment on column ETL_TB_PROCEDURE_LOG.status
  is '状态 1:异常；0:正常';
comment on column ETL_TB_PROCEDURE_LOG.ts
  is '时间戳';
comment on column ETL_TB_PROCEDURE_LOG.tx_date
  is '执行时间';
comment on column ETL_TB_PROCEDURE_LOG.user_name
  is '当前用户名';
alter table ETL_TB_PROCEDURE_LOG
  add constraint PK_TB_PROCEDURE_LOG primary key (PK_ETL_TB_PROCEDURE_LOG)
  using index 
  tablespace NNC_DATA01
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );

prompt
prompt Creating table ETL_TB_PROCEDURE_LOG_DTL
prompt =======================================
prompt
create table ETL_TB_PROCEDURE_LOG_DTL
(
  c_sql                       CLOB,
  comments                    VARCHAR2(1000),
  data_date                   VARCHAR2(10),
  deal_row                    NUMBER(38),
  dr                          NUMBER(10) default 0,
  elapsed_time                NUMBER(38),
  end_time                    CHAR(29),
  proc_name                   VARCHAR2(200),
  sql_code                    VARCHAR2(1000),
  sql_state                   VARCHAR2(1000),
  start_time                  CHAR(29),
  status                      NUMBER(10),
  ts                          CHAR(19) default to_char(sysdate,'yyyy-mm-dd hh24:mi:ss'),
  tx_date                     CHAR(19),
  object_id                   VARCHAR2(200) not null,
  pk_etl_tb_procedure_log     CHAR(20),
  pk_etl_tb_procedure_log_dtl CHAR(20) not null
)
tablespace NNC_DATA01
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );
comment on column ETL_TB_PROCEDURE_LOG_DTL.c_sql
  is '执行的脚本';
comment on column ETL_TB_PROCEDURE_LOG_DTL.dr
  is '是否删除';
comment on column ETL_TB_PROCEDURE_LOG_DTL.ts
  is '时间戳';
alter table ETL_TB_PROCEDURE_LOG_DTL
  add constraint PK_TB_PROCEDURE_LOG_DTL primary key (PK_ETL_TB_PROCEDURE_LOG_DTL)
  using index 
  tablespace NNC_DATA01
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );

prompt
prompt Creating sequence ETL_SEQ
prompt =========================
prompt
create sequence ETL_SEQ
minvalue 1
maxvalue 99999999999999999
start with 8116
increment by 1
nocache;

prompt
prompt Creating package PKG_ETL_BASE
prompt =============================
prompt
CREATE OR REPLACE PACKAGE PKG_ETL_BASE AS

  /***********
  *TYPE DEFINE
  *类型定义
  ************/
  /* 全局变量*/
  /*TYPE TYP_RECURSOR IS REF CURSOR; */
  TYPE TYP_TABLE IS TABLE OF VARCHAR2(32676);
  /*****************
  *FUNCTIONS DEFINE
  *函数定义
  *****************/
     --主键生成机制
  FUNCTION FN_GENERATEPK(CORP VARCHAR) RETURN VARCHAR;
  /**1.当前时间**/
  FUNCTION FN_GETCURRENTTIME  RETURN TIMESTAMP;
  /**2上一天日期**/
  FUNCTION FN_GETLASTDAY(P_INDATE IN VARCHAR2) RETURN VARCHAR2;
  /**3.上一期末日期。去年末一天,上季度末一天,上月末一天,上一天**/
  FUNCTION FN_GETLASTDAY(P_INDATE IN VARCHAR2,P_TYPE IN VARCHAR2) RETURN VARCHAR2;
           PRAGMA RESTRICT_REFERENCES(FN_GETLASTDAY,RNDS,RNPS,WNDS,WNPS);
  /**4.下一天日期**/
  FUNCTION FN_GETNEXTDAY(P_INDATE IN VARCHAR2) RETURN VARCHAR2;
  /**5下一期初日期。下年第一天,下季度第一天,下月第一天,下一天**/
  FUNCTION FN_GETNEXTDAY(P_INDATE IN VARCHAR2,P_TYPE IN VARCHAR2) RETURN VARCHAR2;
           PRAGMA RESTRICT_REFERENCES(FN_GETNEXTDAY,RNDS,RNPS,WNDS,WNPS);
  /**6.期间开始日期。月初日期**/
  FUNCTION FN_GETSTARTDAY(P_INDATE IN VARCHAR2) RETURN VARCHAR2;
  /**7.期间开始日期。年初,季度初,月初,当日**/
  FUNCTION FN_GETSTARTDAY(P_INDATE IN VARCHAR2,P_TYPE IN VARCHAR2) RETURN VARCHAR2;
           PRAGMA RESTRICT_REFERENCES(FN_GETSTARTDAY,RNDS,RNPS,WNDS,WNPS);
  /**8.期间结束日期。月末日期**/
  FUNCTION FN_GETCLOSEDAY(P_INDATE IN VARCHAR2) RETURN VARCHAR2;
  /**9.期间结束日期。年度最后一天,季度末一天,月末一天,当日**/
  FUNCTION FN_GETCLOSEDAY(P_INDATE IN VARCHAR2,P_TYPE IN VARCHAR2) RETURN VARCHAR2;
           PRAGMA RESTRICT_REFERENCES(FN_GETCLOSEDAY,RNDS,RNPS,WNDS,WNPS);
  /**10.同期。上年年末,上年季末,上年月末,上年同日**/
  FUNCTION FN_GETSAMEPERIOD(P_INDATE IN VARCHAR2,P_TYPE IN VARCHAR2) RETURN VARCHAR2;
           PRAGMA RESTRICT_REFERENCES(FN_GETSAMEPERIOD,RNDS,RNPS,WNDS,WNPS);
  /**11.到期初实际天数**/
  FUNCTION FN_GETBGPDDAYS(P_INDATE IN VARCHAR2,P_TYPE IN VARCHAR2) RETURN NUMBER;
           PRAGMA RESTRICT_REFERENCES(FN_GETBGPDDAYS,RNDS,RNPS,WNDS,WNPS);
  /**12.任意期间天数**/
  FUNCTION FN_GETDAYS(S_INDATE IN VARCHAR2,E_INDATE IN VARCHAR2) RETURN NUMBER;
           PRAGMA RESTRICT_REFERENCES(FN_GETDAYS,RNDS,RNPS,WNDS,WNPS);
  /**13.返回自定义天数之后日期**/
  FUNCTION FN_GETCUSDATE(P_INDATE IN VARCHAR2, P_DAY IN NUMBER) RETURN VARCHAR2;
  /**14.返回用分隔符号分隔的第几个字符串 **/
  FUNCTION FN_GET_STRLIST(P_STR_LIST IN CLOB, P_SPLIT IN VARCHAR2, P_STR_NUM IN NUMBER) RETURN VARCHAR2;
           PRAGMA RESTRICT_REFERENCES(FN_GET_STRLIST,RNPS,WNDS,WNPS);
  /**15.返回用两个时间的时间差（毫秒） **/
  FUNCTION FN_TIMESTAMP_CHA(ENDTIME   IN TIMESTAMP,STARTTIME IN TIMESTAMP) RETURN INTEGER ;
  /**16.返回字符串的sql语句是否执行成功，1执行错误，0执行成功 **/
  FUNCTION FN_SQLCHECK(P_cSql   IN CLOB)   RETURN INTEGER ;

  /**17.返回sql语句执行是否正确**/
  FUNCTION FN_DATE8TO10(i_date varchar2) return varchar2;

 /**18.返回sql语句执行是否正确**/
  FUNCTION FN_DATE10TO8(i_date varchar2) return varchar2;


  /*****************
  *PROCEDURE DEFINE
  *****************/

  /**1.记录主日志**/
  PROCEDURE SP_MAINLOGGING(P_OBJ_ID IN VARCHAR2,
                           P_PK_ETL_TB_PROCEDURE_LOG IN VARCHAR2,
                           P_USERNAM     IN VARCHAR2,
                           P_PROCNAM     IN VARCHAR2,
                           P_TXDATE      IN DATE,
                           P_DATADATE    IN VARCHAR2,
                           P_COMMENTS    IN VARCHAR2,
                           P_STARTTIME   IN DATE,
                           P_ENDTIME     IN DATE,
                           P_ELAPSEDTIME IN INTEGER,
                           P_CLIENTIP    IN VARCHAR2,
                           P_CLENTPROC   IN VARCHAR2,
                           P_STATUS      IN VARCHAR2
                      );
  /**2.记录明细日志**/
  PROCEDURE SP_DTLLOGGING(P_OBJ_ID     IN VARCHAR2,
                          P_PK_ETL_TB_PROCEDURE_LOG IN VARCHAR2,
                          P_PROCNAM     IN VARCHAR2,
                          P_TXDATE      IN DATE,
                          P_DATADATE    IN VARCHAR2,
                          P_COMMENTS    IN VARCHAR2,
                          P_SQLCODE     IN VARCHAR2,
                          P_SQLSTATE    IN VARCHAR2,
                          P_STATUS      IN VARCHAR2,
                          P_STARTTIME   IN TIMESTAMP,
                          P_ENDTIME     IN TIMESTAMP,
                          P_ELAPSEDTIME IN INTEGER,
                          P_DEALROW     IN INTEGER,
                          P_CSQL        IN CLOB
                      );
  /**3.清空指定表**/
  PROCEDURE SP_CLEARTABLES(P_USER IN VARCHAR2, P_TABLE IN VARCHAR2);
  /**4.删除主日志**/
  PROCEDURE SP_DELMAINLOG(P_INDATE IN VARCHAR2, P_JOBID IN VARCHAR2);
  /**5.删除索引,先判断是否存在**/
  PROCEDURE SP_DROPINDEX(P_USER IN VARCHAR2,P_TABLE IN VARCHAR2,P_INDEXNAME IN VARCHAR2);
  /**6.重建索引,先判断是否存在**/
  PROCEDURE SP_RECREATEINDEX(P_USER IN VARCHAR2, P_TABLE IN VARCHAR2, P_INDEXNAME IN VARCHAR2, P_COLUMN    IN VARCHAR2);
  /**7.删除分区,先判断是否存在**/
  PROCEDURE SP_DROPPARTITION(P_USER IN VARCHAR2, P_TABLE IN VARCHAR2, P_PARTITION IN VARCHAR2);
  /**8.增加分区,先判断是否存在**/
  PROCEDURE SP_ADDPARTITION(P_USER IN VARCHAR2, P_TABLE IN VARCHAR2, P_TYPE IN VARCHAR2, P_PARTITION IN VARCHAR2,P_TABSPACE IN VARCHAR2, P_VALUE IN VARCHAR2);
  /**9.查询用户名下所有表所有字段是否包含某个字符串**/
  PROCEDURE SP_FIND_STR_ALLTABLE(P_USER IN VARCHAR2 , P_STR IN VARCHAR2);
   /**10. 对DWETL用户下进行全表分析  **/
  PROCEDURE SP_ALL_ANALYZE_TABLE( P_TABLENAME IN VARCHAR2);
  /**11. 返回上一个工作日**/
  FUNCTION SP_GET_LASTWORKDATE(i_date varchar2 ) RETURN VARCHAR2;
  PROCEDURE SP_ETL_PKG_PRO_TEMPLATE(
     P_Creater IN VARCHAR2,--创建人
     P_cObjectId  IN VARCHAR2, --存储过程编码
     P_cProcName  IN VARCHAR2, --存储过程名称
     P_cComments  IN VARCHAR2, --存储过程描述
     P_cSourTable  IN VARCHAR2, --存储过程来源表名
     P_cTargTable  IN VARCHAR2, --存储过程目标表名
     vo_returnid out number);
END PKG_ETL_BASE;
/

prompt
prompt Creating package body PKG_ETL_BASE
prompt ==================================
prompt
CREATE OR REPLACE PACKAGE BODY PKG_ETL_BASE AS



  /***********
    *TYPE DEFINE
  ************/
  /*  TYPE RECURSOR IS REF CURSOR;*/

  /*************
  *FUNCTION
  **************/

     --主键生成机制
FUNCTION FN_GENERATEPK(corp varchar) return varchar as
 PRAGMA AUTONOMOUS_TRANSACTION;
  /*
       NC主键生成函数
   * 生成方法参考自中间件内部算法
   *输入参数： corp  公司主键
   * 输出参数：new_pk 新生成的pk
   ORA-14551: 无法在查询中执行 DML 操作 http://www.linuxidc.com/Linux/2013-06/86712.htm
   数据库事务是一种单元操作，要么是全部操作都成功，要么全部失败。在Oracle中，一个事务是从执行第一个数据管理语言（DML）语句开始，
   直到执行一个COMMIT语句，提交保存这个事务，或者执行一个ROLLBACK语句，放弃此次操作结束。事务的“要么全部完成，
   要么什么都没完成”的本性会使将错误信息记入数据库表中变得很困难，因为当事务失败重新运行时，用来编写日志条目的INSERT语句还未完成。
   针对这种困境，Oracle提供了一种便捷的方法，即自治事务。自治事务从当前事务开始，在其自身的语境中执行。它们能独立地被提交或重新运行，
   而不影响正在运行的事务。正因为这样，它们成了编写错误日志表格的理想形式。在事务中检测到错误时，您可以在错误日志表格中插入一行并提
   交它，然后在不丢失这次插入的情况下回滚主事务。因为自治事务是与主事务相分离的，所以它不能检测到被修改过的行的当前状态。这就好像在
   主事务提交之前，它们一直处于单独的会话里，对自治事务来说，它们是不可用的。然而，反过来情况就不同了：主事务能够检测到已经执行过的
   自治事务的结果。要创建一个自治事务，您必须在匿名块的最高层或者存储过程、函数、数据包或触发的定义部分中，使用PL/SQL中的
   PRAGMA AUTONOMOUS_TRANSACTION语句。在这样的模块或过程中执行的SQLServer语句都是自治的。触发无法包含COMMIT语句，
   除非有PRAGMA AUTONOMOUS_TRANSACTION标记。但是，只有触发中的语句才能被提交，主事务则不行。
  */
  new_oid     varchar(14); --the oid string
  old_oid     varchar(14); --the oid store in the pub_oid
  temp_oid    varchar(14);
  MINI_CODE   number(2);
  MAX_CODE    number(2);
  CODE_LENGTH number(2);
  tempchar    char(1);
  tempascii   number(2);
  carryup     boolean;
  global_count number(2);
  return_str varchar(20);
begin
  --初始化数据
  temp_oid :='';
  old_oid := '';
  new_oid := '10000000000000';
  CODE_LENGTH := 14;
  MINI_CODE := 48;
  MAX_CODE := 90;

  global_count := 14;
  --查询该公司下最大的pk
  select  pub_oid.idnumber
    into old_oid
    from pub_oid pub_oid
   where pub_oid.pk_corp = corp;
-- --DBMS_OUTPUT.put_line('old_oid=' || old_oid);
  --生成新的oid
  new_oid := '';
  FOR counter IN REVERSE 1..CODE_LENGTH LOOP
      carryup := false;
      global_count := counter;
      tempchar := substr(old_oid,counter,1);
      tempascii := ascii(tempchar)+1;

      if tempascii > MAX_CODE then
        tempascii := MINI_CODE;
        carryup := true;
      end if;

      if tempascii = 58 then
        tempascii := 65;
      end if;

      new_oid := new_oid||chr(tempascii);
      if carryup = false then
         -- 'ABCD' --> 'DCBA'
         for icounter in reverse 1..CODE_LENGTH-global_count+1 loop
             tempchar := substr(new_oid,icounter,1);
             temp_oid := temp_oid || tempchar;
         end loop;
         temp_oid := substr(old_oid,1,global_count-1)|| temp_oid;
         --变更临时oid为 new_oid
         new_oid := temp_oid;
         exit; -- 跳出循环
      end if;
  END LOOP;
  ----DBMS_OUTPUT.PUT_LINE('老主键：'||corp || 'AA' || old_oid);
  ----DBMS_OUTPUT.PUT_LINE('新主键：'||corp || 'AA' || new_oid);

  --update the new value 更新pk为新生成的pk
  update pub_oid set idnumber = new_oid where pk_corp = corp;
  return_str := corp || 'AA' || new_oid;
  commit;
  return return_str;
exception
  WHEN NO_DATA_FOUND THEN
  --INSERT THE NEW VALUE 插入新的pk
  insert into pub_oid
      (dr, idnumber, pk_corp)
    values
      (0, new_oid, corp);
    return_str := corp || 'AA' || new_oid;
    commit;
   return return_str;
  WHEN OTHERS THEN
    rollback;
    ----DBMS_OUTPUT.PUT_LINE('公司' || corp || '生成主键发生错误');
    return_str := corp || 'AA' || new_oid;
    return return_str;
end;

    /**1.当前时间
  TIME_TYPE=1：毫秒级 2：秒级
  V_CURRENTTIME=返回当前日期+时间（TIMESTAMP格式）
  **/
    FUNCTION FN_GETCURRENTTIME RETURN TIMESTAMP AS
    V_CURRENTTIME  TIMESTAMP ;
    BEGIN
      SELECT SYSTIMESTAMP INTO V_CURRENTTIME FROM DUAL;
      RETURN V_CURRENTTIME;
    END;

  /**2.上一天日期
  P_INDATE=传入日期,10位字符型
  V_LASTDAY=返回上一天
  **/

  FUNCTION FN_GETLASTDAY(P_INDATE IN VARCHAR2) RETURN VARCHAR2 AS
    V_TXDATE  DATE := TO_DATE(P_INDATE, 'YYYY-MM-DD');
    V_LASTDAY VARCHAR2(10);
  BEGIN
    V_LASTDAY := TO_CHAR(V_TXDATE - 1, 'YYYY-MM-DD');
    RETURN V_LASTDAY;
  END FN_GETLASTDAY;

  /**3.上一期末日期。去年末一天,上季度末一天,上月末一天,上一天
  P_INDATE=传入日期,10位字符型
  P_TYPE=期间类型 Y-年 Q-季度 M-月 D-日
  V_LASTDAY=返回上一期末日期
  **/
  FUNCTION FN_GETLASTDAY(P_INDATE IN VARCHAR2, P_TYPE IN VARCHAR2)
    RETURN VARCHAR2 AS
    V_TXDATE  DATE := TO_DATE(P_INDATE, 'YYYY-MM-DD');
    V_LASTDAY VARCHAR2(10);
  BEGIN
    IF P_TYPE = 'Y' THEN
      V_LASTDAY := TO_CHAR(ADD_MONTHS(V_TXDATE, -12), 'YYYY') || '-12-31';
    ELSIF P_TYPE = 'Q' THEN
      V_LASTDAY := TO_CHAR(TRUNC(V_TXDATE, 'Q') - 1, 'YYYY-MM-DD');
    ELSIF P_TYPE = 'M' THEN
      V_LASTDAY := TO_CHAR(LAST_DAY(ADD_MONTHS(V_TXDATE, -1)), 'YYYY-MM-DD');
    ELSE
      V_LASTDAY := TO_CHAR(V_TXDATE - 1, 'YYYY-MM-DD');
    END IF;
    RETURN V_LASTDAY;
  END FN_GETLASTDAY;

  /**4下一天日期
  P_INDATE=传入日期,10位字符型
  V_NEXTDAY=返回下一天日期
  **/
  FUNCTION FN_GETNEXTDAY(P_INDATE IN VARCHAR2) RETURN VARCHAR2 AS
    V_TXDATE  DATE := TO_DATE(P_INDATE, 'YYYY-MM-DD');
    V_NEXTDAY VARCHAR2(10);
  BEGIN
    V_NEXTDAY := TO_CHAR(V_TXDATE + 1, 'YYYY-MM-DD');
    RETURN V_NEXTDAY;
  END FN_GETNEXTDAY;

  /**5.下一期初日期。下年第一天,下季度第一天,下月第一天,下一天
  P_INDATE=传入日期,10位字符型
  P_TYPE=期间类型 Y-年 Q-季度 M-月 D-日
  V_NEXTDAY=返回下一期初日期
  **/
  FUNCTION FN_GETNEXTDAY(P_INDATE IN VARCHAR2, P_TYPE IN VARCHAR2)
    RETURN VARCHAR2 AS
    V_TXDATE  DATE := TO_DATE(P_INDATE, 'YYYY-MM-DD');
    V_NEXTDAY VARCHAR2(10);
  BEGIN
    IF P_TYPE = 'Y' THEN
      V_NEXTDAY := TO_CHAR(ADD_MONTHS(V_TXDATE, 12), 'YYYY') || '-01-01';
    ELSIF P_TYPE = 'Q' THEN
      V_NEXTDAY := TO_CHAR(ADD_MONTHS(TRUNC(V_TXDATE, 'Q'), 3), 'YYYY-MM-DD');
    ELSIF P_TYPE = 'M' THEN
      V_NEXTDAY := TO_CHAR(ADD_MONTHS(V_TXDATE, 1), 'YYYY-MM') || '-01';
    ELSE
      V_NEXTDAY := TO_CHAR(V_TXDATE + 1, 'YYYY-MM-DD');
    END IF;
    RETURN V_NEXTDAY;
  END FN_GETNEXTDAY;

  /**6.期间开始日期。月初日期
  P_INDATE=传入日期,10位字符型
  V_STARTDAY=返回当月第一天
  **/
  FUNCTION FN_GETSTARTDAY(P_INDATE IN VARCHAR2) RETURN VARCHAR2 AS
    V_STARTDAY VARCHAR2(10);
  BEGIN
    V_STARTDAY := SUBSTR(P_INDATE, 1, 7) || '-01';
    RETURN V_STARTDAY;
  END FN_GETSTARTDAY;

  /**7.期间开始日期。年初,季度初,月初,当日
  P_INDATE=传入日期,10位字符型
  P_TYPE=期间类型 Y-年 Q-季度 M-月 D-日
  V_STARTDAY=返回期初日期
  **/
  FUNCTION FN_GETSTARTDAY(P_INDATE IN VARCHAR2, P_TYPE IN VARCHAR2)
    RETURN VARCHAR2 AS
    V_TXDATE   DATE := TO_DATE(P_INDATE, 'YYYY-MM-DD');
    V_STARTDAY VARCHAR2(10);
  BEGIN
    IF P_TYPE = 'Y' THEN
      V_STARTDAY := TO_CHAR(TRUNC(V_TXDATE, 'Y'), 'YYYY-MM-DD');
    ELSIF P_TYPE = 'Q' THEN
      V_STARTDAY := TO_CHAR(TRUNC(V_TXDATE, 'Q'), 'YYYY-MM-DD');
    ELSIF P_TYPE = 'M' THEN
      V_STARTDAY := SUBSTR(P_INDATE, 1, 7) || '-01';
    ELSE
      V_STARTDAY := P_INDATE;
    END IF;
    RETURN V_STARTDAY;
  END FN_GETSTARTDAY;

  /**8.期间结束日期。月末日期
  P_INDATE=传入日期,10位字符型
  V_CLOSEDAY=返回月末日期
  **/
  FUNCTION FN_GETCLOSEDAY(P_INDATE IN VARCHAR2) RETURN VARCHAR2 AS
    V_TXDATE   DATE := TO_DATE(P_INDATE, 'YYYY-MM-DD');
    V_CLOSEDAY VARCHAR2(10);
  BEGIN
    V_CLOSEDAY := TO_CHAR(LAST_DAY(V_TXDATE), 'YYYY-MM-DD');
    RETURN V_CLOSEDAY;
  END FN_GETCLOSEDAY;

  /**9.期间结束日期。年度最后一天,季度末一天,月末一天,当日
  P_INDATE=传入日期,10位字符型
  P_TYPE=期间类型 Y-年 Q-季度 M-月 D-日
  V_CLOSEDAY=返回期末日期
  **/
  FUNCTION FN_GETCLOSEDAY(P_INDATE IN VARCHAR2, P_TYPE IN VARCHAR2)
    RETURN VARCHAR2 AS
    V_TXDATE   DATE := TO_DATE(P_INDATE, 'YYYY-MM-DD');
    V_CLOSEDAY VARCHAR2(10);
  BEGIN
    IF P_TYPE = 'Y' THEN
      V_CLOSEDAY := TO_CHAR(TRUNC(ADD_MONTHS(V_TXDATE, 12), 'Y') - 1,'YYYY-MM-DD');
    ELSIF P_TYPE = 'Q' THEN
      V_CLOSEDAY := TO_CHAR(TRUNC(ADD_MONTHS(V_TXDATE, 3), 'Q') - 1,'YYYY-MM-DD');
    ELSIF P_TYPE = 'M' THEN
      V_CLOSEDAY := TO_CHAR(LAST_DAY(V_TXDATE), 'YYYY-MM-DD');
    ELSE
      V_CLOSEDAY := P_INDATE;
    END IF;
    RETURN V_CLOSEDAY;
  END FN_GETCLOSEDAY;

/**10.上年同期日期。上年年末,上年季末,上年月末,上年同日
P_INDATE=传入日期,10位字符型
P_TYPE=期间类型 Y-上年年末 Q-上年季末 M-上年月末 D-上年同日
V_CLOSEDAY=返回上年同期日期
**/
FUNCTION FN_GETSAMEPERIOD(P_INDATE IN VARCHAR2, P_TYPE IN VARCHAR2)
  RETURN VARCHAR2 AS
  V_TXDATE DATE := TO_DATE(P_INDATE, 'YYYY-MM-DD');
  V_DAY    VARCHAR2(10);
BEGIN
  IF P_TYPE = 'Y' THEN
    V_DAY := TO_CHAR(ADD_MONTHS(V_TXDATE, -12), 'YYYY') || '-12-31';
  ELSIF P_TYPE = 'Q' THEN
    V_DAY := TO_CHAR(ADD_MONTHS(LAST_DAY(ADD_MONTHS(TRUNC(V_TXDATE, 'Q'), 2)), -12), 'YYYY-MM-DD');
  ELSIF P_TYPE = 'M' THEN
    V_DAY := TO_CHAR(LAST_DAY(ADD_MONTHS(V_TXDATE, -12)), 'YYYY-MM-DD');
  ELSIF P_TYPE = 'D' THEN
    V_DAY := TO_CHAR(ADD_MONTHS(V_TXDATE, -12), 'YYYY-MM-DD');
  ELSE
    V_DAY := P_INDATE;
  END IF;
  RETURN V_DAY;
END FN_GETSAMEPERIOD;

  /**11.到期初实际天数
  P_INDATE=传入日期,10位字符型
  P_TYPE=期间类型 Y-年 HY-半年 Q-季 M-月 W-周
  V_DAYS=返回实际天数
  **/
  FUNCTION FN_GETBGPDDAYS(P_INDATE IN VARCHAR2, P_TYPE IN VARCHAR2)
    RETURN NUMBER AS
    V_TXDATE DATE := TO_DATE(P_INDATE, 'YYYY-MM-DD');
    V_DAYS   NUMBER;
  BEGIN
    IF P_TYPE = 'Y' THEN
      V_DAYS := V_TXDATE - TRUNC(TO_DATE(P_INDATE, 'YYYY-MM-DD'), 'Y') + 1;
    ELSIF P_TYPE = 'HY' THEN
      IF SUBSTR(P_INDATE, 5, 4) <= '0630' THEN
        V_DAYS := V_TXDATE - TO_DATE(SUBSTR(P_INDATE, 1, 4) || '0101', 'YYYYMMDD') + 1;
      ELSE
        V_DAYS := V_TXDATE - TO_DATE(SUBSTR(P_INDATE, 1, 4) || '0701', 'YYYYMMDD') + 1;
      END IF;
    ELSIF P_TYPE = 'Q' THEN
      V_DAYS := V_TXDATE - TRUNC(TO_DATE(P_INDATE, 'YYYY-MM-DD'), 'Q') + 1;
    ELSIF P_TYPE = 'M' THEN
      V_DAYS := V_TXDATE - TO_DATE(SUBSTR(P_INDATE, 1, 7) || '01', 'YYYY-MM-DD') + 1;
    ELSIF P_TYPE = 'W' THEN
      V_DAYS := V_TXDATE - TRUNC(TO_DATE(P_INDATE, 'YYYY-MM-DD'), 'IW') + 1;
    END IF;
    RETURN V_DAYS;
  END FN_GETBGPDDAYS;

  /**12.任意期间天数
  S_INDATE=传入开始日期,10位字符型
  E_INDATE=传入结束日期,10位字符型
  V_DAYS=返回实际天数
  **/
  FUNCTION FN_GETDAYS(S_INDATE IN VARCHAR2, E_INDATE IN VARCHAR2)
    RETURN NUMBER AS
    VS_INDATE DATE := TO_DATE(S_INDATE, 'YYYY-MM-DD');
    VE_INDATE DATE := TO_DATE(E_INDATE, 'YYYY-MM-DD');
    V_DAYS    NUMBER;
  BEGIN
    V_DAYS := VE_INDATE - VS_INDATE + 1;
    RETURN V_DAYS;
  END FN_GETDAYS;
  /**13.返回自定义天数之后日期
  P_INDATE=传入开始日期,10位字符型
  P_DAY=传入天数
  V_INDATE=返回P_DAY之后日期
  **/
  FUNCTION FN_GETCUSDATE(P_INDATE IN VARCHAR2, P_DAY IN NUMBER)
    RETURN VARCHAR2 AS
    D_INDATE DATE := TO_DATE(P_INDATE, 'YYYY-MM-DD');
    E_INDATE DATE;
    V_INDATE VARCHAR2(20);
  BEGIN
    E_INDATE := D_INDATE + P_DAY;
    V_INDATE := TO_CHAR(E_INDATE,'YYYY-MM-DD');
    RETURN V_INDATE;
  END FN_GETCUSDATE;

    /**14.返回用分隔符号分隔的第几个字符串
  P_STR_LIST=传入字符串
  P_SPLIT=分隔符号
  P_STR_NUM=需要返回的第几个字符串
  V_STR=返回对应的字符串
  **/
  FUNCTION FN_GET_STRLIST(P_STR_LIST IN CLOB, P_SPLIT IN VARCHAR2, P_STR_NUM IN NUMBER)
  RETURN VARCHAR2 IS --STR_LIST 拼接的字符串，用分号隔开   STR_NUM 返回第几个值
    V_STR CLOB := '';
    n_length number :=0;
    Star_index number :=0;
    v_Count  number :=0;
  BEGIN
    --判断当前字符串有几个分隔符
     SELECT NVL(LENGTH(REGEXP_REPLACE(REPLACE(P_STR_LIST, P_SPLIT, '@'),  '[^@]+',  '')),0) INTO v_Count FROM DUAL;

    IF P_STR_NUM <= 0 OR P_STR_NUM >v_Count+1 THEN
       V_STR :='' ;--输入的第n个元素不存在
    ELSIF P_STR_NUM =1 AND v_Count = 0 THEN --只有一个元素的情况
      V_STR :=P_STR_LIST;
    ELSIF P_STR_NUM = v_Count+1 THEN ---取最后一个元素
      Star_index := INSTR(TO_CHAR(P_STR_LIST),P_SPLIT,1,P_STR_NUM-1) + LENGTH(P_SPLIT) ;
      V_STR := substr(TO_CHAR(P_STR_LIST),Star_index,90000);
    ELSIF P_STR_NUM = 1 and  v_Count>0 THEN ---第一个元素
      n_length := INSTR(TO_CHAR(P_STR_LIST),P_SPLIT,1,P_STR_NUM) - 1 ;
      V_STR := SUBSTR(TO_CHAR(P_STR_LIST),1,n_length);
    ELSE
      n_length := INSTR(TO_CHAR(P_STR_LIST),P_SPLIT,1,P_STR_NUM) -  INSTR(TO_CHAR(P_STR_LIST),P_SPLIT,1,P_STR_NUM-1) - 1;
      Star_index := INSTR(TO_CHAR(P_STR_LIST),P_SPLIT,1,P_STR_NUM-1) +1 ;
      V_STR := SUBSTR(TO_CHAR(P_STR_LIST),Star_index,n_length);
    END IF;
    RETURN V_STR;
  END FN_GET_STRLIST;

  /**15.返回用两个时间的时间差 （毫秒）
  ENDTIME=传入结束时间（TIMESTAMP）
  STARTTIME=传入开始时间（TIMESTAMP）
  返回（毫秒）
  **/
 FUNCTION FN_TIMESTAMP_CHA(ENDTIME   IN TIMESTAMP,
                                            STARTTIME IN TIMESTAMP)
 RETURN INTEGER
 AS
  STR      VARCHAR2(50);
  MISECOND INTEGER;
  SECONDS  INTEGER;
  MINUTES  INTEGER;
  HOURS    INTEGER;
  DAYS     INTEGER;
BEGIN
  STR := TO_CHAR(ENDTIME - STARTTIME);
  MISECOND := TO_NUMBER(SUBSTR(STR, INSTR(STR, ' ') + 10, 3));
  SECONDS := TO_NUMBER(SUBSTR(STR, INSTR(STR, ' ') + 7, 2));
  MINUTES := TO_NUMBER(SUBSTR(STR, INSTR(STR, ' ') + 4, 2));
  HOURS := TO_NUMBER(SUBSTR(STR, INSTR(STR, ' ') + 1, 2));
  DAYS := TO_NUMBER(SUBSTR(STR, 1, INSTR(STR, ' ')));

  RETURN DAYS * 24 * 60 * 60 * 1000 + HOURS * 60 * 60 * 1000 + MINUTES * 60 * 1000 + SECONDS * 1000 + MISECOND;
END;

 /**16.返回sql语句执行是否正确
  P_cSql=校验的sql语句
  返回1 执行错误 0 执行正确
  **/
 FUNCTION FN_SQLCHECK(P_cSql   IN CLOB)
 RETURN INTEGER
 AS
 BEGIN
    IF  P_cSql IS NULL THEN
      RETURN 0;
    END IF;
        --DBMS_OUTPUT.PUT_LINE(P_cSql);
        EXECUTE IMMEDIATE  P_cSql ;
        RETURN 0;
    EXCEPTION
      WHEN OTHERS THEN
      RETURN 1;
    RAISE;

 END;
/**17.返回sql语句执行是否正确
  i_date= 日期'yyyymmdd'
  date_convert = 返回日期'yyyy-mm-dd'
  'yyyymmdd' 转换为 'yyyy-mm-dd'
  **/
  function FN_DATE8TO10(i_date varchar2) return varchar2 is
    date_convert varchar2(20);
  begin
    date_convert:= to_char(to_date(i_date,'YYYYMMDD'),'YYYY-MM-YY');
    return date_convert;
  end;
  /**18.返回sql语句执行是否正确
  i_date=日期'yyyy-mm-dd'
  date_convert = 返回日期 'yyyymmdd'
  'yyyy-mm-dd' 转换为 'yyyymmdd'
  **/
    function FN_DATE10TO8(i_date varchar2) return varchar2 is
      date_convert varchar2(20);
  begin
    date_convert:= to_char(to_date(i_date,'YYYY-MM-DD'),'YYYYMMYY');
    return date_convert;
  end;



  /************
  *PROCEDURE
  *************/
  /**1.记录主日志
  P_OBJ_ID=过程编码
  P_USERNAM=用户
  P_PROCNAM=过程名
  P_TXDATE=执行日期
  P_DATADATE=数据日期（参数日期）
  P_COMMENTS=执行内容
  P_STARTTIME=开始时间
  P_ENDTIME=结束时间
  P_ELAPSEDTIME=总耗时（秒）
  P_CLIENTIP=执行该过程客户机IP
  P_CLENTPROC=执行该过程的客户端程序
  P_STATUS=状态:0-成功,1-失败
  **/
  PROCEDURE SP_MAINLOGGING(P_OBJ_ID IN VARCHAR2,
                           P_PK_ETL_TB_PROCEDURE_LOG IN VARCHAR2,
                           P_USERNAM     IN VARCHAR2,
                           P_PROCNAM     IN VARCHAR2,
                           P_TXDATE      IN DATE,
                           P_DATADATE    IN VARCHAR2,
                           P_COMMENTS    IN VARCHAR2,
                           P_STARTTIME   IN DATE,
                           P_ENDTIME     IN DATE,
                           P_ELAPSEDTIME IN INTEGER,
                           P_CLIENTIP    IN VARCHAR2,
                           P_CLENTPROC   IN VARCHAR2,
                           P_STATUS      IN VARCHAR2) AS
  BEGIN
    INSERT INTO ETL_TB_PROCEDURE_LOG
      (OBJECT_ID,
       PK_ETL_TB_PROCEDURE_LOG,
       USER_NAME,
       PROC_NAME,
       TX_DATE,
       DATA_DATE,
       COMMENTS,
       START_TIME,
       END_TIME,
       ELAPSED_TIME,
       CLIENT_IP,
       CLIENT_PROC,
       STATUS)
    VALUES
      (P_OBJ_ID,
       P_PK_ETL_TB_PROCEDURE_LOG,
       P_USERNAM,
       UPPER(P_PROCNAM),
       TO_CHAR(P_TXDATE,'YYYY-MM-DD HH24:MI:SS'),
       P_DATADATE,
       P_COMMENTS,
       TO_CHAR(P_STARTTIME,'YYYY-MM-DD HH24:MI:SS'),
       TO_CHAR(P_ENDTIME,'YYYY-MM-DD HH24:MI:SS'),
       P_ELAPSEDTIME,
       P_CLIENTIP,
       P_CLENTPROC,
       P_STATUS);
    COMMIT;
  END SP_MAINLOGGING;

  /**2.记录明细日志
  P_OBJ_ID=过程编码
  P_PROCNAM=过程名
  P_TXDATE=执行日期
  P_DATADATE=数据日期（参数日期）
  P_COMMENTS=执行内容
  P_SQLCODE=执行结果代码
  P_SQLSTATE=执行结果说明
  P_STATUS=状态:0-成功,1-失败
  P_STARTTIME=开始时间
  P_ENDTIME=结束时间
  P_ELAPSEDTIME=总耗时（秒）
  P_DEALROW= 处理记录数
  **/
  PROCEDURE SP_DTLLOGGING(
                          P_OBJ_ID     IN VARCHAR2,
                          P_PK_ETL_TB_PROCEDURE_LOG  IN VARCHAR2,
                          P_PROCNAM     IN VARCHAR2,
                          P_TXDATE      IN DATE,
                          P_DATADATE    IN VARCHAR2,
                          P_COMMENTS    IN VARCHAR2,
                          P_SQLCODE     IN VARCHAR2,
                          P_SQLSTATE    IN VARCHAR2,
                          P_STATUS      IN VARCHAR2,
                          P_STARTTIME   IN TIMESTAMP,
                          P_ENDTIME     IN TIMESTAMP ,
                          P_ELAPSEDTIME IN INTEGER,
                          P_DEALROW     IN INTEGER,
                          P_CSQL        IN CLOB) AS
  BEGIN
    INSERT INTO ETL_TB_PROCEDURE_LOG_DTL
      (OBJECT_ID ,
       PK_ETL_TB_PROCEDURE_LOG ,
       PK_ETL_TB_PROCEDURE_LOG_DTL ,
       PROC_NAME,
       TX_DATE,
       DATA_DATE,
       COMMENTS,
       SQL_CODE,
       SQL_STATE,
       STATUS,
       START_TIME,
       END_TIME,
       ELAPSED_TIME,
       DEAL_ROW,
       C_SQL)
    VALUES
      (P_OBJ_ID ,
       P_PK_ETL_TB_PROCEDURE_LOG ,
       lpad(etl_seq.nextval,20,'0') ,
       P_PROCNAM,
       TO_CHAR(P_TXDATE,'YYYY-MM-DD HH24:MI:SS'),
       P_DATADATE,
       P_COMMENTS,
       P_SQLCODE,
       P_SQLSTATE,
       P_STATUS,
       TO_CHAR(P_STARTTIME,'YYYY-MM-DD HH24:MI:SSxff'),
       TO_CHAR(P_ENDTIME,'YYYY-MM-DD HH24:MI:SSxff'),
       P_ELAPSEDTIME,
       P_DEALROW,
       P_CSQL);
    COMMIT;

  END SP_DTLLOGGING;

  /**3.清空指定表
  P_USER=用户名
  P_TABLE=表名
  **/
  PROCEDURE SP_CLEARTABLES(P_USER IN VARCHAR2, P_TABLE IN VARCHAR2) AS
  BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || P_USER || '.' || P_TABLE;
  END SP_CLEARTABLES;

  /**4.删除主日志
  P_INDATE=数据日期（参数日期）
  P_JOBNAME=过程名
  **/
  PROCEDURE SP_DELMAINLOG(P_INDATE IN VARCHAR2, P_JOBID IN VARCHAR2) AS
  BEGIN
    DELETE FROM ETL_TB_PROCEDURE_LOG
     WHERE DATA_DATE = P_INDATE
       AND OBJECT_ID = UPPER(P_JOBID) /*AND STATUS<>'9-ERROR'*/
    ;
    COMMIT;
  END SP_DELMAINLOG;

  /**5.删除索引,先判断是否存在
  P_USER=用户名
  P_TABLE=表名
  P_INDEXNAME=索引名
  **/
  PROCEDURE SP_DROPINDEX(P_USER      IN VARCHAR2,
                         P_TABLE     IN VARCHAR2,
                         P_INDEXNAME IN VARCHAR2) AS
    V_COUNT INTEGER;
  BEGIN
    SELECT COUNT(*)
      INTO V_COUNT
      FROM ALL_INDEXES
     WHERE OWNER = UPPER(P_USER)
       AND TABLE_NAME = UPPER(P_TABLE)
       AND INDEX_NAME = UPPER(P_INDEXNAME);
    IF V_COUNT > 0 THEN
      EXECUTE IMMEDIATE 'DROP INDEX ' || P_USER || '.' || P_INDEXNAME;
    END IF;
  END SP_DROPINDEX;

  /**6.重建索引,先判断是否存在
  P_USER=用户名
  P_TABLE=表名
  P_INDEXNAME=索引名
  P_COLUMN=列名
  **/
  PROCEDURE SP_RECREATEINDEX(P_USER      IN VARCHAR2,
                             P_TABLE     IN VARCHAR2,
                             P_INDEXNAME IN VARCHAR2,
                             P_COLUMN    IN VARCHAR2) AS
    V_COUNT INTEGER;
  BEGIN
    SELECT COUNT(*)
      INTO V_COUNT
      FROM ALL_INDEXES
     WHERE OWNER = UPPER(P_USER)
       AND TABLE_NAME = UPPER(P_TABLE)
       AND INDEX_NAME = UPPER(P_INDEXNAME);
    IF V_COUNT > 0 THEN
      EXECUTE IMMEDIATE 'DROP INDEX ' || P_USER || '.' || P_INDEXNAME;
    END IF;
    EXECUTE IMMEDIATE 'CREATE INDEX ' || P_USER || '.' || P_INDEXNAME ||' ON ' || P_USER || '.' || P_TABLE || '(' || P_COLUMN ||') TABLESPACE DW_INDEX NOLOGGING PARAllEL 2';
  END SP_RECREATEINDEX;

  /**7.删除分区,先判断是否存在
   需要给用户赋权限以访问DBA_TAB_PARTITIONS
   grant select on dba_tab_partitions to user_name
   P_USER=用户名
   P_TABLE=表名
   P_PARTITION=分区名
  **/
  PROCEDURE SP_DROPPARTITION(P_USER      IN VARCHAR2,
                             P_TABLE     IN VARCHAR2,
                             P_PARTITION IN VARCHAR2) AS
    V_COUNT INTEGER;
  BEGIN
    SELECT COUNT(*)
      INTO V_COUNT
      FROM DBA_TAB_PARTITIONS
     WHERE TABLE_OWNER = UPPER(P_USER)
       AND TABLE_NAME = UPPER(P_TABLE) ;

    IF V_COUNT <= 0 THEN
      RETURN ;
    ELSE
      SELECT COUNT(*)
        INTO V_COUNT
        FROM DBA_TAB_PARTITIONS
       WHERE TABLE_OWNER = UPPER(P_USER)
         AND TABLE_NAME = UPPER(P_TABLE)
         AND PARTITION_NAME = UPPER(P_PARTITION);
      IF V_COUNT > 0 THEN
      EXECUTE IMMEDIATE 'ALTER TABLE ' || P_USER || '.' || P_TABLE ||' DROP PARTITION ' || P_PARTITION;
      END IF;
    END IF;

  END SP_DROPPARTITION;

  /**8.增加分区,先判断是否存在
   P_USER=用户名
   P_TABLE=表名
   P_TYPE=分区类型,L:list分区;R:range分区。
   P_PARTITION=分区名
   P_VALUE=分区值
  **/
  PROCEDURE SP_ADDPARTITION(P_USER      IN VARCHAR2,
                            P_TABLE     IN VARCHAR2,
                            P_TYPE      IN VARCHAR2,
                            P_PARTITION IN VARCHAR2,
                            P_TABSPACE  IN VARCHAR2 ,
                            P_VALUE     IN VARCHAR2) AS
    V_COUNT INTEGER;
    V_SQL   VARCHAR2(1000);
    V_TABSPACE VARCHAR2(1000) :=NVL(P_TABSPACE,'NNC_DATA01');
    V_USER VARCHAR2(10) :=P_USER;
  BEGIN

    SELECT COUNT(*)
      INTO V_COUNT
      FROM DBA_TAB_PARTITIONS
     WHERE TABLE_OWNER = UPPER(V_USER)
       AND TABLE_NAME = UPPER(P_TABLE);
    IF V_COUNT <= 0 THEN
      RETURN ;
    ELSE
      SELECT COUNT(*)
        INTO V_COUNT
        FROM DBA_TAB_PARTITIONS
       WHERE TABLE_OWNER = UPPER(V_USER)
         AND TABLE_NAME = UPPER(P_TABLE)
         AND PARTITION_NAME = UPPER(P_PARTITION);
      IF V_COUNT <= 0 THEN
        IF P_TYPE = 'L' THEN
          V_SQL := 'ALTER TABLE ' || V_USER || '.' || P_TABLE ||' ADD PARTITION ' || P_PARTITION || ' VALUES (''' || P_VALUE || ''') TABLESPACE ' || V_TABSPACE ;
          EXECUTE IMMEDIATE V_SQL;
        ELSIF P_TYPE = 'R' THEN
          V_SQL := 'ALTER TABLE ' || V_USER || '.' || P_TABLE ||  ' ADD PARTITION ' || P_PARTITION || ' VALUES LESS THAN (''' || P_VALUE || ''') TABLESPACE '|| V_TABSPACE ;
          EXECUTE IMMEDIATE V_SQL;
        END IF;
      END IF;
    END IF;

  END SP_ADDPARTITION;

  /**9.查询用户名下所有表所有字段是否包含某个字符串
   P_USER=用户名
   P_Str=查询的字符串
   返回结果表 ETL_TB_FIND_STR_ALLTABLE
   grant select on DBA_TAB_COLUMNS to dwetl;
   grant select on DBA_TABLES to dwetl;
   grant create table to dwetl;
  **/
   PROCEDURE SP_FIND_STR_ALLTABLE
        (
        P_USER IN VARCHAR2 , --查询的用户名
        P_STR IN VARCHAR2 --查询的字符串
        ) IS
    V_RE_SQL  INT;
    V_SQLSTR VARCHAR2(2000):='';
    V_SQLTABLENAME  VARCHAR2(200):='';
    V_SQLCOLUMN  VARCHAR2(200):='';
    V_STR_TEMP   VARCHAR2(200):='';
    V_DB_USER   VARCHAR2(50):='';
    TYPE REF_CURSOR_TYPE IS REF CURSOR;  --定义一个动态游标
    COLUMN_NAME REF_CURSOR_TYPE;
    TABLE_NAME REF_CURSOR_TYPE;
  BEGIN
        SELECT COUNT(1) INTO V_RE_SQL FROM USER_TABLES WHERE TABLE_NAME = 'ETL_TB_FIND_STR_ALLTABLE';
        IF V_RE_SQL=0 THEN --创建ETL_TB_FIND_STR_ALLTABLE
              V_SQLSTR := '
                       CREATE TABLE ETL_TB_FIND_STR_ALLTABLE
                       (
                       TABLENAME VARCHAR2(30),
                       COLUMN_NAME VARCHAR2(30)
                       )
              ';
              EXECUTE IMMEDIATE V_SQLSTR;
          ELSE --清空表ETL_TB_FIND_STR_ALLTABLE
            EXECUTE IMMEDIATE 'TRUNCATE TABLE ETL_TB_FIND_STR_ALLTABLE';
        END IF;

        COMMIT;
        V_DB_USER:=UPPER(P_USER);
        V_SQLSTR := 'SELECT TABLE_NAME  FROM DBA_TABLES A WHERE A.OWNER=''' || V_DB_USER ||  '''  AND TABLESPACE_NAME IS NOT NULL  AND TABLE_NAME <> ''ETL_TB_FIND_STR_ALLTABLE''';
         OPEN TABLE_NAME FOR V_SQLSTR ;
              LOOP
                   FETCH TABLE_NAME INTO V_SQLTABLENAME;
                   EXIT WHEN TABLE_NAME%NOTFOUND;
                   V_SQLCOLUMN:='SELECT  COLUMN_NAME  FROM DBA_TAB_COLUMNS    A  WHERE TABLE_NAME =''' || V_SQLTABLENAME || ''' AND a.OWNER=''' || V_DB_USER ||  '''  AND A.DATA_TYPE NOT IN ( ''CLOB'',''BLOB'')  ';

                           OPEN COLUMN_NAME FOR V_SQLCOLUMN ;
                                    LOOP
                                        FETCH COLUMN_NAME INTO V_STR_TEMP;
                                        EXIT WHEN COLUMN_NAME%NOTFOUND;
                                        V_SQLSTR := 'SELECT COUNT(' || V_STR_TEMP || ')   FROM  ' || V_DB_USER || '.' || V_SQLTABLENAME || ' WHERE ' || V_STR_TEMP || ' LIKE ''%' || P_STR || '%''';
                                        EXECUTE IMMEDIATE V_SQLSTR INTO V_RE_SQL;
                                        COMMIT;
                                        IF V_RE_SQL>1 THEN
                                          V_SQLSTR:= 'INSERT INTO ETL_TB_FIND_STR_ALLTABLE VALUES (''' || V_SQLTABLENAME || ''' ,''' ||  V_STR_TEMP || ''')';
                                          EXECUTE IMMEDIATE V_SQLSTR ;
                                          COMMIT;
                                        END IF ;
                                    END LOOP;
                                    CLOSE COLUMN_NAME;
                            COMMIT;
        END LOOP;
        CLOSE TABLE_NAME;
   END  ;

  /**10. 对DWETL用户下进行全表分析
   P_TABLENAME=表名
   对表进行全表分析
  **/
   PROCEDURE SP_ALL_ANALYZE_TABLE
        (
        P_TABLENAME IN VARCHAR2 --查询的用户名
        ) IS
        V_SQLSTR VARCHAR2(2000):='';
   BEGIN
     V_SQLSTR := 'ANALYZE TABLE ' || UPPER(P_TABLENAME) || ' COMPUTE STATISTICS FOR TABLE FOR ALL INDEXED COLUMNS';
     EXECUTE IMMEDIATE V_SQLSTR ;
   END ;
  /**11  返回上一个工作日
  i_date = 当前日期
  **/

  FUNCTION SP_GET_LASTWORKDATE(i_date varchar2 )
    return varchar2 is
    v_ret_date varchar2(10) :=i_date ;
    v_count     int := 0 ;
    v_Str_sql varchar2(2000);
  begin
   WHILE  v_count = 0  LOOP  --如果返回0则表明为休息日 继续判断上一天   返回1 代表为工作日

    v_ret_date := TO_CHAR(TO_DATE(v_ret_date,'yyyy-mm-dd') -1 ,'yyyy-mm-dd');
    --以下为NC57版本工作日历的处理逻辑，不同项目按照不同情况进行修改
    --默认取上海交易所的工作日历
    v_Str_sql := 'SELECT COUNT(1) FROM  sxzq.sec_tradecalender  WHERE TRADEDATE = ''' || v_ret_date || ''' and ISTRADE = ''Y'' and PK_BOURSESET = ''0001A110000000000228'' ';

    EXECUTE IMMEDIATE TO_CHAR(v_Str_sql) into v_count  ;
    END LOOP;

    return v_ret_date;

  END;
/*创建存储过程模板*/
PROCEDURE SP_ETL_PKG_PRO_TEMPLATE(
     P_Creater IN VARCHAR2,--创建人
     P_cObjectId  IN VARCHAR2, --存储过程编码
     P_cProcName  IN VARCHAR2, --存储过程名称
     P_cComments  IN VARCHAR2, --存储过程描述
     P_cSourTable  IN VARCHAR2, --存储过程来源表名
     P_cTargTable  IN VARCHAR2, --存储过程目标表名
     vo_returnid out number)
     IS
           /*===============================================================+
      ----------------------存储过程信息-------------------------------
          功能描述: 存储过程生成
          参数描述:
          创建日期: 2017-02-15
          创建人  : 王伟
          版本号  : V1.0
          加载策略:
          代码工具(DB version):ORACLE11g(11.2.0.1.0)
          修改历史:
          修改日期    版本号    修改人              修改说明

      +===============================================================*/

      ----公共变量(必须定义)-------------------------------------------
        v_cObjectId      VARCHAR2(30) :=P_cObjectId;             --对象编码
        v_cProcName   VARCHAR2(50)  := P_cProcName ;          --存储过程名称;
        v_cComments   VARCHAR2(1000):=P_cComments ;        --存储过程中描述
        v_cSourTable    VARCHAR2(500)  := P_cSourTable ;          --来源表
        v_cTargTable    VARCHAR2(500)  := P_cTargTable ;          --目标表

      ----自定义变量(变量命名规则为 字符型：v_c*; 数字型v_n*; 日期型v_d*; *:后缀每一个英文字母大写,其余字母小写)----
        v_tSysdate        VARCHAR2(10)  := '' ;
        v_cExists    NUMBER := '';                     --获取是否存在要创建的存储过程
        v_cSql             CLOB := '';                     --获取执行SQL
      ----主程序体-----------------------------------------------------
      BEGIN --Main
               SELECT TO_CHAR(SYSDATE,'YYYY-MM-DD') INTO v_tSysdate FROM DUAL;


           --判断存储过程是否存在
                 SELECT COUNT(1)
                 INTO v_cExists
                 FROM USER_OBJECTS
                 WHERE OBJECT_TYPE='PROCEDURE' AND  OBJECT_NAME = v_cProcName ;
                 IF v_cExists >= 1 THEN
                    vo_returnid := 1;
                    RETURN;
                 END IF;

                 v_cSql :='
CREATE OR REPLACE PROCEDURE ' || v_cProcName || '
(
    vi_cDataDate IN VARCHAR2 ,  --输入参数,10位日期
    vo_cSqlCode  OUT NUMBER     --输出参数,返回代码
) IS
/*===============================================================+
----------------------存储过程信息-------------------------------
    功能描述: 存储过程开发模板模板
    参数描述: 输入参数:vi_cDataDate值为YYYY-MM-DD类型日期,10位;
              输出参数:vo_cSqlCode值为返回代码;
    创建日期: ' || v_tSysdate || '
    创建人  : ' || P_Creater || '
    版本号  : V1.0
    加载策略:
    目标表  : ' ||  v_cTargTable || '
    源表    : ' ||  v_cSourTable  || '

    代码工具(DB version):ORACLE11g(11.2.0.1.0)
    修改历史:
    修改日期    版本号    修改人              修改说明

+===============================================================*/

----公共变量(必须定义)-------------------------------------------
  v_cObjectId      VARCHAR2(100) :=''' || v_cObjectId || ''';             --对象编码
  v_cUserName   VARCHAR2(50)  := '''' ;          --登陆数据库执行存储过程的用户
  v_cProcName   VARCHAR2(200)  := ''' || v_cProcName || ''' ;          --存储过程名称;@需要手动填写@
  v_dTxDate     DATE          := SYSDATE ;       --本期数据处理日期
  v_cComments   VARCHAR2(1000):= '''' ;           --存储过程中事务处理结果描述
  v_tStime      TIMESTAMP          := SYSDATE ;      --程序中事务开始时间
  v_tEtime      TIMESTAMP          := SYSDATE ;      --程序中事务结束时间
  v_nDealRow    INT           := 0 ;            --事务处理行数
  v_cCliIP      VARCHAR2(100) :='''' ;            --客户端IP
  v_cCliProcNam VARCHAR2(100) :='''' ;            --客户端程序名称

  v_cProcDesc   VARCHAR2(200) := ''' || v_cComments || '''  ;   --存储过程功能描述;@需要手动填写@
  v_cTranDesc   VARCHAR2(1000):= '''' ;           --存储过程中各事务描述;
  v_cSqlMsg     VARCHAR2(500) := '''' ;           --事务处理过程中数据库返回的信息
  v_tStime0     TIMESTAMP          := SYSDATE ;      --JOB的开始时间

  v_cMaxDate    VARCHAR2(10)  :=''3000-12-31'';   --最大日期
  v_cSql          CLOB := '''';                     --获取执行SQL
  v_cSqlLog     CLOB := '''';                     --获取执行SQL相关日志
  v_pk_log   VARCHAR2(20) := LPAD(ETL_SEQ.NEXTVAL,20,''0''); --获取执行当前存储过程的20位主键

----自定义变量(变量命名规则为 字符型：v_c*; 数字型v_n*; 日期型v_d*; *:后缀每一个英文字母大写,其余字母小写)----
  v_cParms      CLOB := '''';                     --获取输入参数
  v_cTest         VARCHAR2(1000);         --测试参数，可以删除
----主程序体-----------------------------------------------------
BEGIN --Main


----#####################################################################################---
----#########!!!!!!!!!!!!!!!初始化信息start ，禁止修改！!!!!!!!!!!!!!!!!########-
----###############################---
    vo_cSqlCode := 0;                         --vo_cSqlCode赋初始值
    v_tStime0 := PKG_ETL_BASE.FN_GETCURRENTTIME;--取主体开始时间
    --删除主日志中该JOB的当日记录
    BEGIN
        PKG_ETL_BASE.SP_DELMAINLOG(vi_cDataDate,v_cObjectId);
    --错误信息
    EXCEPTION
    WHEN OTHERS THEN
        vo_cSqlCode := SQLCODE;
        v_cSqlMsg := SQLERRM;
        ROLLBACK;
        RAISE;
    END;

    --获取客户端IP和客户端程序名称以及数据库用户; 要访问视图V$SESSION需授权: GRANT SELECT ON V$SESSION TO USER
    v_tStime := PKG_ETL_BASE.FN_GETCURRENTTIME;--获取事务开始日期
    BEGIN
        SELECT
            SYS.LOGIN_USER  数据库用户,
            NVL(SYS_CONTEXT(''USERENV'', ''IP_ADDRESS''),''NOIP'') 登录IP,
            NVL(PROGRAM,''NOPROC'') 应用程序
            INTO v_cUserName,v_cCliIP,v_cCliProcNam
        FROM V$SESSION
        WHERE AUDSID = USERENV(''SESSIONID'');

        --记录本事务日志
        vo_cSqlCode:=SQLCODE;
        v_cSqlMsg:=SQLERRM;
        v_cComments:=''获取IP、用户、客户端程序成功'';
        v_nDealRow:= SQL%ROWCOUNT;

    EXCEPTION
    WHEN OTHERS THEN
        vo_cSqlCode:=SQLCODE;
        v_cSqlMsg:=SQLERRM;
        v_cComments:=''获取IP、用户、客户端程序出错'';
        v_nDealRow:= SQL%ROWCOUNT;
        ROLLBACK;

        --获取事务结束时间
        v_tEtime := PKG_ETL_BASE.FN_GETCURRENTTIME;
        PKG_ETL_BASE.SP_DTLLOGGING(v_cObjectId,v_pk_log,v_cProcName,v_dTxDate,vi_cDataDate,v_cComments,vo_cSqlCode,v_cSqlMsg,''1'',v_tStime,v_tEtime,PKG_ETL_BASE.FN_TIMESTAMP_CHA(v_tEtime,v_tStime),v_nDealRow,v_cSqlLog);
        RAISE;
    END;
----####################---
----##########!!!!!!!!!!!!!!!!初始化信息end   ，禁止修改！!!!!!!!!!!!!!!!!!!!!######
----#####################################################################################---




----##########业务处理节点模块start (如果有多个业务处理节点，则每个节点都按如下内容增加) ##########----

        -----============================================
        ------==============STEP1 START=====================
        ------===========================
    BEGIN
        v_tStime := PKG_ETL_BASE.FN_GETCURRENTTIME;--事务开始日期
        --设置业务处理变量
        v_cTranDesc :=''STEP1:示例,逻辑处理第一步''; --存储过程中事务的功能描述;@需要手动填写@
        v_cParms :=''输入参数
        vi_cDataDate: ''||vi_cDataDate||''
        '';

        ----以下内容为事务处理sql----
        ----存储过程执行语句采用动态SQL执行，日志需要打印当前执行的sql
        v_cSql:=''
        SELECT SYSDATE || :vi_cDataDate FROM DUAL
        '';
        --执行sql日志
        v_cSqlLog :=v_cParms||CHR(10)||''
        执行sql:''||v_cSql||''
        '';
        --DBMS_OUTPUT.PUT_LINE(v_cSql);
        --执行sql
        EXECUTE IMMEDIATE TO_CHAR(v_cSql) INTO v_cTest USING vi_cDataDate ;

        --记录节点运行日志
        vo_cSqlCode := SQLCODE;
        v_cSqlMsg := SQLERRM || '','' || dbms_utility.format_error_backtrace;
        v_nDealRow:= SQL%ROWCOUNT;
        v_cComments := v_cTranDesc||'' 处理成功'';
        v_tEtime := PKG_ETL_BASE.FN_GETCURRENTTIME;
        PKG_ETL_BASE.SP_DTLLOGGING(v_cObjectId,v_pk_log,v_cProcName,v_dTxDate,vi_cDataDate,v_cComments,vo_cSqlCode,v_cSqlMsg,''0'',v_tStime,v_tEtime,PKG_ETL_BASE.FN_TIMESTAMP_CHA(v_tEtime,v_tStime),v_nDealRow,v_cSqlLog);
        COMMIT;

    --节点错误信息
    EXCEPTION
    WHEN OTHERS THEN
        vo_cSqlCode := SQLCODE;
        v_cSqlMsg := SQLERRM || '','' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
        v_cComments := v_cTranDesc||'' 处理失败'';
        v_tEtime := PKG_ETL_BASE.FN_GETCURRENTTIME;--事务结束时间
        ROLLBACK;
        PKG_ETL_BASE.SP_DTLLOGGING(v_cObjectId,v_pk_log,v_cProcName,v_dTxDate,vi_cDataDate,v_cComments,vo_cSqlCode,v_cSqlMsg,''1'',v_tStime,v_tEtime,PKG_ETL_BASE.FN_TIMESTAMP_CHA(v_tEtime,v_tStime),v_nDealRow,v_cSqlLog);
        RAISE;
    END;
    ------==========================
    ------===============STEP1 END===============================
    ------=====================================================

----##########业务处理节点模块end  (如果有多个业务处理节点，则每个节点都按以上内容增加) ##########----






----########################################################################################---
----#####!!!!!!!!!!!!!!!!!!!!!!!!!主日志处理start ,禁止修改！!!!!!!!!!!!!!!!!!!!!!!!!!!####----
----#############################---
--插入主体日志信息

v_cComments:= v_cProcDesc||'' 处理成功'';
v_tEtime := PKG_ETL_BASE.FN_GETCURRENTTIME;--取主体结束时间
PKG_ETL_BASE.SP_MAINLOGGING(v_cObjectId,v_pk_log,SYS.LOGIN_USER,v_cProcName,v_dTxDate,vi_cDataDate,v_cComments,v_tStime0,v_tEtime,PKG_ETL_BASE.FN_TIMESTAMP_CHA(v_tEtime,v_tStime),v_cCliIP,v_cCliProcNam,''0'');


--主体错误信息处理
EXCEPTION
WHEN OTHERS THEN
    vo_cSqlCode := SQLCODE;
    v_cSqlMsg := SQLERRM;
    v_cComments:= v_cProcDesc||'' 处理失败'';
    ROLLBACK;
    v_tEtime := PKG_ETL_BASE.FN_GETCURRENTTIME;--取主体结束时间
    PKG_ETL_BASE.SP_MAINLOGGING(v_cObjectId,v_pk_log,SYS.LOGIN_USER,v_cProcName,v_dTxDate,vi_cDataDate,v_cComments,v_tStime0,v_tEtime,PKG_ETL_BASE.FN_TIMESTAMP_CHA(v_tEtime,v_tStime),v_cCliIP,v_cCliProcNam,''1'');
    --RAISE_application_error(vo_cSqlCode,v_cSqlMsg);
    RAISE;

----######################---
----#####!!!!!!!!!!!!!!!!!!!!!!!!!主日志处理end   ,禁止修改！!!!!!!!!!!!!!!!!!!!!!!!!!!#####--
----############################################################################################---
END;
                 ';
                 --DBMS_OUTPUT.PUT_LINE(v_cSql);
                     EXECUTE IMMEDIATE  v_cSql ;
                     vo_returnid :=0;
    --主体错误信息处理
    EXCEPTION
    WHEN OTHERS THEN
      vo_returnid :=1;
      ROLLBACK;
       RAISE;

    END;

END PKG_ETL_BASE;
/

prompt
prompt Creating trigger ETL_TR_OBJECT_HIS
prompt ==================================
prompt
CREATE OR REPLACE TRIGGER "ETL_TR_OBJECT_HIS"
  AFTER DDL ON  SCHEMA
  /*
  创建需要建立表：ETL_TB_OBJECT_HIS
        create table ETL_TB_OBJECT_HIS
(
  object_name       VARCHAR2(30),
  object_type       VARCHAR2(30),
  create_time       VARCHAR2(30),
  change_type       VARCHAR2(30),
  object_script     CLOB,
  ts                CHAR(19) default to_char(sysdate,'yyyy-mm-dd hh24:mi:ss'),
  pk_etl_object_his CHAR(20) not null,
  oper_ip           VARCHAR2(30),
  oper_owner        VARCHAR2(30),
  obj_id            VARCHAR2(20),
  dr                NUMBER(10) default 0
)
tablespace NNC_DATA01
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
    pctincrease 0
  );

  获取权限：grant select on V_$SESSION to 用户
                  grant select on ALL_SOURCE to 用户
  */
DECLARE
  v_object_name    VARCHAR2(30);
  v_object_type      VARCHAR2(30);
  v_create_time      VARCHAR2(30);
  v_change_type    VARCHAR2(30);
  v_object_script    CLOB; --对象脚本内容
  v_login_user       VARCHAR2(30);
  v_login_ip           VARCHAR2(30);
  v_cCliIP   VARCHAR2(20);
  CURSOR CUR_TEXT
       IS
       SELECT B.TEXT
       FROM ALL_SOURCE B
       WHERE B.TYPE IN ('TRIGGER','PROCEDURE','PACKAGE','PACKAGE BODY','SEQUENCE','VIEW','FUNCTION','DATABASE LINK')
       AND B.OWNER=v_login_user  AND B.NAME=v_object_name
       ORDER BY LINE;
       --定义一个游标变量v_cinfo c_emp%ROWTYPE ，该类型为游标c_emp中的一行数据类型
       c_row CUR_TEXT%rowtype;

BEGIN
  v_object_name   := ora_dict_obj_name; --获取对象名称
  v_object_type     := ora_dict_obj_type ; --获取对象类型
  v_create_time     := TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS');  --获取事件发生时间
  v_change_type    := ora_sysevent;  --获取对象变更类型
  v_login_user       :=  ora_login_user; --获取操作的用户名
  v_login_ip          := ora_client_ip_address;--获取操作的IP地址
  v_cCliIP := ''; --获取执行客户端程序
  SELECT --SYS.LOGIN_USER 数据库用户,
           NVL(SYS_CONTEXT('USERENV', 'IP_ADDRESS'), 'NOIP'), --登录IP
           NVL(OSUSER, 'NOUSER')-- 应用程序
      INTO v_login_ip,v_cCliIP
      FROM V$SESSION
     WHERE AUDSID = USERENV('SESSIONID');

  /* --获取脚本内容--*/
  FOR c_row in CUR_TEXT LOOP
      v_object_script  := v_object_script || c_row.text ;
  END LOOP;

  IF ora_dict_obj_type IN ('TRIGGER','PROCEDURE','PACKAGE','PACKAGE BODY','SEQUENCE','VIEW','FUNCTION','DATABASE LINK')  THEN

    INSERT INTO ETL_TB_OBJECT_HIS   (
                            OBJECT_NAME,
                            OBJECT_TYPE,
                            CREATE_TIME,
                            CHANGE_TYPE,
                            OBJECT_SCRIPT,
                            TS,
                            PK_ETL_OBJECT_HIS,
                            OPER_IP,
                            OPER_OWNER,
                            OBJ_ID
                              )
                   VALUES
                          (v_object_name,
                          v_object_type,
                          v_create_time,
                          v_change_type,
                          v_object_script,
                          TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'),
                          LPAD(ETL_SEQ.NEXTVAL,20,'0'),
                          v_login_ip,
                          v_login_user,
                          v_cCliIP
                          );
    END IF;
END;
/


prompt Done
spool off
set define on
