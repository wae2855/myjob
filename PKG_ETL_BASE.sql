prompt PL/SQL Developer Export User Objects for user NCTEST@LINUX_ORCL
prompt Created by wangwei on 2018��9��3��
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
  is '�洢������־����';
comment on column ETL_TB_PROCEDURE_LOG.object_id
  is '�������';
comment on column ETL_TB_PROCEDURE_LOG.client_ip
  is '�ͻ���ִ�ж���IP';
comment on column ETL_TB_PROCEDURE_LOG.comments
  is '��ע';
comment on column ETL_TB_PROCEDURE_LOG.data_date
  is '��������';
comment on column ETL_TB_PROCEDURE_LOG.dr
  is '�Ƿ�ɾ�� 1:ɾ����0:δɾ��';
comment on column ETL_TB_PROCEDURE_LOG.end_time
  is '����ʱ��';
comment on column ETL_TB_PROCEDURE_LOG.proc_name
  is '�洢��������';
comment on column ETL_TB_PROCEDURE_LOG.status
  is '״̬ 1:�쳣��0:����';
comment on column ETL_TB_PROCEDURE_LOG.ts
  is 'ʱ���';
comment on column ETL_TB_PROCEDURE_LOG.tx_date
  is 'ִ��ʱ��';
comment on column ETL_TB_PROCEDURE_LOG.user_name
  is '��ǰ�û���';
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
  is 'ִ�еĽű�';
comment on column ETL_TB_PROCEDURE_LOG_DTL.dr
  is '�Ƿ�ɾ��';
comment on column ETL_TB_PROCEDURE_LOG_DTL.ts
  is 'ʱ���';
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
  *���Ͷ���
  ************/
  /* ȫ�ֱ���*/
  /*TYPE TYP_RECURSOR IS REF CURSOR; */
  TYPE TYP_TABLE IS TABLE OF VARCHAR2(32676);
  /*****************
  *FUNCTIONS DEFINE
  *��������
  *****************/
     --�������ɻ���
  FUNCTION FN_GENERATEPK(CORP VARCHAR) RETURN VARCHAR;
  /**1.��ǰʱ��**/
  FUNCTION FN_GETCURRENTTIME  RETURN TIMESTAMP;
  /**2��һ������**/
  FUNCTION FN_GETLASTDAY(P_INDATE IN VARCHAR2) RETURN VARCHAR2;
  /**3.��һ��ĩ���ڡ�ȥ��ĩһ��,�ϼ���ĩһ��,����ĩһ��,��һ��**/
  FUNCTION FN_GETLASTDAY(P_INDATE IN VARCHAR2,P_TYPE IN VARCHAR2) RETURN VARCHAR2;
           PRAGMA RESTRICT_REFERENCES(FN_GETLASTDAY,RNDS,RNPS,WNDS,WNPS);
  /**4.��һ������**/
  FUNCTION FN_GETNEXTDAY(P_INDATE IN VARCHAR2) RETURN VARCHAR2;
  /**5��һ�ڳ����ڡ������һ��,�¼��ȵ�һ��,���µ�һ��,��һ��**/
  FUNCTION FN_GETNEXTDAY(P_INDATE IN VARCHAR2,P_TYPE IN VARCHAR2) RETURN VARCHAR2;
           PRAGMA RESTRICT_REFERENCES(FN_GETNEXTDAY,RNDS,RNPS,WNDS,WNPS);
  /**6.�ڼ俪ʼ���ڡ��³�����**/
  FUNCTION FN_GETSTARTDAY(P_INDATE IN VARCHAR2) RETURN VARCHAR2;
  /**7.�ڼ俪ʼ���ڡ����,���ȳ�,�³�,����**/
  FUNCTION FN_GETSTARTDAY(P_INDATE IN VARCHAR2,P_TYPE IN VARCHAR2) RETURN VARCHAR2;
           PRAGMA RESTRICT_REFERENCES(FN_GETSTARTDAY,RNDS,RNPS,WNDS,WNPS);
  /**8.�ڼ�������ڡ���ĩ����**/
  FUNCTION FN_GETCLOSEDAY(P_INDATE IN VARCHAR2) RETURN VARCHAR2;
  /**9.�ڼ�������ڡ�������һ��,����ĩһ��,��ĩһ��,����**/
  FUNCTION FN_GETCLOSEDAY(P_INDATE IN VARCHAR2,P_TYPE IN VARCHAR2) RETURN VARCHAR2;
           PRAGMA RESTRICT_REFERENCES(FN_GETCLOSEDAY,RNDS,RNPS,WNDS,WNPS);
  /**10.ͬ�ڡ�������ĩ,���꼾ĩ,������ĩ,����ͬ��**/
  FUNCTION FN_GETSAMEPERIOD(P_INDATE IN VARCHAR2,P_TYPE IN VARCHAR2) RETURN VARCHAR2;
           PRAGMA RESTRICT_REFERENCES(FN_GETSAMEPERIOD,RNDS,RNPS,WNDS,WNPS);
  /**11.���ڳ�ʵ������**/
  FUNCTION FN_GETBGPDDAYS(P_INDATE IN VARCHAR2,P_TYPE IN VARCHAR2) RETURN NUMBER;
           PRAGMA RESTRICT_REFERENCES(FN_GETBGPDDAYS,RNDS,RNPS,WNDS,WNPS);
  /**12.�����ڼ�����**/
  FUNCTION FN_GETDAYS(S_INDATE IN VARCHAR2,E_INDATE IN VARCHAR2) RETURN NUMBER;
           PRAGMA RESTRICT_REFERENCES(FN_GETDAYS,RNDS,RNPS,WNDS,WNPS);
  /**13.�����Զ�������֮������**/
  FUNCTION FN_GETCUSDATE(P_INDATE IN VARCHAR2, P_DAY IN NUMBER) RETURN VARCHAR2;
  /**14.�����÷ָ����ŷָ��ĵڼ����ַ��� **/
  FUNCTION FN_GET_STRLIST(P_STR_LIST IN CLOB, P_SPLIT IN VARCHAR2, P_STR_NUM IN NUMBER) RETURN VARCHAR2;
           PRAGMA RESTRICT_REFERENCES(FN_GET_STRLIST,RNPS,WNDS,WNPS);
  /**15.����������ʱ���ʱ�����룩 **/
  FUNCTION FN_TIMESTAMP_CHA(ENDTIME   IN TIMESTAMP,STARTTIME IN TIMESTAMP) RETURN INTEGER ;
  /**16.�����ַ�����sql����Ƿ�ִ�гɹ���1ִ�д���0ִ�гɹ� **/
  FUNCTION FN_SQLCHECK(P_cSql   IN CLOB)   RETURN INTEGER ;

  /**17.����sql���ִ���Ƿ���ȷ**/
  FUNCTION FN_DATE8TO10(i_date varchar2) return varchar2;

 /**18.����sql���ִ���Ƿ���ȷ**/
  FUNCTION FN_DATE10TO8(i_date varchar2) return varchar2;


  /*****************
  *PROCEDURE DEFINE
  *****************/

  /**1.��¼����־**/
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
  /**2.��¼��ϸ��־**/
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
  /**3.���ָ����**/
  PROCEDURE SP_CLEARTABLES(P_USER IN VARCHAR2, P_TABLE IN VARCHAR2);
  /**4.ɾ������־**/
  PROCEDURE SP_DELMAINLOG(P_INDATE IN VARCHAR2, P_JOBID IN VARCHAR2);
  /**5.ɾ������,���ж��Ƿ����**/
  PROCEDURE SP_DROPINDEX(P_USER IN VARCHAR2,P_TABLE IN VARCHAR2,P_INDEXNAME IN VARCHAR2);
  /**6.�ؽ�����,���ж��Ƿ����**/
  PROCEDURE SP_RECREATEINDEX(P_USER IN VARCHAR2, P_TABLE IN VARCHAR2, P_INDEXNAME IN VARCHAR2, P_COLUMN    IN VARCHAR2);
  /**7.ɾ������,���ж��Ƿ����**/
  PROCEDURE SP_DROPPARTITION(P_USER IN VARCHAR2, P_TABLE IN VARCHAR2, P_PARTITION IN VARCHAR2);
  /**8.���ӷ���,���ж��Ƿ����**/
  PROCEDURE SP_ADDPARTITION(P_USER IN VARCHAR2, P_TABLE IN VARCHAR2, P_TYPE IN VARCHAR2, P_PARTITION IN VARCHAR2,P_TABSPACE IN VARCHAR2, P_VALUE IN VARCHAR2);
  /**9.��ѯ�û��������б������ֶ��Ƿ����ĳ���ַ���**/
  PROCEDURE SP_FIND_STR_ALLTABLE(P_USER IN VARCHAR2 , P_STR IN VARCHAR2);
   /**10. ��DWETL�û��½���ȫ�����  **/
  PROCEDURE SP_ALL_ANALYZE_TABLE( P_TABLENAME IN VARCHAR2);
  /**11. ������һ��������**/
  FUNCTION SP_GET_LASTWORKDATE(i_date varchar2 ) RETURN VARCHAR2;
  PROCEDURE SP_ETL_PKG_PRO_TEMPLATE(
     P_Creater IN VARCHAR2,--������
     P_cObjectId  IN VARCHAR2, --�洢���̱���
     P_cProcName  IN VARCHAR2, --�洢��������
     P_cComments  IN VARCHAR2, --�洢��������
     P_cSourTable  IN VARCHAR2, --�洢������Դ����
     P_cTargTable  IN VARCHAR2, --�洢����Ŀ�����
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

     --�������ɻ���
FUNCTION FN_GENERATEPK(corp varchar) return varchar as
 PRAGMA AUTONOMOUS_TRANSACTION;
  /*
       NC�������ɺ���
   * ���ɷ����ο����м���ڲ��㷨
   *��������� corp  ��˾����
   * ���������new_pk �����ɵ�pk
   ORA-14551: �޷��ڲ�ѯ��ִ�� DML ���� http://www.linuxidc.com/Linux/2013-06/86712.htm
   ���ݿ�������һ�ֵ�Ԫ������Ҫô��ȫ���������ɹ���Ҫôȫ��ʧ�ܡ���Oracle�У�һ�������Ǵ�ִ�е�һ�����ݹ������ԣ�DML����俪ʼ��
   ֱ��ִ��һ��COMMIT��䣬�ύ����������񣬻���ִ��һ��ROLLBACK��䣬�����˴β�������������ġ�Ҫôȫ����ɣ�
   Ҫôʲô��û��ɡ��ı��Ի�ʹ��������Ϣ�������ݿ���б�ú����ѣ���Ϊ������ʧ����������ʱ��������д��־��Ŀ��INSERT��仹δ��ɡ�
   �������������Oracle�ṩ��һ�ֱ�ݵķ�����������������������ӵ�ǰ����ʼ������������ﾳ��ִ�С������ܶ����ر��ύ���������У�
   ����Ӱ���������е���������Ϊ���������ǳ��˱�д������־����������ʽ���������м�⵽����ʱ���������ڴ�����־����в���һ�в���
   ������Ȼ���ڲ���ʧ��β��������»ع���������Ϊ�����������������������ģ����������ܼ�⵽���޸Ĺ����еĵ�ǰ״̬����ͺ�����
   �������ύ֮ǰ������һֱ���ڵ����ĻỰ�������������˵�������ǲ����õġ�Ȼ��������������Ͳ�ͬ�ˣ��������ܹ���⵽�Ѿ�ִ�й���
   ��������Ľ����Ҫ����һ���������������������������߲���ߴ洢���̡����������ݰ��򴥷��Ķ��岿���У�ʹ��PL/SQL�е�
   PRAGMA AUTONOMOUS_TRANSACTION��䡣��������ģ��������ִ�е�SQLServer��䶼�����εġ������޷�����COMMIT��䣬
   ������PRAGMA AUTONOMOUS_TRANSACTION��ǡ����ǣ�ֻ�д����е������ܱ��ύ�����������С�
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
  --��ʼ������
  temp_oid :='';
  old_oid := '';
  new_oid := '10000000000000';
  CODE_LENGTH := 14;
  MINI_CODE := 48;
  MAX_CODE := 90;

  global_count := 14;
  --��ѯ�ù�˾������pk
  select  pub_oid.idnumber
    into old_oid
    from pub_oid pub_oid
   where pub_oid.pk_corp = corp;
-- --DBMS_OUTPUT.put_line('old_oid=' || old_oid);
  --�����µ�oid
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
         --�����ʱoidΪ new_oid
         new_oid := temp_oid;
         exit; -- ����ѭ��
      end if;
  END LOOP;
  ----DBMS_OUTPUT.PUT_LINE('��������'||corp || 'AA' || old_oid);
  ----DBMS_OUTPUT.PUT_LINE('��������'||corp || 'AA' || new_oid);

  --update the new value ����pkΪ�����ɵ�pk
  update pub_oid set idnumber = new_oid where pk_corp = corp;
  return_str := corp || 'AA' || new_oid;
  commit;
  return return_str;
exception
  WHEN NO_DATA_FOUND THEN
  --INSERT THE NEW VALUE �����µ�pk
  insert into pub_oid
      (dr, idnumber, pk_corp)
    values
      (0, new_oid, corp);
    return_str := corp || 'AA' || new_oid;
    commit;
   return return_str;
  WHEN OTHERS THEN
    rollback;
    ----DBMS_OUTPUT.PUT_LINE('��˾' || corp || '����������������');
    return_str := corp || 'AA' || new_oid;
    return return_str;
end;

    /**1.��ǰʱ��
  TIME_TYPE=1�����뼶 2���뼶
  V_CURRENTTIME=���ص�ǰ����+ʱ�䣨TIMESTAMP��ʽ��
  **/
    FUNCTION FN_GETCURRENTTIME RETURN TIMESTAMP AS
    V_CURRENTTIME  TIMESTAMP ;
    BEGIN
      SELECT SYSTIMESTAMP INTO V_CURRENTTIME FROM DUAL;
      RETURN V_CURRENTTIME;
    END;

  /**2.��һ������
  P_INDATE=��������,10λ�ַ���
  V_LASTDAY=������һ��
  **/

  FUNCTION FN_GETLASTDAY(P_INDATE IN VARCHAR2) RETURN VARCHAR2 AS
    V_TXDATE  DATE := TO_DATE(P_INDATE, 'YYYY-MM-DD');
    V_LASTDAY VARCHAR2(10);
  BEGIN
    V_LASTDAY := TO_CHAR(V_TXDATE - 1, 'YYYY-MM-DD');
    RETURN V_LASTDAY;
  END FN_GETLASTDAY;

  /**3.��һ��ĩ���ڡ�ȥ��ĩһ��,�ϼ���ĩһ��,����ĩһ��,��һ��
  P_INDATE=��������,10λ�ַ���
  P_TYPE=�ڼ����� Y-�� Q-���� M-�� D-��
  V_LASTDAY=������һ��ĩ����
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

  /**4��һ������
  P_INDATE=��������,10λ�ַ���
  V_NEXTDAY=������һ������
  **/
  FUNCTION FN_GETNEXTDAY(P_INDATE IN VARCHAR2) RETURN VARCHAR2 AS
    V_TXDATE  DATE := TO_DATE(P_INDATE, 'YYYY-MM-DD');
    V_NEXTDAY VARCHAR2(10);
  BEGIN
    V_NEXTDAY := TO_CHAR(V_TXDATE + 1, 'YYYY-MM-DD');
    RETURN V_NEXTDAY;
  END FN_GETNEXTDAY;

  /**5.��һ�ڳ����ڡ������һ��,�¼��ȵ�һ��,���µ�һ��,��һ��
  P_INDATE=��������,10λ�ַ���
  P_TYPE=�ڼ����� Y-�� Q-���� M-�� D-��
  V_NEXTDAY=������һ�ڳ�����
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

  /**6.�ڼ俪ʼ���ڡ��³�����
  P_INDATE=��������,10λ�ַ���
  V_STARTDAY=���ص��µ�һ��
  **/
  FUNCTION FN_GETSTARTDAY(P_INDATE IN VARCHAR2) RETURN VARCHAR2 AS
    V_STARTDAY VARCHAR2(10);
  BEGIN
    V_STARTDAY := SUBSTR(P_INDATE, 1, 7) || '-01';
    RETURN V_STARTDAY;
  END FN_GETSTARTDAY;

  /**7.�ڼ俪ʼ���ڡ����,���ȳ�,�³�,����
  P_INDATE=��������,10λ�ַ���
  P_TYPE=�ڼ����� Y-�� Q-���� M-�� D-��
  V_STARTDAY=�����ڳ�����
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

  /**8.�ڼ�������ڡ���ĩ����
  P_INDATE=��������,10λ�ַ���
  V_CLOSEDAY=������ĩ����
  **/
  FUNCTION FN_GETCLOSEDAY(P_INDATE IN VARCHAR2) RETURN VARCHAR2 AS
    V_TXDATE   DATE := TO_DATE(P_INDATE, 'YYYY-MM-DD');
    V_CLOSEDAY VARCHAR2(10);
  BEGIN
    V_CLOSEDAY := TO_CHAR(LAST_DAY(V_TXDATE), 'YYYY-MM-DD');
    RETURN V_CLOSEDAY;
  END FN_GETCLOSEDAY;

  /**9.�ڼ�������ڡ�������һ��,����ĩһ��,��ĩһ��,����
  P_INDATE=��������,10λ�ַ���
  P_TYPE=�ڼ����� Y-�� Q-���� M-�� D-��
  V_CLOSEDAY=������ĩ����
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

/**10.����ͬ�����ڡ�������ĩ,���꼾ĩ,������ĩ,����ͬ��
P_INDATE=��������,10λ�ַ���
P_TYPE=�ڼ����� Y-������ĩ Q-���꼾ĩ M-������ĩ D-����ͬ��
V_CLOSEDAY=��������ͬ������
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

  /**11.���ڳ�ʵ������
  P_INDATE=��������,10λ�ַ���
  P_TYPE=�ڼ����� Y-�� HY-���� Q-�� M-�� W-��
  V_DAYS=����ʵ������
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

  /**12.�����ڼ�����
  S_INDATE=���뿪ʼ����,10λ�ַ���
  E_INDATE=�����������,10λ�ַ���
  V_DAYS=����ʵ������
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
  /**13.�����Զ�������֮������
  P_INDATE=���뿪ʼ����,10λ�ַ���
  P_DAY=��������
  V_INDATE=����P_DAY֮������
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

    /**14.�����÷ָ����ŷָ��ĵڼ����ַ���
  P_STR_LIST=�����ַ���
  P_SPLIT=�ָ�����
  P_STR_NUM=��Ҫ���صĵڼ����ַ���
  V_STR=���ض�Ӧ���ַ���
  **/
  FUNCTION FN_GET_STRLIST(P_STR_LIST IN CLOB, P_SPLIT IN VARCHAR2, P_STR_NUM IN NUMBER)
  RETURN VARCHAR2 IS --STR_LIST ƴ�ӵ��ַ������÷ֺŸ���   STR_NUM ���صڼ���ֵ
    V_STR CLOB := '';
    n_length number :=0;
    Star_index number :=0;
    v_Count  number :=0;
  BEGIN
    --�жϵ�ǰ�ַ����м����ָ���
     SELECT NVL(LENGTH(REGEXP_REPLACE(REPLACE(P_STR_LIST, P_SPLIT, '@'),  '[^@]+',  '')),0) INTO v_Count FROM DUAL;

    IF P_STR_NUM <= 0 OR P_STR_NUM >v_Count+1 THEN
       V_STR :='' ;--����ĵ�n��Ԫ�ز�����
    ELSIF P_STR_NUM =1 AND v_Count = 0 THEN --ֻ��һ��Ԫ�ص����
      V_STR :=P_STR_LIST;
    ELSIF P_STR_NUM = v_Count+1 THEN ---ȡ���һ��Ԫ��
      Star_index := INSTR(TO_CHAR(P_STR_LIST),P_SPLIT,1,P_STR_NUM-1) + LENGTH(P_SPLIT) ;
      V_STR := substr(TO_CHAR(P_STR_LIST),Star_index,90000);
    ELSIF P_STR_NUM = 1 and  v_Count>0 THEN ---��һ��Ԫ��
      n_length := INSTR(TO_CHAR(P_STR_LIST),P_SPLIT,1,P_STR_NUM) - 1 ;
      V_STR := SUBSTR(TO_CHAR(P_STR_LIST),1,n_length);
    ELSE
      n_length := INSTR(TO_CHAR(P_STR_LIST),P_SPLIT,1,P_STR_NUM) -  INSTR(TO_CHAR(P_STR_LIST),P_SPLIT,1,P_STR_NUM-1) - 1;
      Star_index := INSTR(TO_CHAR(P_STR_LIST),P_SPLIT,1,P_STR_NUM-1) +1 ;
      V_STR := SUBSTR(TO_CHAR(P_STR_LIST),Star_index,n_length);
    END IF;
    RETURN V_STR;
  END FN_GET_STRLIST;

  /**15.����������ʱ���ʱ��� �����룩
  ENDTIME=�������ʱ�䣨TIMESTAMP��
  STARTTIME=���뿪ʼʱ�䣨TIMESTAMP��
  ���أ����룩
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

 /**16.����sql���ִ���Ƿ���ȷ
  P_cSql=У���sql���
  ����1 ִ�д��� 0 ִ����ȷ
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
/**17.����sql���ִ���Ƿ���ȷ
  i_date= ����'yyyymmdd'
  date_convert = ��������'yyyy-mm-dd'
  'yyyymmdd' ת��Ϊ 'yyyy-mm-dd'
  **/
  function FN_DATE8TO10(i_date varchar2) return varchar2 is
    date_convert varchar2(20);
  begin
    date_convert:= to_char(to_date(i_date,'YYYYMMDD'),'YYYY-MM-YY');
    return date_convert;
  end;
  /**18.����sql���ִ���Ƿ���ȷ
  i_date=����'yyyy-mm-dd'
  date_convert = �������� 'yyyymmdd'
  'yyyy-mm-dd' ת��Ϊ 'yyyymmdd'
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
  /**1.��¼����־
  P_OBJ_ID=���̱���
  P_USERNAM=�û�
  P_PROCNAM=������
  P_TXDATE=ִ������
  P_DATADATE=�������ڣ��������ڣ�
  P_COMMENTS=ִ������
  P_STARTTIME=��ʼʱ��
  P_ENDTIME=����ʱ��
  P_ELAPSEDTIME=�ܺ�ʱ���룩
  P_CLIENTIP=ִ�иù��̿ͻ���IP
  P_CLENTPROC=ִ�иù��̵Ŀͻ��˳���
  P_STATUS=״̬:0-�ɹ�,1-ʧ��
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

  /**2.��¼��ϸ��־
  P_OBJ_ID=���̱���
  P_PROCNAM=������
  P_TXDATE=ִ������
  P_DATADATE=�������ڣ��������ڣ�
  P_COMMENTS=ִ������
  P_SQLCODE=ִ�н������
  P_SQLSTATE=ִ�н��˵��
  P_STATUS=״̬:0-�ɹ�,1-ʧ��
  P_STARTTIME=��ʼʱ��
  P_ENDTIME=����ʱ��
  P_ELAPSEDTIME=�ܺ�ʱ���룩
  P_DEALROW= �����¼��
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

  /**3.���ָ����
  P_USER=�û���
  P_TABLE=����
  **/
  PROCEDURE SP_CLEARTABLES(P_USER IN VARCHAR2, P_TABLE IN VARCHAR2) AS
  BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || P_USER || '.' || P_TABLE;
  END SP_CLEARTABLES;

  /**4.ɾ������־
  P_INDATE=�������ڣ��������ڣ�
  P_JOBNAME=������
  **/
  PROCEDURE SP_DELMAINLOG(P_INDATE IN VARCHAR2, P_JOBID IN VARCHAR2) AS
  BEGIN
    DELETE FROM ETL_TB_PROCEDURE_LOG
     WHERE DATA_DATE = P_INDATE
       AND OBJECT_ID = UPPER(P_JOBID) /*AND STATUS<>'9-ERROR'*/
    ;
    COMMIT;
  END SP_DELMAINLOG;

  /**5.ɾ������,���ж��Ƿ����
  P_USER=�û���
  P_TABLE=����
  P_INDEXNAME=������
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

  /**6.�ؽ�����,���ж��Ƿ����
  P_USER=�û���
  P_TABLE=����
  P_INDEXNAME=������
  P_COLUMN=����
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

  /**7.ɾ������,���ж��Ƿ����
   ��Ҫ���û���Ȩ���Է���DBA_TAB_PARTITIONS
   grant select on dba_tab_partitions to user_name
   P_USER=�û���
   P_TABLE=����
   P_PARTITION=������
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

  /**8.���ӷ���,���ж��Ƿ����
   P_USER=�û���
   P_TABLE=����
   P_TYPE=��������,L:list����;R:range������
   P_PARTITION=������
   P_VALUE=����ֵ
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

  /**9.��ѯ�û��������б������ֶ��Ƿ����ĳ���ַ���
   P_USER=�û���
   P_Str=��ѯ���ַ���
   ���ؽ���� ETL_TB_FIND_STR_ALLTABLE
   grant select on DBA_TAB_COLUMNS to dwetl;
   grant select on DBA_TABLES to dwetl;
   grant create table to dwetl;
  **/
   PROCEDURE SP_FIND_STR_ALLTABLE
        (
        P_USER IN VARCHAR2 , --��ѯ���û���
        P_STR IN VARCHAR2 --��ѯ���ַ���
        ) IS
    V_RE_SQL  INT;
    V_SQLSTR VARCHAR2(2000):='';
    V_SQLTABLENAME  VARCHAR2(200):='';
    V_SQLCOLUMN  VARCHAR2(200):='';
    V_STR_TEMP   VARCHAR2(200):='';
    V_DB_USER   VARCHAR2(50):='';
    TYPE REF_CURSOR_TYPE IS REF CURSOR;  --����һ����̬�α�
    COLUMN_NAME REF_CURSOR_TYPE;
    TABLE_NAME REF_CURSOR_TYPE;
  BEGIN
        SELECT COUNT(1) INTO V_RE_SQL FROM USER_TABLES WHERE TABLE_NAME = 'ETL_TB_FIND_STR_ALLTABLE';
        IF V_RE_SQL=0 THEN --����ETL_TB_FIND_STR_ALLTABLE
              V_SQLSTR := '
                       CREATE TABLE ETL_TB_FIND_STR_ALLTABLE
                       (
                       TABLENAME VARCHAR2(30),
                       COLUMN_NAME VARCHAR2(30)
                       )
              ';
              EXECUTE IMMEDIATE V_SQLSTR;
          ELSE --��ձ�ETL_TB_FIND_STR_ALLTABLE
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

  /**10. ��DWETL�û��½���ȫ�����
   P_TABLENAME=����
   �Ա����ȫ�����
  **/
   PROCEDURE SP_ALL_ANALYZE_TABLE
        (
        P_TABLENAME IN VARCHAR2 --��ѯ���û���
        ) IS
        V_SQLSTR VARCHAR2(2000):='';
   BEGIN
     V_SQLSTR := 'ANALYZE TABLE ' || UPPER(P_TABLENAME) || ' COMPUTE STATISTICS FOR TABLE FOR ALL INDEXED COLUMNS';
     EXECUTE IMMEDIATE V_SQLSTR ;
   END ;
  /**11  ������һ��������
  i_date = ��ǰ����
  **/

  FUNCTION SP_GET_LASTWORKDATE(i_date varchar2 )
    return varchar2 is
    v_ret_date varchar2(10) :=i_date ;
    v_count     int := 0 ;
    v_Str_sql varchar2(2000);
  begin
   WHILE  v_count = 0  LOOP  --�������0�����Ϊ��Ϣ�� �����ж���һ��   ����1 ����Ϊ������

    v_ret_date := TO_CHAR(TO_DATE(v_ret_date,'yyyy-mm-dd') -1 ,'yyyy-mm-dd');
    --����ΪNC57�汾���������Ĵ����߼�����ͬ��Ŀ���ղ�ͬ��������޸�
    --Ĭ��ȡ�Ϻ��������Ĺ�������
    v_Str_sql := 'SELECT COUNT(1) FROM  sxzq.sec_tradecalender  WHERE TRADEDATE = ''' || v_ret_date || ''' and ISTRADE = ''Y'' and PK_BOURSESET = ''0001A110000000000228'' ';

    EXECUTE IMMEDIATE TO_CHAR(v_Str_sql) into v_count  ;
    END LOOP;

    return v_ret_date;

  END;
/*�����洢����ģ��*/
PROCEDURE SP_ETL_PKG_PRO_TEMPLATE(
     P_Creater IN VARCHAR2,--������
     P_cObjectId  IN VARCHAR2, --�洢���̱���
     P_cProcName  IN VARCHAR2, --�洢��������
     P_cComments  IN VARCHAR2, --�洢��������
     P_cSourTable  IN VARCHAR2, --�洢������Դ����
     P_cTargTable  IN VARCHAR2, --�洢����Ŀ�����
     vo_returnid out number)
     IS
           /*===============================================================+
      ----------------------�洢������Ϣ-------------------------------
          ��������: �洢��������
          ��������:
          ��������: 2017-02-15
          ������  : ��ΰ
          �汾��  : V1.0
          ���ز���:
          ���빤��(DB version):ORACLE11g(11.2.0.1.0)
          �޸���ʷ:
          �޸�����    �汾��    �޸���              �޸�˵��

      +===============================================================*/

      ----��������(���붨��)-------------------------------------------
        v_cObjectId      VARCHAR2(30) :=P_cObjectId;             --�������
        v_cProcName   VARCHAR2(50)  := P_cProcName ;          --�洢��������;
        v_cComments   VARCHAR2(1000):=P_cComments ;        --�洢����������
        v_cSourTable    VARCHAR2(500)  := P_cSourTable ;          --��Դ��
        v_cTargTable    VARCHAR2(500)  := P_cTargTable ;          --Ŀ���

      ----�Զ������(������������Ϊ �ַ��ͣ�v_c*; ������v_n*; ������v_d*; *:��׺ÿһ��Ӣ����ĸ��д,������ĸСд)----
        v_tSysdate        VARCHAR2(10)  := '' ;
        v_cExists    NUMBER := '';                     --��ȡ�Ƿ����Ҫ�����Ĵ洢����
        v_cSql             CLOB := '';                     --��ȡִ��SQL
      ----��������-----------------------------------------------------
      BEGIN --Main
               SELECT TO_CHAR(SYSDATE,'YYYY-MM-DD') INTO v_tSysdate FROM DUAL;


           --�жϴ洢�����Ƿ����
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
    vi_cDataDate IN VARCHAR2 ,  --�������,10λ����
    vo_cSqlCode  OUT NUMBER     --�������,���ش���
) IS
/*===============================================================+
----------------------�洢������Ϣ-------------------------------
    ��������: �洢���̿���ģ��ģ��
    ��������: �������:vi_cDataDateֵΪYYYY-MM-DD��������,10λ;
              �������:vo_cSqlCodeֵΪ���ش���;
    ��������: ' || v_tSysdate || '
    ������  : ' || P_Creater || '
    �汾��  : V1.0
    ���ز���:
    Ŀ���  : ' ||  v_cTargTable || '
    Դ��    : ' ||  v_cSourTable  || '

    ���빤��(DB version):ORACLE11g(11.2.0.1.0)
    �޸���ʷ:
    �޸�����    �汾��    �޸���              �޸�˵��

+===============================================================*/

----��������(���붨��)-------------------------------------------
  v_cObjectId      VARCHAR2(100) :=''' || v_cObjectId || ''';             --�������
  v_cUserName   VARCHAR2(50)  := '''' ;          --��½���ݿ�ִ�д洢���̵��û�
  v_cProcName   VARCHAR2(200)  := ''' || v_cProcName || ''' ;          --�洢��������;@��Ҫ�ֶ���д@
  v_dTxDate     DATE          := SYSDATE ;       --�������ݴ�������
  v_cComments   VARCHAR2(1000):= '''' ;           --�洢������������������
  v_tStime      TIMESTAMP          := SYSDATE ;      --����������ʼʱ��
  v_tEtime      TIMESTAMP          := SYSDATE ;      --�������������ʱ��
  v_nDealRow    INT           := 0 ;            --����������
  v_cCliIP      VARCHAR2(100) :='''' ;            --�ͻ���IP
  v_cCliProcNam VARCHAR2(100) :='''' ;            --�ͻ��˳�������

  v_cProcDesc   VARCHAR2(200) := ''' || v_cComments || '''  ;   --�洢���̹�������;@��Ҫ�ֶ���д@
  v_cTranDesc   VARCHAR2(1000):= '''' ;           --�洢�����и���������;
  v_cSqlMsg     VARCHAR2(500) := '''' ;           --��������������ݿⷵ�ص���Ϣ
  v_tStime0     TIMESTAMP          := SYSDATE ;      --JOB�Ŀ�ʼʱ��

  v_cMaxDate    VARCHAR2(10)  :=''3000-12-31'';   --�������
  v_cSql          CLOB := '''';                     --��ȡִ��SQL
  v_cSqlLog     CLOB := '''';                     --��ȡִ��SQL�����־
  v_pk_log   VARCHAR2(20) := LPAD(ETL_SEQ.NEXTVAL,20,''0''); --��ȡִ�е�ǰ�洢���̵�20λ����

----�Զ������(������������Ϊ �ַ��ͣ�v_c*; ������v_n*; ������v_d*; *:��׺ÿһ��Ӣ����ĸ��д,������ĸСд)----
  v_cParms      CLOB := '''';                     --��ȡ�������
  v_cTest         VARCHAR2(1000);         --���Բ���������ɾ��
----��������-----------------------------------------------------
BEGIN --Main


----#####################################################################################---
----#########!!!!!!!!!!!!!!!��ʼ����Ϣstart ����ֹ�޸ģ�!!!!!!!!!!!!!!!!########-
----###############################---
    vo_cSqlCode := 0;                         --vo_cSqlCode����ʼֵ
    v_tStime0 := PKG_ETL_BASE.FN_GETCURRENTTIME;--ȡ���忪ʼʱ��
    --ɾ������־�и�JOB�ĵ��ռ�¼
    BEGIN
        PKG_ETL_BASE.SP_DELMAINLOG(vi_cDataDate,v_cObjectId);
    --������Ϣ
    EXCEPTION
    WHEN OTHERS THEN
        vo_cSqlCode := SQLCODE;
        v_cSqlMsg := SQLERRM;
        ROLLBACK;
        RAISE;
    END;

    --��ȡ�ͻ���IP�Ϳͻ��˳��������Լ����ݿ��û�; Ҫ������ͼV$SESSION����Ȩ: GRANT SELECT ON V$SESSION TO USER
    v_tStime := PKG_ETL_BASE.FN_GETCURRENTTIME;--��ȡ����ʼ����
    BEGIN
        SELECT
            SYS.LOGIN_USER  ���ݿ��û�,
            NVL(SYS_CONTEXT(''USERENV'', ''IP_ADDRESS''),''NOIP'') ��¼IP,
            NVL(PROGRAM,''NOPROC'') Ӧ�ó���
            INTO v_cUserName,v_cCliIP,v_cCliProcNam
        FROM V$SESSION
        WHERE AUDSID = USERENV(''SESSIONID'');

        --��¼��������־
        vo_cSqlCode:=SQLCODE;
        v_cSqlMsg:=SQLERRM;
        v_cComments:=''��ȡIP���û����ͻ��˳���ɹ�'';
        v_nDealRow:= SQL%ROWCOUNT;

    EXCEPTION
    WHEN OTHERS THEN
        vo_cSqlCode:=SQLCODE;
        v_cSqlMsg:=SQLERRM;
        v_cComments:=''��ȡIP���û����ͻ��˳������'';
        v_nDealRow:= SQL%ROWCOUNT;
        ROLLBACK;

        --��ȡ�������ʱ��
        v_tEtime := PKG_ETL_BASE.FN_GETCURRENTTIME;
        PKG_ETL_BASE.SP_DTLLOGGING(v_cObjectId,v_pk_log,v_cProcName,v_dTxDate,vi_cDataDate,v_cComments,vo_cSqlCode,v_cSqlMsg,''1'',v_tStime,v_tEtime,PKG_ETL_BASE.FN_TIMESTAMP_CHA(v_tEtime,v_tStime),v_nDealRow,v_cSqlLog);
        RAISE;
    END;
----####################---
----##########!!!!!!!!!!!!!!!!��ʼ����Ϣend   ����ֹ�޸ģ�!!!!!!!!!!!!!!!!!!!!######
----#####################################################################################---




----##########ҵ����ڵ�ģ��start (����ж��ҵ����ڵ㣬��ÿ���ڵ㶼��������������) ##########----

        -----============================================
        ------==============STEP1 START=====================
        ------===========================
    BEGIN
        v_tStime := PKG_ETL_BASE.FN_GETCURRENTTIME;--����ʼ����
        --����ҵ�������
        v_cTranDesc :=''STEP1:ʾ��,�߼������һ��''; --�洢����������Ĺ�������;@��Ҫ�ֶ���д@
        v_cParms :=''�������
        vi_cDataDate: ''||vi_cDataDate||''
        '';

        ----��������Ϊ������sql----
        ----�洢����ִ�������ö�̬SQLִ�У���־��Ҫ��ӡ��ǰִ�е�sql
        v_cSql:=''
        SELECT SYSDATE || :vi_cDataDate FROM DUAL
        '';
        --ִ��sql��־
        v_cSqlLog :=v_cParms||CHR(10)||''
        ִ��sql:''||v_cSql||''
        '';
        --DBMS_OUTPUT.PUT_LINE(v_cSql);
        --ִ��sql
        EXECUTE IMMEDIATE TO_CHAR(v_cSql) INTO v_cTest USING vi_cDataDate ;

        --��¼�ڵ�������־
        vo_cSqlCode := SQLCODE;
        v_cSqlMsg := SQLERRM || '','' || dbms_utility.format_error_backtrace;
        v_nDealRow:= SQL%ROWCOUNT;
        v_cComments := v_cTranDesc||'' ����ɹ�'';
        v_tEtime := PKG_ETL_BASE.FN_GETCURRENTTIME;
        PKG_ETL_BASE.SP_DTLLOGGING(v_cObjectId,v_pk_log,v_cProcName,v_dTxDate,vi_cDataDate,v_cComments,vo_cSqlCode,v_cSqlMsg,''0'',v_tStime,v_tEtime,PKG_ETL_BASE.FN_TIMESTAMP_CHA(v_tEtime,v_tStime),v_nDealRow,v_cSqlLog);
        COMMIT;

    --�ڵ������Ϣ
    EXCEPTION
    WHEN OTHERS THEN
        vo_cSqlCode := SQLCODE;
        v_cSqlMsg := SQLERRM || '','' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
        v_cComments := v_cTranDesc||'' ����ʧ��'';
        v_tEtime := PKG_ETL_BASE.FN_GETCURRENTTIME;--�������ʱ��
        ROLLBACK;
        PKG_ETL_BASE.SP_DTLLOGGING(v_cObjectId,v_pk_log,v_cProcName,v_dTxDate,vi_cDataDate,v_cComments,vo_cSqlCode,v_cSqlMsg,''1'',v_tStime,v_tEtime,PKG_ETL_BASE.FN_TIMESTAMP_CHA(v_tEtime,v_tStime),v_nDealRow,v_cSqlLog);
        RAISE;
    END;
    ------==========================
    ------===============STEP1 END===============================
    ------=====================================================

----##########ҵ����ڵ�ģ��end  (����ж��ҵ����ڵ㣬��ÿ���ڵ㶼��������������) ##########----






----########################################################################################---
----#####!!!!!!!!!!!!!!!!!!!!!!!!!����־����start ,��ֹ�޸ģ�!!!!!!!!!!!!!!!!!!!!!!!!!!####----
----#############################---
--����������־��Ϣ

v_cComments:= v_cProcDesc||'' ����ɹ�'';
v_tEtime := PKG_ETL_BASE.FN_GETCURRENTTIME;--ȡ�������ʱ��
PKG_ETL_BASE.SP_MAINLOGGING(v_cObjectId,v_pk_log,SYS.LOGIN_USER,v_cProcName,v_dTxDate,vi_cDataDate,v_cComments,v_tStime0,v_tEtime,PKG_ETL_BASE.FN_TIMESTAMP_CHA(v_tEtime,v_tStime),v_cCliIP,v_cCliProcNam,''0'');


--���������Ϣ����
EXCEPTION
WHEN OTHERS THEN
    vo_cSqlCode := SQLCODE;
    v_cSqlMsg := SQLERRM;
    v_cComments:= v_cProcDesc||'' ����ʧ��'';
    ROLLBACK;
    v_tEtime := PKG_ETL_BASE.FN_GETCURRENTTIME;--ȡ�������ʱ��
    PKG_ETL_BASE.SP_MAINLOGGING(v_cObjectId,v_pk_log,SYS.LOGIN_USER,v_cProcName,v_dTxDate,vi_cDataDate,v_cComments,v_tStime0,v_tEtime,PKG_ETL_BASE.FN_TIMESTAMP_CHA(v_tEtime,v_tStime),v_cCliIP,v_cCliProcNam,''1'');
    --RAISE_application_error(vo_cSqlCode,v_cSqlMsg);
    RAISE;

----######################---
----#####!!!!!!!!!!!!!!!!!!!!!!!!!����־����end   ,��ֹ�޸ģ�!!!!!!!!!!!!!!!!!!!!!!!!!!#####--
----############################################################################################---
END;
                 ';
                 --DBMS_OUTPUT.PUT_LINE(v_cSql);
                     EXECUTE IMMEDIATE  v_cSql ;
                     vo_returnid :=0;
    --���������Ϣ����
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
  ������Ҫ������ETL_TB_OBJECT_HIS
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

  ��ȡȨ�ޣ�grant select on V_$SESSION to �û�
                  grant select on ALL_SOURCE to �û�
  */
DECLARE
  v_object_name    VARCHAR2(30);
  v_object_type      VARCHAR2(30);
  v_create_time      VARCHAR2(30);
  v_change_type    VARCHAR2(30);
  v_object_script    CLOB; --����ű�����
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
       --����һ���α����v_cinfo c_emp%ROWTYPE ��������Ϊ�α�c_emp�е�һ����������
       c_row CUR_TEXT%rowtype;

BEGIN
  v_object_name   := ora_dict_obj_name; --��ȡ��������
  v_object_type     := ora_dict_obj_type ; --��ȡ��������
  v_create_time     := TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS');  --��ȡ�¼�����ʱ��
  v_change_type    := ora_sysevent;  --��ȡ����������
  v_login_user       :=  ora_login_user; --��ȡ�������û���
  v_login_ip          := ora_client_ip_address;--��ȡ������IP��ַ
  v_cCliIP := ''; --��ȡִ�пͻ��˳���
  SELECT --SYS.LOGIN_USER ���ݿ��û�,
           NVL(SYS_CONTEXT('USERENV', 'IP_ADDRESS'), 'NOIP'), --��¼IP
           NVL(OSUSER, 'NOUSER')-- Ӧ�ó���
      INTO v_login_ip,v_cCliIP
      FROM V$SESSION
     WHERE AUDSID = USERENV('SESSIONID');

  /* --��ȡ�ű�����--*/
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
