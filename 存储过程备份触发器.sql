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
       AND B.OWNER=v_login_user  AND B.NAME=v_object_name and B.TYPE = v_object_type
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


-- Create sequence 
create sequence ETL_SEQ
minvalue 1
maxvalue 99999999999999999
start with 7813
increment by 1
nocache;


