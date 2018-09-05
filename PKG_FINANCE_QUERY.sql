prompt PL/SQL Developer Export User Objects for user NCTEST@LINUX_ORCL
prompt Created by wangwei on 2018��9��3��
set define off
spool PKG_FINANCE_QUERY.log

prompt
prompt Creating package PKG_FINANCE_QUERY
prompt ==================================
prompt
CREATE OR REPLACE PACKAGE PKG_FINANCE_QUERY AS
--������������ѯ
  /***********
  *TYPE DEFINE
  *���Ͷ���
  ************/
  /* ȫ�ֱ���*/
  /*TYPE TYP_RECURSOR IS REF CURSOR; */
  v_dblink varchar2(200):=''; --�磺@gdxc
  accchart_code VARCHAR2(200) := '0001'; --��Ŀ������� ���ڻ�ȡ�Է���Ŀʹ��

/*
����������ѯƾ֤��ϸ������
*/
   type DETAIL_ROW is RECORD
    (
      pk_voucher  varchar2(20) , --ƾ֤����
      nov  number(38) ,--ƾ֤����
      pk_org  varchar2(20),  --����ҵ��Ԫ
      pk_group varchar2(20), --��������
      pk_accountingbook char(20), --���������˲�
      accountcode varchar2(40), --������ƿ�Ŀ
      yearv varchar2(4), --�����ڼ�-��
      periodv varchar2(2), --�����ڼ�-��
      adjustperiod varchar2(3), --�����ڼ�-��
      prepareddatev varchar2(19), --�Ƶ�����
      opppsitesubj varchar2(200), --�Է���Ŀ
      direction varchar2(19),--��Ŀ����
      freevaluetype1 varchar2(20),--�������㵵������1
      valueid1 varchar2(20),--������������ֵ1
      freevaluetype2 varchar2(20),--�������㵵������2
      valueid2 varchar2(20),--������������ֵ2
      freevaluetype3 varchar2(20),--�������㵵������3
      valueid3 varchar2(20),--������������ֵ3
      freevaluetype4 varchar2(20),--�������㵵������4
      valueid4 varchar2(20),--������������ֵ4
      freevaluetype5 varchar2(20),--�������㵵������5
      valueid5 varchar2(20),--������������ֵ5
      pk_currtype varchar2(20), --����
      excrate2 number(15,8) ,----����ֵ
      explanation varchar2(300), --ժҪ
      localdebitamount number(28,8) ,--���ҽ跽������
      debitquantity number(28,8) ,--�跽��������
      localcreditamount number(28,8),--���Ҵ���������
      creditquantity number(20,8) --������������
    );
  /*
����������ѯƾ֤��������
*/
  TYPE DETAIL_ROW_SUM IS RECORD
    (
      pk_org  varchar2(20),  --����ҵ��Ԫ
      pk_group varchar2(20), --��������
      pk_accountingbook char(20), --���������˲�
      accountcode varchar2(40), --������ƿ�Ŀ
      freevaluetype1 varchar2(20),--�������㵵������1
      valueid1 varchar2(20),--������������ֵ1
      freevaluetype2 varchar2(20),--�������㵵������2
      valueid2 varchar2(20),--������������ֵ2
      freevaluetype3 varchar2(20),--�������㵵������3
      valueid3 varchar2(20),--������������ֵ3
      freevaluetype4 varchar2(20),--�������㵵������4
      valueid4 varchar2(20),--������������ֵ4
      freevaluetype5 varchar2(20),--�������㵵������5
      valueid5 varchar2(20),--������������ֵ5
      detal_count INT, --ƾ֤����
      localdebitamount number(28,8) ,--�����ۼƱ��ҽ跽������
      debitquantity number(28,8) ,--�����ۼƽ跽��������
      localcreditamount number(28,8),--�����ۼƱ��Ҵ���������
      creditquantity number(20,8), --�����ۼƴ�����������
      sumdebitamount number(28,8) ,--�ۼƱ��ҽ跽������
      sumcreditamount number(28,8) ,--�ۼƱ��Ҵ���������
      beginning_balances  number(20,8), --�ڳ����
      Final_balance  number(20,8), --��ĩ���
      Final_amount  number(20,8)--������
    );
 


  TYPE QueryDETAIL IS TABLE OF DETAIL_ROW;--�Զ���table��
  TYPE QueryDETAIL_SUM IS TABLE OF DETAIL_ROW_SUM;--�Զ���table��

  /**�����÷ָ����ŷָ��ĵڼ����ַ��� **/
  FUNCTION FN_GET_STRLIST(P_STR_LIST IN CLOB, P_SPLIT IN VARCHAR2, P_STR_NUM IN NUMBER) RETURN VARCHAR2;
           PRAGMA RESTRICT_REFERENCES(FN_GET_STRLIST,RNPS,WNDS,WNPS);

  /**������������ת���� �����ö��ŷָ����ŷָ�������**/
  FUNCTION FN_FREEVALUE_CODE_TO_PK(vi_code IN CLOB,
   vi_tablecode IN varchar2 --��������� ����λ�ڱ�bd_accassitem
   )RETURN CLOB;

  FUNCTION FN_OPPOSITESUBJ_CODE_TO_PK(vi_code in varchar2)
  RETURN VARCHAR2;
 /**1. ���������������sql���ƴ�ӹ���**/
  FUNCTION MAKE_SQL_FREEVALUE_DTL(
        vi_colnum IN INT,
        vi_freevaluetype in varchar2 default null,
        --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
        vi_valueid in varchar2 default null
        --������������ �����Ӧ��������������(bd_accassitem)�鿴
    )
  RETURN clob;
/**2. �����������sql��� ƴ�ӹ���**/
  FUNCTION Make_SQL_FREEVALUE(
  --������������ �����Ӧ��������������(bd_accassitem)�鿴
        vi_freevaluetype1 in varchar2 default null,
        --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
        vi_valueid1 in varchar2 default null,
        --������������ �����Ӧ��������������(bd_accassitem)�鿴
        vi_freevaluetype2 in varchar2 default null,
        --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
        vi_valueid2 in varchar2 default null,
        --������������ �����Ӧ��������������(bd_accassitem)�鿴
        vi_freevaluetype3 in varchar2 default null,
        --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
        vi_valueid3 in varchar2 default null,
        --������������ �����Ӧ��������������(bd_accassitem)�鿴
        vi_freevaluetype4 in varchar2 default null,
        --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
        vi_valueid4 in varchar2 default null,
        vi_freevaluetype5 in varchar2 default null,
      --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
        vi_valueid5 in varchar2 default null
  )
  RETURN clob;
/**3. �����������sql���SELECT ���ֶ�ƴ�ӹ���**/
  FUNCTION MAKE_SQL_FREEVALUE_TITLE(
     sql_type in int, --0 select��Ӧ�ֶ�ƴ��  1 group by ��Ӧ�ֶ�ƴ��
  --������������ �����Ӧ��������������(bd_accassitem)�鿴
      vi_freevaluetype1 in varchar2 default null,
      --������������ �����Ӧ��������������(bd_accassitem)�鿴
      vi_freevaluetype2 in varchar2 default null,
      vi_freevaluetype3 in varchar2 default null,
      vi_freevaluetype4 in varchar2 default null,
      vi_freevaluetype5 in varchar2 default null
  )
  RETURN VARCHAR2;
 /**4. ���ض�Ӧ��Ŀ�����������ƾ֤��ϸ**/
  FUNCTION QUERYDETAIL_CHECK(
      vi_sdate in varchar2,--��ʼ����
      vi_edate in varchar2,--��������
      vi_account in varchar2,--��ƿ�Ŀ
      vi_accountingbook in varchar2 default null ,--��ƺ����˲�
      vi_oppaccount in varchar2 default null, --�Է���Ŀ
      --������������ �����Ӧ��������������(bd_accassitem)�鿴
      vi_freevaluetype1 in varchar2 default null,
      --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
      vi_valueid1 in varchar2 default null,
      --������������ �����Ӧ��������������(bd_accassitem)�鿴
      vi_freevaluetype2 in varchar2 default null,
      --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
      vi_valueid2 in varchar2 default null,
      --������������ �����Ӧ��������������(bd_accassitem)�鿴
      vi_freevaluetype3 in varchar2 default null,
      --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
      vi_valueid3 in varchar2 default null,
      --������������ �����Ӧ��������������(bd_accassitem)�鿴
      vi_freevaluetype4 in varchar2 default null,
      --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
      vi_valueid4 in varchar2 default null,
      vi_freevaluetype5 in varchar2 default null,
      --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
      vi_valueid5 in varchar2 default null
     )
      RETURN QueryDETAIL  --����table����
      PIPELINED; --��ˮʽ

  /**5. ���ض�Ӧ��Ŀ����������Ŀ�Ŀ���**/
  FUNCTION QUERYDETAIL_CHECK_SUM(
      vi_sdate in varchar2,--��ʼ����
      vi_edate in varchar2,--��������
      vi_account in varchar2,--��ƿ�Ŀ
      vi_accountingbook in varchar2  default null,--��ƺ����˲�
      vi_oppaccount in varchar2 default null, --�Է���Ŀ
      vi_freevaluetype1 in varchar2 default null,
      --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
      vi_valueid1 in varchar2 default null,
      --������������ �����Ӧ��������������(bd_accassitem)�鿴
      vi_freevaluetype2 in varchar2 default null,
      --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
      vi_valueid2 in varchar2 default null,
      --������������ �����Ӧ��������������(bd_accassitem)�鿴
      vi_freevaluetype3 in varchar2 default null,
      --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
      vi_valueid3 in varchar2 default null,
      --������������ �����Ӧ��������������(bd_accassitem)�鿴
      vi_freevaluetype4 in varchar2 default null,
      --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
      vi_valueid4 in varchar2 default null,
      vi_freevaluetype5 in varchar2 default null,
      --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
      vi_valueid5 in varchar2 default null
     )
      RETURN QueryDETAIL_SUM  --����table����
      PIPELINED; --��ˮʽ

END PKG_FINANCE_QUERY;
/

prompt
prompt Creating package body PKG_FINANCE_QUERY
prompt =======================================
prompt
CREATE OR REPLACE PACKAGE BODY PKG_FINANCE_QUERY --����ƾ֤���������ѯ
 AS

  /*
  ���±���Ҫͨ��DBlink���з��ʣ���Ҫ��ͨ��Ӧ�Ķ�ȡȨ�ޡ�
  gl_detail
  org_accountingbook
  org_orgs
  org_group
  bd_account
  gl_docfree1
  FI_FREEMAP
  bd_accassitem
  bd_accasoa
  ���ָ����������õĵ���*/

  /**.�����÷ָ����ŷָ��ĵڼ����ַ���
  P_STR_LIST=�����ַ���
  P_SPLIT=�ָ�����
  P_STR_NUM=��Ҫ���صĵڼ����ַ���
  V_STR=���ض�Ӧ���ַ���
  **/
  FUNCTION FN_GET_STRLIST(P_STR_LIST IN CLOB,
                          P_SPLIT    IN VARCHAR2,
                          P_STR_NUM  IN NUMBER) RETURN VARCHAR2 IS
    --STR_LIST ƴ�ӵ��ַ������÷ֺŸ���   STR_NUM ���صڼ���ֵ
    V_STR      CLOB := '';
    n_length   number := 0;
    Star_index number := 0;
    v_Count    number := 0;
  BEGIN
    --�жϵ�ǰ�ַ����м����ָ���
    SELECT NVL(LENGTH(REGEXP_REPLACE(REPLACE(P_STR_LIST, P_SPLIT, '@'),
                                     '[^@]+',
                                     '')),
               0)
      INTO v_Count
      FROM DUAL;
  
    IF P_STR_NUM <= 0 OR P_STR_NUM > v_Count + 1 THEN
      V_STR := ''; --����ĵ�n��Ԫ�ز�����
    ELSIF P_STR_NUM = 1 AND v_Count = 0 THEN
      --ֻ��һ��Ԫ�ص����
      V_STR := P_STR_LIST;
    ELSIF P_STR_NUM = v_Count + 1 THEN
      ---ȡ���һ��Ԫ��
      Star_index := INSTR(TO_CHAR(P_STR_LIST), P_SPLIT, 1, P_STR_NUM - 1) +
                    LENGTH(P_SPLIT);
      V_STR      := substr(TO_CHAR(P_STR_LIST), Star_index, 90000);
    ELSIF P_STR_NUM = 1 and v_Count > 0 THEN
      ---��һ��Ԫ��
      n_length := INSTR(TO_CHAR(P_STR_LIST), P_SPLIT, 1, P_STR_NUM) - 1;
      V_STR    := SUBSTR(TO_CHAR(P_STR_LIST), 1, n_length);
    ELSE
      n_length   := INSTR(TO_CHAR(P_STR_LIST), P_SPLIT, 1, P_STR_NUM) -
                    INSTR(TO_CHAR(P_STR_LIST), P_SPLIT, 1, P_STR_NUM - 1) - 1;
      Star_index := INSTR(TO_CHAR(P_STR_LIST), P_SPLIT, 1, P_STR_NUM - 1) + 1;
      V_STR      := SUBSTR(TO_CHAR(P_STR_LIST), Star_index, n_length);
    END IF;
    RETURN V_STR;
  END FN_GET_STRLIST;

  /**������������ת���� �����ö��ŷָ����ŷָ�������**/
  FUNCTION FN_FREEVALUE_CODE_TO_PK(vi_code      IN CLOB,
                                   vi_tablecode IN varchar2 --��������� ����λ�ڱ�bd_accassitem
                                   ) RETURN CLOB IS
    v_pkstr     CLOB; --���ص�pk
    v_tablename varchar2(200); --��������
    v_pkfield   varchar2(200); --����������ֶ���
    v_sql       varchar2(3000);
  begin
  
    begin
      --��ȡ�����ı���
      select b.defaulttablename
        into v_tablename
        from bd_accassitem a
        left join md_class b
          on a.classid = b.id
       where a.code = vi_tablecode;
    
      --��ȡ�����������ֶ���
      select c.name
        into v_pkfield
        from bd_accassitem a
        left join md_class b
          on a.classid = b.id
       inner join md_column c
          on b.defaulttablename = c.tableid
       where c.pkey = 'Y'
         and code = vi_tablecode;
    
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        return '';
    END;
  
    /* ====�ر�ע�⣺    Ĭ�����е����ı����ֶ�������code ,��������Ŀ������
                    �����ֶ�����˴��������߼��жϡ�
       ======
    */
    v_sql := 'select wm_concat(' || v_pkfield || ') from ' || v_tablename ||
             v_dblink || ' where code in (''' ||
             replace(vi_code, ',', ''',''') || ''')';
    EXECUTE IMMEDIATE v_sql
      into v_pkstr;
    return v_pkstr;
  end;
  /**�Է���Ŀ����ת���� �����ö��ŷָ����ŷָ�������**/
  FUNCTION FN_OPPOSITESUBJ_CODE_TO_PK(vi_code in varchar2) RETURN VARCHAR2 IS
    TYPE ref_cursor_type IS REF CURSOR; --����һ����̬�α�
    rowdata      ref_cursor_type;
    v_code       varchar2(2000) := replace(vi_code, ',', ''',''');
    v_sql        varchar2(3000) := '';
    v_pk         varchar2(100) := '';
    v_return_str varchar2(3000) := '';
    ROW_ID       INT := 0;
  BEGIN
    IF INSTR(v_code, '%') > 0 THEN
      v_code := 'LIKE (''' || v_code || ''')';
    ELSE
      v_code := 'IN (''' || v_code || ''')';
    END IF;
    v_sql := ' SELECT a.pk_accasoa
                    FROM bd_accasoa' || v_dblink || ' a
                    inner  join BD_ACCOUNT' || v_dblink || ' b
                    ON a.pk_account = b.pk_account
                    INNER JOIN bd_accchart' || v_dblink || ' C
                    ON B.PK_accchart = C.PK_accchart
                    WHERE C.CODE = ''' || accchart_code ||
             ''' and b.CODE ' || v_code || '';
    --dbms_output.put_line(v_sql);
  
    OPEN rowdata FOR v_sql;
    LOOP
      FETCH rowdata
        INTO v_pk;
      exit when rowdata%notfound;
      ROW_ID       := ROW_ID + 1; --���ڼ�¼��������
      v_return_str := v_return_str || 'or a.oppositesubj like ''%' || v_pk ||
                      '%'' ';
    END LOOP;
    CLOSE rowdata;
  
    IF NVL(vi_code, '999') <> '999' AND NVL(v_return_str, '999') <> '999' THEN
      v_return_str := 'and ( ' || substr(v_return_str, 3) || ')';
    ELSE
      v_return_str := '--�޸ÿ�Ŀ����';
    END IF;
  
    return v_return_str;
  
  END;
  /*
   1.���������������sql���ƴ�ӹ���
  */
  FUNCTION MAKE_SQL_FREEVALUE_DTL(vi_colnum        IN INT,
                                  vi_freevaluetype in varchar2 default null,
                                  --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
                                  vi_valueid in varchar2 default null
                                  --������������ �����Ӧ��������������(bd_accassitem)�鿴
                                  ) RETURN clob is
    v_freevaluetype varchar2(200);
    v_docnum        int;
    v_valueid       clob;
    v_num           INT;
    v_returnstr     clob;
    v_cSql_add      varchar2(3000);
    v_sql           varchar2(2000);
  BEGIN
    --step1: ͨ�����������ȡ��������
    begin
      v_sql := 'select b.pk_accassitem   from bd_accassitem' || v_dblink ||
               '  b where b.code =''' || vi_freevaluetype || '''';
      EXECUTE IMMEDIATE v_sql
        into v_freevaluetype;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        return '';
    END;
    --step2: ͨ��������gl_docfree1�����ĸ��ֶ�
    v_sql := 'SELECT count(1)   FROM FI_FREEMAP' || v_dblink ||
             '  where pk_checktype  = ''' || v_freevaluetype || '''';
    EXECUTE IMMEDIATE v_sql
      into v_num;
  
    if nvl(v_freevaluetype, '999') <> '999' AND v_num = 1 then
      --��ѯȷ����������������gl_docfree1 ���������
      v_sql := ' SELECT num FROM FI_FREEMAP' || v_dblink ||
               '  where pk_checktype =''' || v_freevaluetype || '''';
      EXECUTE IMMEDIATE v_sql
        into v_docnum;
    
      if nvl(vi_valueid, '999') <> '999' then
        --������ڸ����������ֵ
        v_valueid := FN_FREEVALUE_CODE_TO_PK(vi_valueid, v_freevaluetype); --vi_valueid�����ö��ŷָ����磺1001,1002,1003
        if nvl(v_valueid, '999') <> '999' then
          return '���ҵ���ֵ�����쳣';
        end if;
        v_valueid  := replace(v_valueid, ',', ''',''');
        v_cSql_add := ' inner join (
                                             select b.assid,b.F' ||
                      v_docnum ||
                      '  as valueid
                                             from gl_docfree1' ||
                      v_dblink || ' b
                                             where b.dr=0
                                             and b.F' ||
                      v_docnum || ' in (''' || v_valueid || ''')
                                              )  freetable' ||
                      vi_colnum || '
                                             on a.assid = freetable' ||
                      vi_colnum ||
                      '.assid
                                             ';
      else
        v_cSql_add := ' inner join (
                                             select b.assid,b.F' ||
                      v_docnum ||
                      '   as valueid
                                             from gl_docfree1' ||
                      v_dblink || ' b
                                             where  b.dr = 0 and (
                                                        b.F' ||
                      v_docnum ||
                      ' IS NOT NULL
                                                        AND  b.F' ||
                      v_docnum ||
                      '  <> ''NN/A''
                                                        AND  b.F' ||
                      v_docnum ||
                      '  <> ''~''
                                                     )
                                             ) freetable' ||
                      vi_colnum || '
                                             on a.assid = freetable' ||
                      vi_colnum ||
                      '.assid
                                             ';
      end if;
      v_returnstr := v_cSql_add;
    elsif v_num > 1 then
      --��ѯ����б��渨�������쳣ϵͳ�������⣬��ѯ���޸����������
      v_returnstr := '';
    end if;
    --DBMS_OUTPUT.put_line(v_returnstr);
    return v_returnstr;
  END;

  /*
  2.�����������sql��� ƴ�ӹ���
  */
  FUNCTION MAKE_SQL_FREEVALUE(
                              --������������ �����Ӧ��������������(bd_accassitem)�鿴
                              vi_freevaluetype1 in varchar2 default null,
                              --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
                              vi_valueid1 in varchar2 default null,
                              --������������ �����Ӧ��������������(bd_accassitem)�鿴
                              vi_freevaluetype2 in varchar2 default null,
                              --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
                              vi_valueid2 in varchar2 default null,
                              --������������ �����Ӧ��������������(bd_accassitem)�鿴
                              vi_freevaluetype3 in varchar2 default null,
                              --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
                              vi_valueid3 in varchar2 default null,
                              --������������ �����Ӧ��������������(bd_accassitem)�鿴
                              vi_freevaluetype4 in varchar2 default null,
                              --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
                              vi_valueid4       in varchar2 default null,
                              vi_freevaluetype5 in varchar2 default null,
                              --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
                              vi_valueid5 in varchar2 default null)
    RETURN clob is
    sql_add clob;
  BEGIN
    IF nvl(vi_freevaluetype1, '999') <> '999' then
      sql_add := sql_add ||
                 Make_SQL_FREEVALUE_dtl(1, vi_freevaluetype1, vi_valueid1);
    END IF;
    IF nvl(vi_freevaluetype2, '999') <> '999' then
      sql_add := sql_add ||
                 Make_SQL_FREEVALUE_dtl(2, vi_freevaluetype2, vi_valueid2);
    END IF;
    IF nvl(vi_freevaluetype3, '999') <> '999' then
      sql_add := sql_add ||
                 Make_SQL_FREEVALUE_dtl(3, vi_freevaluetype3, vi_valueid3);
    END IF;
    IF nvl(vi_freevaluetype4, '999') <> '999' then
      sql_add := sql_add ||
                 Make_SQL_FREEVALUE_dtl(4, vi_freevaluetype4, vi_valueid4);
    END IF;
    IF nvl(vi_freevaluetype5, '999') <> '999' then
      sql_add := sql_add ||
                 Make_SQL_FREEVALUE_dtl(5, vi_freevaluetype5, vi_valueid5);
    END IF;
    RETURN sql_add;
  END;

  /*
  3.�����������sql���SELECT ���ֶ�ƴ�ӹ���
  */
  FUNCTION MAKE_SQL_FREEVALUE_TITLE(sql_type in int, --0 select��Ӧ�ֶ�ƴ��  1 group by ��Ӧ�ֶ�ƴ��
                                    --������������ �����Ӧ��������������(bd_accassitem)�鿴
                                    vi_freevaluetype1 in varchar2 default null,
                                    --������������ �����Ӧ��������������(bd_accassitem)�鿴
                                    vi_freevaluetype2 in varchar2 default null,
                                    vi_freevaluetype3 in varchar2 default null,
                                    vi_freevaluetype4 in varchar2 default null,
                                    vi_freevaluetype5 in varchar2 default null)
    RETURN VARCHAR2 is
    sql_add clob;
  BEGIN
    IF sql_type = 0 THEN
      --SELECT �ֶ�ƴ��
      IF nvl(vi_freevaluetype1, '999') <> '999' then
        sql_add := sql_add || '''' || vi_freevaluetype1 || '''' ||
                   ', freetable1.valueid,';
      ELSE
        sql_add := sql_add || '''''' || '  freevaluetype1, '''' valueid1,';
      END IF;
    
      IF nvl(vi_freevaluetype2, '999') <> '999' then
        sql_add := sql_add || '''' || vi_freevaluetype2 || '''' ||
                   ', freetable2.valueid,';
      ELSE
        sql_add := sql_add || '''''' || '  freevaluetype2, '''' valueid2,';
      END IF;
    
      IF nvl(vi_freevaluetype3, '999') <> '999' then
        sql_add := sql_add || '''' || vi_freevaluetype3 || '''' ||
                   ', freetable3.valueid,';
      ELSE
        sql_add := sql_add || '''''' || '  freevaluetype3, '''' valueid3,';
      END IF;
    
      IF nvl(vi_freevaluetype4, '999') <> '999' then
        sql_add := sql_add || '''' || vi_freevaluetype4 || '''' ||
                   ', freetable4.valueid,';
      ELSE
        sql_add := sql_add || '''''' || '  freevaluetype4, '''' valueid4,';
      END IF;
      IF nvl(vi_freevaluetype5, '999') <> '999' then
        sql_add := sql_add || '''' || vi_freevaluetype5 || '''' ||
                   ', freetable5.valueid,';
      ELSE
        sql_add := sql_add || '''''' || '  freevaluetype5, '''' valueid5,';
      END IF;
    
    ELSIF sql_type = 1 THEN
      --GROUP BY �ֶ�ƴ��
      IF nvl(vi_freevaluetype1, '999') <> '999' then
        sql_add := sql_add || ',''' || vi_freevaluetype1 || '''' ||
                   ', freetable1.valueid';
      ELSE
        sql_add := sql_add || '';
      END IF;
    
      IF nvl(vi_freevaluetype2, '999') <> '999' then
        sql_add := sql_add || ',''' || vi_freevaluetype2 || '''' ||
                   ', freetable2.valueid';
      ELSE
        sql_add := sql_add || '';
      END IF;
    
      IF nvl(vi_freevaluetype3, '999') <> '999' then
        sql_add := sql_add || ',''' || vi_freevaluetype3 || '''' ||
                   ', freetable3.valueid';
      ELSE
        sql_add := sql_add || '';
      END IF;
    
      IF nvl(vi_freevaluetype4, '999') <> '999' then
        sql_add := sql_add || ',''' || vi_freevaluetype4 || '''' ||
                   ', freetable4.valueid';
      ELSE
        sql_add := sql_add || '';
      END IF;
    
      IF nvl(vi_freevaluetype5, '999') <> '999' then
        sql_add := sql_add || ',''' || vi_freevaluetype5 || '''' ||
                   ', freetable5.valueid';
      ELSE
        sql_add := sql_add || '';
      END IF;
    
    ELSE
      sql_add := '';
    END IF;
    RETURN sql_add;
  END;

  /*  4.��ѯ��Ӧ��Ŀ��ϸƾ֤��¼
  ������
  vi_sdate                   �� ��ѯ��ʼ����
  vi_edate                   �� ��ѯ��β����
  vi_accountingbook  ����ƺ����˲�����
  vi_account                ����ƿ�Ŀ����
  vi_freevaluetype        ����������������������(bd_accassitem)�鿴
  vi_valueid             ����Ӧ����ֵ����
                                  �ɸ���(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
  ʹ�÷�����
   select * from
        table(PAK_GL_CHECK.QUERYDETAIL_CHECK('����,'��ƺ����˲�����','��ƿ�Ŀ����','����������������','��Ӧ����ֵ����'));
  
   ʹ�÷�����
   select * from  table(pkg_finance_query.QUERYDETAIL_CHECK( '@gdxc','2017-11-01',
        '2017-11-01',
        '224102%',
        '981-0001', --�˲� ѡ���Ĭ��ȫѡ
        '601102',--�Է���Ŀ ѡ���Ĭ��ȫѡ
        '0001' , -- �������㵵������ ��Ĭ�ϲ�ѡ
        '10001'  -- �������㵵��ֵ���� ��Ĭ��ȫѡ
        )
        )
  
  */
  FUNCTION QUERYDETAIL_CHECK(
                             vi_sdate          in varchar2, --��ʼ����
                             vi_edate          in varchar2, --��������
                             vi_account        in varchar2, --��ƿ�Ŀ
                             vi_accountingbook in varchar2 default null, --��ƺ����˲�
                             vi_oppaccount     in varchar2 default null, --�Է���Ŀ
                             --������������ �����Ӧ��������������(bd_accassitem)�鿴
                             vi_freevaluetype1 in varchar2 default null,
                             --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
                             vi_valueid1 in varchar2 default null,
                             --������������ �����Ӧ��������������(bd_accassitem)�鿴
                             vi_freevaluetype2 in varchar2 default null,
                             --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
                             vi_valueid2 in varchar2 default null,
                             --������������ �����Ӧ��������������(bd_accassitem)�鿴
                             vi_freevaluetype3 in varchar2 default null,
                             --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
                             vi_valueid3 in varchar2 default null,
                             --������������ �����Ӧ��������������(bd_accassitem)�鿴
                             vi_freevaluetype4 in varchar2 default null,
                             --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
                             vi_valueid4       in varchar2 default null,
                             vi_freevaluetype5 in varchar2 default null,
                             --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
                             vi_valueid5 in varchar2 default null)
    RETURN QueryDETAIL
    PIPELINED IS
    L_RESULT DETAIL_ROW;
    TYPE ref_cursor_type IS REF CURSOR; --����һ����̬�α�
    rowrecord         ref_cursor_type;
    begin_year        varchar2(20) := substr(vi_sdate, 1, 4); --�ڳ����
    v_cSql            varchar2(3000);
    v_cSql_add        varchar2(3000) := '';
    v_sqltitle        varchar2(1000);
    v_sqljoin         clob;
    v_soppaccount     varchar2(3000); --�Է���Ŀɸѡ�����ַ���ƴ��
    v_account         varchar2(3000); --��Ŀɸѡ�����ַ���ƴ��
    v_count           int := 0;
    v_accountbook_str varchar2(3000); --�����˲�����ƴ��
    v_pk_accchart  varchar2(20); --��Ŀ��ϵ����
  BEGIN
     --��ȡ��Ŀ��ϵ����
    SELECT pk_accchart
    INTO v_pk_accchart
    FROM bd_accchart
    WHERE  CODE = accchart_code;
    
    v_sqltitle    := MAKE_SQL_FREEVALUE_TITLE(0,
                                              vi_freevaluetype1,
                                              vi_freevaluetype2,
                                              vi_freevaluetype3,
                                              vi_freevaluetype4);
    v_sqljoin     := Make_SQL_FREEVALUE(vi_freevaluetype1,
                                        vi_valueid1,
                                        vi_freevaluetype2,
                                        vi_valueid2,
                                        vi_freevaluetype3,
                                        vi_valueid3,
                                        vi_freevaluetype4,
                                        vi_valueid4,
                                        vi_freevaluetype5,
                                        vi_valueid5);
    v_soppaccount := FN_OPPOSITESUBJ_CODE_TO_PK(vi_oppaccount);
  
    IF NVL(vi_accountingbook, '999') <> '999' THEN
      v_accountbook_str := ' and b.code in (''' ||
                           replace(vi_accountingbook, ',', ''',''') ||
                           ''')';
    ELSE
      v_accountbook_str := ' ';
    END IF;
  
    IF INSTR(vi_account, '%') > 0 THEN
      --%LIKE ��ʽ
      v_account := ' like (''' || replace(vi_account, ',', ''',''') ||
                   ''')';
      SELECT NVL(LENGTH(REGEXP_REPLACE(REPLACE(vi_account, '%', '@'),
                                       '[^@]+',
                                       '')),
                 0)
        into v_count
        FROM DUAL;
      IF v_count > 1 THEN
        DBMS_OUTPUT.put_line('�����Ŀ��������!ֻ����һ�����ٷֺŲ���');
        return;
      END IF;
    ELSE
      v_account := ' in (''' || replace(vi_account, ',', ''',''') || ''')';
    END IF;
  
    v_cSql := 'SELECT
                                a.pk_voucher   , --ƾ֤����
                                a.nov    ,--ƾ֤����
                                a.pk_org  ,  --����ҵ��Ԫ
                                a.pk_group , --��������
                                a.pk_accountingbook  , --���������˲�
                                a.accountcode  , --������ƿ�Ŀ
                                a.yearv  , --�����ڼ�-��
                                a.periodv  , --�����ڼ�-��
                                a.adjustperiod  , --�����ڼ�-��
                                a.prepareddatev , --�Ƶ�����
                                a.oppositesubj,
                                case when e.balanorient = ''1'' then ''����''
                                     when e.balanorient = ''0'' then ''�跽''
                                     else ''''
                                end , --��Ŀ���� 1:���� 0:�跽
                                --���������ֶ���
                                ' || v_sqltitle || '
                                a.pk_currtype  , --����
                                a.excrate2   ,----����ֵ
                                a.explanation  , --ժҪ
                                a.localdebitamount   ,--���ҽ跽������
                                a.debitquantity   ,--�跽��������
                                a.localcreditamount ,--���Ҵ���������
                                a.creditquantity  --������������
                            from gl_detail' || v_dblink || ' a
                            inner join org_accountingbook' || v_dblink || ' b
                            on a.pk_accountingbook = b.pk_accountingbook
                            inner join org_orgs' || v_dblink || ' c
                            on a.pk_org = c.pk_org
                            inner join org_group' || v_dblink || ' d
                            on a.pk_group = d.pk_group
                            inner join bd_account' || v_dblink || ' e
                            on a.pk_account = e.pk_account and e.pk_accchart = ''' || v_pk_accchart || '''
                            ' || v_sqljoin || '
                            where a.dr = 0
                                       ' || v_soppaccount || '
                                       --and substr(a.prepareddatev,1,10) >= ''' ||
              vi_sdate || '''
                                       and substr(a.prepareddatev,1,10) <= ''' ||
              vi_edate || '''
                                       ' || v_accountbook_str ||
              '  --�˲�
                                       and e.code ' ||
              v_account ||
              ' --��Ŀ
                                       and a.adjustperiod >= ''00''
                                       and a.yearv >= ''' ||
              begin_year || '''
                                        ' || v_cSql_add;
    --DBMS_OUTPUT.put_line(v_cSql);
    OPEN rowrecord FOR v_cSql;
    LOOP
      FETCH rowrecord
        into L_RESULT;
      EXIT WHEN rowrecord%NOTFOUND;
      PIPE ROW(L_RESULT); --���η�����
    END LOOP;
    CLOSE rowrecord;
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.put_line(v_cSql);
      dbms_output.put_line(dbms_utility.format_error_backtrace);
      ROLLBACK;
      RAISE;
  END;

  /*5.  ��ѯ��Ӧ��Ŀ���
  ������
  vi_date                   �� ��ѯ����
  vi_accountingbook  ����ƺ����˲�����
  vi_account                ����ƿ�Ŀ����
                                    �ɴ���1001% ��ѯ��Ӧ���¼���Ŀ��Ŀ�������Դ��� 1001%,1002%��
                                    �ɴ���10010��10011��200101���ŷָ���ϸ��Ŀ������
  vi_freevaluetype        �������������ͱ������(bd_accassitem)�鿴
  vi_valueid             ����Ӧ����ֵ����
                                  �ɸ���(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
   ��ѯ������
   select * from  table(pkg_finance_query.QUERYDETAIL_CHECK_SUM('@gdxc', --DBlink
           '2017-11-01', --��ʼ����
          '2017-11-01',
          '224102%',  --��Ŀ����
          '981-0001', --�˲� ѡ���Ĭ��ȫѡ
          '601102',--�Է���Ŀ ѡ���Ĭ��ȫѡ
          '0001' , -- �������㵵������ ��Ĭ�ϲ�ѡ
          '10001'  -- �������㵵��ֵ���� ��Ĭ��ȫѡ
          )
          )
  */
  FUNCTION QUERYDETAIL_CHECK_SUM(
                                 vi_sdate          in varchar2, --��ʼ����
                                 vi_edate          in varchar2, --��������
                                 vi_account        in varchar2, --��ƿ�Ŀ
                                 vi_accountingbook in varchar2 default null, --��ƺ����˲�
                                 vi_oppaccount     in varchar2 default null, --�Է���Ŀ
                                 --������������ �����Ӧ��������������(bd_accassitem)�鿴
                                 vi_freevaluetype1 in varchar2 default null,
                                 --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
                                 vi_valueid1 in varchar2 default null,
                                 --������������ �����Ӧ��������������(bd_accassitem)�鿴
                                 vi_freevaluetype2 in varchar2 default null,
                                 --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
                                 vi_valueid2 in varchar2 default null,
                                 --������������ �����Ӧ��������������(bd_accassitem)�鿴
                                 vi_freevaluetype3 in varchar2 default null,
                                 --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
                                 vi_valueid3 in varchar2 default null,
                                 --������������ �����Ӧ��������������(bd_accassitem)�鿴
                                 vi_freevaluetype4 in varchar2 default null,
                                 --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
                                 vi_valueid4       in varchar2 default null,
                                 vi_freevaluetype5 in varchar2 default null,
                                 --������������  �����Ӧ����ֵ��������(bd_accassitem)��classid�ֶε�md_class�в��Ҷ�Ӧ������
                                 vi_valueid5 in varchar2 default null)
    RETURN QueryDETAIL_SUM
    PIPELINED IS
    L_RESULT DETAIL_ROW_SUM;
    TYPE ref_cursor_type IS REF CURSOR; --����һ����̬�α�
    rowrecord              ref_cursor_type;
    v_cSql                 clob;
    begin_year             varchar2(20) := substr(vi_sdate, 1, 4); --�ڳ����
    v_sqltitle             varchar2(2000);
    v_sqlgroup             varchar2(2000);
    v_sqljoin              clob;
    v_soppaccount          varchar2(3000); --�Է���Ŀɸѡ�����ַ���ƴ��
    v_account              varchar2(3000); --��Ŀɸѡ�����ַ���ƴ��
    v_accountcodefile      varchar2(200); --��Ŀ�ֶ������⴦��
    v_accountcodefilegroup varchar2(200);
    v_count                int := 0;
    v_accountbook_str      varchar2(3000); --�����˲�����ƴ��
    v_pk_accchart  varchar2(20); --��Ŀ��ϵ����
  BEGIN
    --��ȡ��Ŀ��ϵ����
    SELECT pk_accchart
    INTO v_pk_accchart
    FROM bd_accchart
    WHERE  CODE = accchart_code;
    
    v_sqltitle    := MAKE_SQL_FREEVALUE_TITLE(0,
                                              vi_freevaluetype1,
                                              vi_freevaluetype2,
                                              vi_freevaluetype3,
                                              vi_freevaluetype4,
                                              vi_freevaluetype5);
    v_sqlgroup    := MAKE_SQL_FREEVALUE_TITLE(1,
                                              vi_freevaluetype1,
                                              vi_freevaluetype2,
                                              vi_freevaluetype3,
                                              vi_freevaluetype4,
                                              vi_freevaluetype5);
    v_sqljoin     := Make_SQL_FREEVALUE(vi_freevaluetype1,
                                        vi_valueid1,
                                        vi_freevaluetype2,
                                        vi_valueid2,
                                        vi_freevaluetype3,
                                        vi_valueid3,
                                        vi_freevaluetype4,
                                        vi_valueid4,
                                        vi_freevaluetype5,
                                        vi_valueid5);
    v_soppaccount := FN_OPPOSITESUBJ_CODE_TO_PK(vi_oppaccount);
  
    IF NVL(vi_accountingbook, '999') <> '999' THEN
      v_accountbook_str := ' and b.code in (''' ||
                           replace(vi_accountingbook, ',', ''',''') ||
                           ''')';
    ELSE
      v_accountbook_str := ' ';
    END IF;
  
    IF INSTR(vi_account, '%') > 0 THEN
      --%LIKE ��ʽ
      v_account              := ' like (''' ||
                                replace(vi_account, ',', ''',''') || ''')';
      v_accountcodefile      := '''' || replace(vi_account, '%', '') ||
                                ''' accountcode';
      v_accountcodefilegroup := '';
      SELECT NVL(LENGTH(REGEXP_REPLACE(REPLACE(vi_account, '%', '@'),
                                       '[^@]+',
                                       '')),
                 0)
        into v_count
        FROM DUAL;
      IF v_count > 1 THEN
        DBMS_OUTPUT.put_line('�����Ŀ��������!ֻ����һ�����ٷֺŲ���');
        return;
      END IF;
    ELSE
      v_account              := ' in (''' ||
                                replace(vi_account, ',', ''',''') || ''')';
      v_accountcodefile      := 'a.accountcode';
      v_accountcodefilegroup := ',' || v_accountcodefile;
    END IF;
  
    v_cSql := 'SELECT
                                c.code org_code ,  --����ҵ��Ԫ
                                d.code group_code , --��������
                                b.code accountbookcode, --���������˲�
                                ' || v_accountcodefile ||
              ' , --������ƿ�Ŀ
                                --���������ֶ���
                                ' || v_sqltitle || '
                                COUNT(1) detal_count,
                                SUM(case when substr(a.prepareddatev,1,10) >=  ''' ||
              vi_sdate || '''
                                                   and substr(a.prepareddatev,1,10) <=  ''' ||
              vi_edate || '''
                                         then a.localdebitamount
                                         else 0 end
                                         )  localdebitamount,--�����ۼƱ��ҽ跽������
                                SUM(case when substr(a.prepareddatev,1,10) >= ''' ||
              vi_sdate || '''
                                                   and substr(a.prepareddatev,1,10) <= ''' ||
              vi_edate || '''
                                         then a.debitquantity
                                         else 0 end
                                         ) debitquantity ,--�ۼƽ跽��������
                                SUM(case when substr(a.prepareddatev,1,10) >=  ''' ||
              vi_sdate || '''
                                                   and substr(a.prepareddatev,1,10) <=  ''' ||
              vi_edate || '''
                                         then a.localcreditamount
                                         else 0 end
                                         )  localcreditamount ,--�����ۼƱ��Ҵ���������
                                SUM(case when substr(a.prepareddatev,1,10) >=  ''' ||
              vi_sdate || '''
                                                   and substr(a.prepareddatev,1,10) <=  ''' ||
              vi_edate || '''
                                         then a.creditquantity
                                         else 0 end
                                         )  creditquantity ,--�ۼƴ�����������
                                SUM(case when a.adjustperiod >=''00''
                                         then a.localdebitamount
                                         else 0 end
                                         )  sumdebitamount,--�ۼƱ��ҽ跽������
                                SUM(case when  a.adjustperiod >=''00''
                                         then a.localcreditamount
                                         else 0 end
                                         )  sumcreditamount ,--�ۼƱ��Ҵ���������
                                SUM( case when substr(a.prepareddatev,1,10) < ''' ||
              vi_sdate ||
              ''' and e.balanorient= ''0'' then --�跽    1:���� 0:�跽
                                                                            a.localdebitamount - a.localcreditamount
                                          when substr(a.prepareddatev,1,10) < ''' ||
              vi_sdate ||
              ''' and e.balanorient = ''1'' then --����
                                                                            a.localcreditamount - a.localdebitamount
                                          else 0 end
                                ) beginning_balances, --�ڳ����
                                SUM( case when substr(a.prepareddatev,1,10) <= ''' ||
              vi_edate ||
              ''' and e.balanorient = ''0'' then --�跽
                                                                            a.localdebitamount - a.localcreditamount
                                                  when substr(a.prepareddatev,1,10) <= ''' ||
              vi_edate ||
              ''' and e.balanorient = ''1'' then --����
                                                                             a.localcreditamount - a.localdebitamount
                                          else 0 end
                                ) Final_balance,  --��ĩ���
                                SUM(case when substr(a.prepareddatev,1,10) <= ''' ||
              vi_edate || ''' and substr(a.prepareddatev,1,10) >= ''' ||
              vi_sdate || '''
                                                           and e.balanorient = ''0'' then --�跽
                                                           a.localdebitamount - a.localcreditamount
                                                  when substr(a.prepareddatev,1,10) <= ''' ||
              vi_edate || ''' and substr(a.prepareddatev,1,10) >= ''' ||
              vi_sdate || '''
                                                           and e.balanorient = ''1'' then --����
                                                           a.localcreditamount - a.localdebitamount
                                          else 0 end
                                ) Final_amount --������
                            from gl_detail' || v_dblink || ' a
                            inner join org_accountingbook' ||  v_dblink || ' b
                            on a.pk_accountingbook = b.pk_accountingbook
                            inner join org_orgs' || v_dblink || ' c
                            on a.pk_org = c.pk_org
                            inner join org_group' || v_dblink || ' d
                            on a.pk_group = d.pk_group
                            inner join bd_account' || v_dblink || ' e
                            on a.pk_account = e.pk_account and e.pk_accchart = ''' || v_pk_accchart || '''
                            ' || v_sqljoin || '
                            where a.dr = 0
                                       ' || v_soppaccount || '
                                       and substr(a.prepareddatev,1,10) <= ''' ||
              vi_edate || '''
                                       ' || v_accountbook_str ||
              ' --�˲�
                                       and e.code ' ||
              v_account ||
              ' --��Ŀ
                                       and a.adjustperiod >= ''00''
                                       and a.yearv >= ''' ||
              begin_year || '''
                              group by   c.code  ,  --����ҵ��Ԫ
                                                d.code  , --��������
                                                b.code    --���������˲�
                                                ' ||
              v_accountcodefilegroup ||
              ' --������ƿ�Ŀ
                                                --���������ֶ���
                                                ' ||
              v_sqlgroup || '
                           ';
    --DBMS_OUTPUT.put_line(v_cSql);
    OPEN rowrecord FOR v_cSql;
    LOOP
      FETCH rowrecord
        into L_RESULT;
      EXIT WHEN rowrecord%NOTFOUND;
      PIPE ROW(L_RESULT); --���η�����
    END LOOP;
    CLOSE rowrecord;
  
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.put_line(v_cSql);
      dbms_output.put_line(dbms_utility.format_error_backtrace);
      ROLLBACK;
      RAISE;
  END;

END PKG_FINANCE_QUERY;
/


prompt Done
spool off
set define on
