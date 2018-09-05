prompt PL/SQL Developer Export User Objects for user NCTEST@LINUX_ORCL
prompt Created by wangwei on 2018年9月3日
set define off
spool PKG_FINANCE_QUERY.log

prompt
prompt Creating package PKG_FINANCE_QUERY
prompt ==================================
prompt
CREATE OR REPLACE PACKAGE PKG_FINANCE_QUERY AS
--财务发生额余额查询
  /***********
  *TYPE DEFINE
  *类型定义
  ************/
  /* 全局变量*/
  /*TYPE TYP_RECURSOR IS REF CURSOR; */
  v_dblink varchar2(200):=''; --如：@gdxc
  accchart_code VARCHAR2(200) := '0001'; --科目分类编码 用于获取对方科目使用

/*
用于声明查询凭证明细的类型
*/
   type DETAIL_ROW is RECORD
    (
      pk_voucher  varchar2(20) , --凭证主键
      nov  number(38) ,--凭证编码
      pk_org  varchar2(20),  --所属业务单元
      pk_group varchar2(20), --所属集团
      pk_accountingbook char(20), --所属核算账簿
      accountcode varchar2(40), --所属会计科目
      yearv varchar2(4), --所属期间-年
      periodv varchar2(2), --所属期间-月
      adjustperiod varchar2(3), --调整期间-月
      prepareddatev varchar2(19), --制单日期
      opppsitesubj varchar2(200), --对方科目
      direction varchar2(19),--科目方向
      freevaluetype1 varchar2(20),--辅助核算档案编码1
      valueid1 varchar2(20),--所属辅助核算值1
      freevaluetype2 varchar2(20),--辅助核算档案编码2
      valueid2 varchar2(20),--所属辅助核算值2
      freevaluetype3 varchar2(20),--辅助核算档案编码3
      valueid3 varchar2(20),--所属辅助核算值3
      freevaluetype4 varchar2(20),--辅助核算档案编码4
      valueid4 varchar2(20),--所属辅助核算值4
      freevaluetype5 varchar2(20),--辅助核算档案编码5
      valueid5 varchar2(20),--所属辅助核算值5
      pk_currtype varchar2(20), --币种
      excrate2 number(15,8) ,----汇率值
      explanation varchar2(300), --摘要
      localdebitamount number(28,8) ,--本币借方发生额
      debitquantity number(28,8) ,--借方发生数量
      localcreditamount number(28,8),--本币贷方发生额
      creditquantity number(20,8) --贷方发生数量
    );
  /*
用于声明查询凭证余额的类型
*/
  TYPE DETAIL_ROW_SUM IS RECORD
    (
      pk_org  varchar2(20),  --所属业务单元
      pk_group varchar2(20), --所属集团
      pk_accountingbook char(20), --所属核算账簿
      accountcode varchar2(40), --所属会计科目
      freevaluetype1 varchar2(20),--辅助核算档案编码1
      valueid1 varchar2(20),--所属辅助核算值1
      freevaluetype2 varchar2(20),--辅助核算档案编码2
      valueid2 varchar2(20),--所属辅助核算值2
      freevaluetype3 varchar2(20),--辅助核算档案编码3
      valueid3 varchar2(20),--所属辅助核算值3
      freevaluetype4 varchar2(20),--辅助核算档案编码4
      valueid4 varchar2(20),--所属辅助核算值4
      freevaluetype5 varchar2(20),--辅助核算档案编码5
      valueid5 varchar2(20),--所属辅助核算值5
      detal_count INT, --凭证数量
      localdebitamount number(28,8) ,--本期累计本币借方发生额
      debitquantity number(28,8) ,--本期累计借方发生数量
      localcreditamount number(28,8),--本期累计本币贷方发生额
      creditquantity number(20,8), --本期累计贷方发生数量
      sumdebitamount number(28,8) ,--累计本币借方发生额
      sumcreditamount number(28,8) ,--累计本币贷方发生额
      beginning_balances  number(20,8), --期初余额
      Final_balance  number(20,8), --期末余额
      Final_amount  number(20,8)--发生额
    );
 


  TYPE QueryDETAIL IS TABLE OF DETAIL_ROW;--自定义table类
  TYPE QueryDETAIL_SUM IS TABLE OF DETAIL_ROW_SUM;--自定义table类

  /**返回用分隔符号分隔的第几个字符串 **/
  FUNCTION FN_GET_STRLIST(P_STR_LIST IN CLOB, P_SPLIT IN VARCHAR2, P_STR_NUM IN NUMBER) RETURN VARCHAR2;
           PRAGMA RESTRICT_REFERENCES(FN_GET_STRLIST,RNPS,WNDS,WNPS);

  /**基础档案编码转主键 返回用逗号分隔符号分隔的主键**/
  FUNCTION FN_FREEVALUE_CODE_TO_PK(vi_code IN CLOB,
   vi_tablecode IN varchar2 --档案表编码 编码位于表bd_accassitem
   )RETURN CLOB;

  FUNCTION FN_OPPOSITESUBJ_CODE_TO_PK(vi_code in varchar2)
  RETURN VARCHAR2;
 /**1. 单个辅助核算关联sql语句拼接功能**/
  FUNCTION MAKE_SQL_FREEVALUE_DTL(
        vi_colnum IN INT,
        vi_freevaluetype in varchar2 default null,
        --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
        vi_valueid in varchar2 default null
        --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
    )
  RETURN clob;
/**2. 辅助核算关联sql语句 拼接功能**/
  FUNCTION Make_SQL_FREEVALUE(
  --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
        vi_freevaluetype1 in varchar2 default null,
        --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
        vi_valueid1 in varchar2 default null,
        --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
        vi_freevaluetype2 in varchar2 default null,
        --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
        vi_valueid2 in varchar2 default null,
        --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
        vi_freevaluetype3 in varchar2 default null,
        --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
        vi_valueid3 in varchar2 default null,
        --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
        vi_freevaluetype4 in varchar2 default null,
        --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
        vi_valueid4 in varchar2 default null,
        vi_freevaluetype5 in varchar2 default null,
      --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
        vi_valueid5 in varchar2 default null
  )
  RETURN clob;
/**3. 辅助核算关联sql语句SELECT 的字段拼接功能**/
  FUNCTION MAKE_SQL_FREEVALUE_TITLE(
     sql_type in int, --0 select对应字段拼接  1 group by 对应字段拼接
  --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
      vi_freevaluetype1 in varchar2 default null,
      --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
      vi_freevaluetype2 in varchar2 default null,
      vi_freevaluetype3 in varchar2 default null,
      vi_freevaluetype4 in varchar2 default null,
      vi_freevaluetype5 in varchar2 default null
  )
  RETURN VARCHAR2;
 /**4. 返回对应科目及辅助核算的凭证明细**/
  FUNCTION QUERYDETAIL_CHECK(
      vi_sdate in varchar2,--起始日期
      vi_edate in varchar2,--结束日期
      vi_account in varchar2,--会计科目
      vi_accountingbook in varchar2 default null ,--会计核算账簿
      vi_oppaccount in varchar2 default null, --对方科目
      --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
      vi_freevaluetype1 in varchar2 default null,
      --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
      vi_valueid1 in varchar2 default null,
      --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
      vi_freevaluetype2 in varchar2 default null,
      --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
      vi_valueid2 in varchar2 default null,
      --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
      vi_freevaluetype3 in varchar2 default null,
      --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
      vi_valueid3 in varchar2 default null,
      --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
      vi_freevaluetype4 in varchar2 default null,
      --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
      vi_valueid4 in varchar2 default null,
      vi_freevaluetype5 in varchar2 default null,
      --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
      vi_valueid5 in varchar2 default null
     )
      RETURN QueryDETAIL  --返回table类型
      PIPELINED; --流水式

  /**5. 返回对应科目及辅助核算的科目余额**/
  FUNCTION QUERYDETAIL_CHECK_SUM(
      vi_sdate in varchar2,--起始日期
      vi_edate in varchar2,--结束日期
      vi_account in varchar2,--会计科目
      vi_accountingbook in varchar2  default null,--会计核算账簿
      vi_oppaccount in varchar2 default null, --对方科目
      vi_freevaluetype1 in varchar2 default null,
      --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
      vi_valueid1 in varchar2 default null,
      --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
      vi_freevaluetype2 in varchar2 default null,
      --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
      vi_valueid2 in varchar2 default null,
      --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
      vi_freevaluetype3 in varchar2 default null,
      --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
      vi_valueid3 in varchar2 default null,
      --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
      vi_freevaluetype4 in varchar2 default null,
      --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
      vi_valueid4 in varchar2 default null,
      vi_freevaluetype5 in varchar2 default null,
      --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
      vi_valueid5 in varchar2 default null
     )
      RETURN QueryDETAIL_SUM  --返回table类型
      PIPELINED; --流水式

END PKG_FINANCE_QUERY;
/

prompt
prompt Creating package body PKG_FINANCE_QUERY
prompt =======================================
prompt
CREATE OR REPLACE PACKAGE BODY PKG_FINANCE_QUERY --财务凭证余额及发生额查询
 AS

  /*
  如下表需要通过DBlink进行访问，需要开通相应的读取权限。
  gl_detail
  org_accountingbook
  org_orgs
  org_group
  bd_account
  gl_docfree1
  FI_FREEMAP
  bd_accassitem
  bd_accasoa
  各种辅助核算引用的档案*/

  /**.返回用分隔符号分隔的第几个字符串
  P_STR_LIST=传入字符串
  P_SPLIT=分隔符号
  P_STR_NUM=需要返回的第几个字符串
  V_STR=返回对应的字符串
  **/
  FUNCTION FN_GET_STRLIST(P_STR_LIST IN CLOB,
                          P_SPLIT    IN VARCHAR2,
                          P_STR_NUM  IN NUMBER) RETURN VARCHAR2 IS
    --STR_LIST 拼接的字符串，用分号隔开   STR_NUM 返回第几个值
    V_STR      CLOB := '';
    n_length   number := 0;
    Star_index number := 0;
    v_Count    number := 0;
  BEGIN
    --判断当前字符串有几个分隔符
    SELECT NVL(LENGTH(REGEXP_REPLACE(REPLACE(P_STR_LIST, P_SPLIT, '@'),
                                     '[^@]+',
                                     '')),
               0)
      INTO v_Count
      FROM DUAL;
  
    IF P_STR_NUM <= 0 OR P_STR_NUM > v_Count + 1 THEN
      V_STR := ''; --输入的第n个元素不存在
    ELSIF P_STR_NUM = 1 AND v_Count = 0 THEN
      --只有一个元素的情况
      V_STR := P_STR_LIST;
    ELSIF P_STR_NUM = v_Count + 1 THEN
      ---取最后一个元素
      Star_index := INSTR(TO_CHAR(P_STR_LIST), P_SPLIT, 1, P_STR_NUM - 1) +
                    LENGTH(P_SPLIT);
      V_STR      := substr(TO_CHAR(P_STR_LIST), Star_index, 90000);
    ELSIF P_STR_NUM = 1 and v_Count > 0 THEN
      ---第一个元素
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

  /**基础档案编码转主键 返回用逗号分隔符号分隔的主键**/
  FUNCTION FN_FREEVALUE_CODE_TO_PK(vi_code      IN CLOB,
                                   vi_tablecode IN varchar2 --档案表编码 编码位于表bd_accassitem
                                   ) RETURN CLOB IS
    v_pkstr     CLOB; --返回的pk
    v_tablename varchar2(200); --档案表名
    v_pkfield   varchar2(200); --档案表编码字段名
    v_sql       varchar2(3000);
  begin
  
    begin
      --获取档案的表名
      select b.defaulttablename
        into v_tablename
        from bd_accassitem a
        left join md_class b
          on a.classid = b.id
       where a.code = vi_tablecode;
    
      --获取档案的主键字段名
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
  
    /* ====特别注意：    默认所有档案的编码字段名都是code ,例如像项目档案，
                    编码字段特殊此处做特殊逻辑判断。
       ======
    */
    v_sql := 'select wm_concat(' || v_pkfield || ') from ' || v_tablename ||
             v_dblink || ' where code in (''' ||
             replace(vi_code, ',', ''',''') || ''')';
    EXECUTE IMMEDIATE v_sql
      into v_pkstr;
    return v_pkstr;
  end;
  /**对方科目编码转主键 返回用逗号分隔符号分隔的主键**/
  FUNCTION FN_OPPOSITESUBJ_CODE_TO_PK(vi_code in varchar2) RETURN VARCHAR2 IS
    TYPE ref_cursor_type IS REF CURSOR; --定义一个动态游标
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
      ROW_ID       := ROW_ID + 1; --用于记录处理行数
      v_return_str := v_return_str || 'or a.oppositesubj like ''%' || v_pk ||
                      '%'' ';
    END LOOP;
    CLOSE rowdata;
  
    IF NVL(vi_code, '999') <> '999' AND NVL(v_return_str, '999') <> '999' THEN
      v_return_str := 'and ( ' || substr(v_return_str, 3) || ')';
    ELSE
      v_return_str := '--无该科目编码';
    END IF;
  
    return v_return_str;
  
  END;
  /*
   1.单个辅助核算关联sql语句拼接功能
  */
  FUNCTION MAKE_SQL_FREEVALUE_DTL(vi_colnum        IN INT,
                                  vi_freevaluetype in varchar2 default null,
                                  --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
                                  vi_valueid in varchar2 default null
                                  --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
                                  ) RETURN clob is
    v_freevaluetype varchar2(200);
    v_docnum        int;
    v_valueid       clob;
    v_num           INT;
    v_returnstr     clob;
    v_cSql_add      varchar2(3000);
    v_sql           varchar2(2000);
  BEGIN
    --step1: 通过档案编码获取档案主键
    begin
      v_sql := 'select b.pk_accassitem   from bd_accassitem' || v_dblink ||
               '  b where b.code =''' || vi_freevaluetype || '''';
      EXECUTE IMMEDIATE v_sql
        into v_freevaluetype;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        return '';
    END;
    --step2: 通过档案在gl_docfree1所属哪个字段
    v_sql := 'SELECT count(1)   FROM FI_FREEMAP' || v_dblink ||
             '  where pk_checktype  = ''' || v_freevaluetype || '''';
    EXECUTE IMMEDIATE v_sql
      into v_num;
  
    if nvl(v_freevaluetype, '999') <> '999' AND v_num = 1 then
      --查询确定辅助核算类型在gl_docfree1 保存的列名
      v_sql := ' SELECT num FROM FI_FREEMAP' || v_dblink ||
               '  where pk_checktype =''' || v_freevaluetype || '''';
      EXECUTE IMMEDIATE v_sql
        into v_docnum;
    
      if nvl(vi_valueid, '999') <> '999' then
        --传入存在辅助核算编码值
        v_valueid := FN_FREEVALUE_CODE_TO_PK(vi_valueid, v_freevaluetype); --vi_valueid编码用逗号分隔，如：1001,1002,1003
        if nvl(v_valueid, '999') <> '999' then
          return '查找档案值出现异常';
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
      --查询多个列保存辅助核算异常系统出现问题，查询按无辅助核算进行
      v_returnstr := '';
    end if;
    --DBMS_OUTPUT.put_line(v_returnstr);
    return v_returnstr;
  END;

  /*
  2.辅助核算关联sql语句 拼接功能
  */
  FUNCTION MAKE_SQL_FREEVALUE(
                              --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
                              vi_freevaluetype1 in varchar2 default null,
                              --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
                              vi_valueid1 in varchar2 default null,
                              --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
                              vi_freevaluetype2 in varchar2 default null,
                              --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
                              vi_valueid2 in varchar2 default null,
                              --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
                              vi_freevaluetype3 in varchar2 default null,
                              --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
                              vi_valueid3 in varchar2 default null,
                              --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
                              vi_freevaluetype4 in varchar2 default null,
                              --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
                              vi_valueid4       in varchar2 default null,
                              vi_freevaluetype5 in varchar2 default null,
                              --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
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
  3.辅助核算关联sql语句SELECT 的字段拼接功能
  */
  FUNCTION MAKE_SQL_FREEVALUE_TITLE(sql_type in int, --0 select对应字段拼接  1 group by 对应字段拼接
                                    --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
                                    vi_freevaluetype1 in varchar2 default null,
                                    --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
                                    vi_freevaluetype2 in varchar2 default null,
                                    vi_freevaluetype3 in varchar2 default null,
                                    vi_freevaluetype4 in varchar2 default null,
                                    vi_freevaluetype5 in varchar2 default null)
    RETURN VARCHAR2 is
    sql_add clob;
  BEGIN
    IF sql_type = 0 THEN
      --SELECT 字段拼接
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
      --GROUP BY 字段拼接
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

  /*  4.查询对应科目明细凭证分录
  参数：
  vi_sdate                   ： 查询起始日期
  vi_edate                   ： 查询结尾日期
  vi_accountingbook  ：会计核算账簿主键
  vi_account                ：会计科目主键
  vi_freevaluetype        ：辅助核算类型主键可在(bd_accassitem)查看
  vi_valueid             ：对应档案值主键
                                  可根据(bd_accassitem)中classid字段到md_class中查找对应档案表
  使用方法：
   select * from
        table(PAK_GL_CHECK.QUERYDETAIL_CHECK('日期,'会计核算账簿主键','会计科目主键','辅助核算类型主键','对应档案值主键'));
  
   使用范例：
   select * from  table(pkg_finance_query.QUERYDETAIL_CHECK( '@gdxc','2017-11-01',
        '2017-11-01',
        '224102%',
        '981-0001', --账簿 选填，空默认全选
        '601102',--对方科目 选填，空默认全选
        '0001' , -- 辅助核算档案编码 空默认不选
        '10001'  -- 辅助核算档案值编码 空默认全选
        )
        )
  
  */
  FUNCTION QUERYDETAIL_CHECK(
                             vi_sdate          in varchar2, --起始日期
                             vi_edate          in varchar2, --结束日期
                             vi_account        in varchar2, --会计科目
                             vi_accountingbook in varchar2 default null, --会计核算账簿
                             vi_oppaccount     in varchar2 default null, --对方科目
                             --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
                             vi_freevaluetype1 in varchar2 default null,
                             --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
                             vi_valueid1 in varchar2 default null,
                             --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
                             vi_freevaluetype2 in varchar2 default null,
                             --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
                             vi_valueid2 in varchar2 default null,
                             --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
                             vi_freevaluetype3 in varchar2 default null,
                             --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
                             vi_valueid3 in varchar2 default null,
                             --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
                             vi_freevaluetype4 in varchar2 default null,
                             --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
                             vi_valueid4       in varchar2 default null,
                             vi_freevaluetype5 in varchar2 default null,
                             --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
                             vi_valueid5 in varchar2 default null)
    RETURN QueryDETAIL
    PIPELINED IS
    L_RESULT DETAIL_ROW;
    TYPE ref_cursor_type IS REF CURSOR; --定义一个动态游标
    rowrecord         ref_cursor_type;
    begin_year        varchar2(20) := substr(vi_sdate, 1, 4); --期初年份
    v_cSql            varchar2(3000);
    v_cSql_add        varchar2(3000) := '';
    v_sqltitle        varchar2(1000);
    v_sqljoin         clob;
    v_soppaccount     varchar2(3000); --对方科目筛选条件字符串拼接
    v_account         varchar2(3000); --科目筛选条件字符串拼接
    v_count           int := 0;
    v_accountbook_str varchar2(3000); --核算账簿条件拼接
    v_pk_accchart  varchar2(20); --科目体系主键
  BEGIN
     --获取科目体系主键
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
      --%LIKE 形式
      v_account := ' like (''' || replace(vi_account, ',', ''',''') ||
                   ''')';
      SELECT NVL(LENGTH(REGEXP_REPLACE(REPLACE(vi_account, '%', '@'),
                                       '[^@]+',
                                       '')),
                 0)
        into v_count
        FROM DUAL;
      IF v_count > 1 THEN
        DBMS_OUTPUT.put_line('输入科目参数错误!只能有一个带百分号参数');
        return;
      END IF;
    ELSE
      v_account := ' in (''' || replace(vi_account, ',', ''',''') || ''')';
    END IF;
  
    v_cSql := 'SELECT
                                a.pk_voucher   , --凭证主键
                                a.nov    ,--凭证编码
                                a.pk_org  ,  --所属业务单元
                                a.pk_group , --所属集团
                                a.pk_accountingbook  , --所属核算账簿
                                a.accountcode  , --所属会计科目
                                a.yearv  , --所属期间-年
                                a.periodv  , --所属期间-月
                                a.adjustperiod  , --调整期间-月
                                a.prepareddatev , --制单日期
                                a.oppositesubj,
                                case when e.balanorient = ''1'' then ''贷方''
                                     when e.balanorient = ''0'' then ''借方''
                                     else ''''
                                end , --科目方向 1:贷方 0:借方
                                --辅助核算字段名
                                ' || v_sqltitle || '
                                a.pk_currtype  , --币种
                                a.excrate2   ,----汇率值
                                a.explanation  , --摘要
                                a.localdebitamount   ,--本币借方发生额
                                a.debitquantity   ,--借方发生数量
                                a.localcreditamount ,--本币贷方发生额
                                a.creditquantity  --贷方发生数量
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
              '  --账簿
                                       and e.code ' ||
              v_account ||
              ' --科目
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
      PIPE ROW(L_RESULT); --依次返回行
    END LOOP;
    CLOSE rowrecord;
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.put_line(v_cSql);
      dbms_output.put_line(dbms_utility.format_error_backtrace);
      ROLLBACK;
      RAISE;
  END;

  /*5.  查询对应科目余额
  参数：
  vi_date                   ： 查询日期
  vi_accountingbook  ：会计核算账簿编码
  vi_account                ：会计科目编码
                                    可传入1001% 查询对应及下级科目科目余额。不可以传入 1001%,1002%。
                                    可传入10010，10011，200101逗号分隔明细科目发生额
  vi_freevaluetype        ：辅助核算类型编码可在(bd_accassitem)查看
  vi_valueid             ：对应档案值编码
                                  可根据(bd_accassitem)中classid字段到md_class中查找对应档案表
   查询范例：
   select * from  table(pkg_finance_query.QUERYDETAIL_CHECK_SUM('@gdxc', --DBlink
           '2017-11-01', --开始日期
          '2017-11-01',
          '224102%',  --科目编码
          '981-0001', --账簿 选填，空默认全选
          '601102',--对方科目 选填，空默认全选
          '0001' , -- 辅助核算档案编码 空默认不选
          '10001'  -- 辅助核算档案值编码 空默认全选
          )
          )
  */
  FUNCTION QUERYDETAIL_CHECK_SUM(
                                 vi_sdate          in varchar2, --起始日期
                                 vi_edate          in varchar2, --结束日期
                                 vi_account        in varchar2, --会计科目
                                 vi_accountingbook in varchar2 default null, --会计核算账簿
                                 vi_oppaccount     in varchar2 default null, --对方科目
                                 --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
                                 vi_freevaluetype1 in varchar2 default null,
                                 --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
                                 vi_valueid1 in varchar2 default null,
                                 --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
                                 vi_freevaluetype2 in varchar2 default null,
                                 --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
                                 vi_valueid2 in varchar2 default null,
                                 --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
                                 vi_freevaluetype3 in varchar2 default null,
                                 --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
                                 vi_valueid3 in varchar2 default null,
                                 --辅助核算类型 传入对应档案表主键可在(bd_accassitem)查看
                                 vi_freevaluetype4 in varchar2 default null,
                                 --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
                                 vi_valueid4       in varchar2 default null,
                                 vi_freevaluetype5 in varchar2 default null,
                                 --辅助核算内容  传入对应档案值主键根据(bd_accassitem)中classid字段到md_class中查找对应档案表
                                 vi_valueid5 in varchar2 default null)
    RETURN QueryDETAIL_SUM
    PIPELINED IS
    L_RESULT DETAIL_ROW_SUM;
    TYPE ref_cursor_type IS REF CURSOR; --定义一个动态游标
    rowrecord              ref_cursor_type;
    v_cSql                 clob;
    begin_year             varchar2(20) := substr(vi_sdate, 1, 4); --期初年份
    v_sqltitle             varchar2(2000);
    v_sqlgroup             varchar2(2000);
    v_sqljoin              clob;
    v_soppaccount          varchar2(3000); --对方科目筛选条件字符串拼接
    v_account              varchar2(3000); --科目筛选条件字符串拼接
    v_accountcodefile      varchar2(200); --科目字段名特殊处理
    v_accountcodefilegroup varchar2(200);
    v_count                int := 0;
    v_accountbook_str      varchar2(3000); --核算账簿条件拼接
    v_pk_accchart  varchar2(20); --科目体系主键
  BEGIN
    --获取科目体系主键
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
      --%LIKE 形式
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
        DBMS_OUTPUT.put_line('输入科目参数错误!只能有一个带百分号参数');
        return;
      END IF;
    ELSE
      v_account              := ' in (''' ||
                                replace(vi_account, ',', ''',''') || ''')';
      v_accountcodefile      := 'a.accountcode';
      v_accountcodefilegroup := ',' || v_accountcodefile;
    END IF;
  
    v_cSql := 'SELECT
                                c.code org_code ,  --所属业务单元
                                d.code group_code , --所属集团
                                b.code accountbookcode, --所属核算账簿
                                ' || v_accountcodefile ||
              ' , --所属会计科目
                                --辅助核算字段名
                                ' || v_sqltitle || '
                                COUNT(1) detal_count,
                                SUM(case when substr(a.prepareddatev,1,10) >=  ''' ||
              vi_sdate || '''
                                                   and substr(a.prepareddatev,1,10) <=  ''' ||
              vi_edate || '''
                                         then a.localdebitamount
                                         else 0 end
                                         )  localdebitamount,--本期累计本币借方发生额
                                SUM(case when substr(a.prepareddatev,1,10) >= ''' ||
              vi_sdate || '''
                                                   and substr(a.prepareddatev,1,10) <= ''' ||
              vi_edate || '''
                                         then a.debitquantity
                                         else 0 end
                                         ) debitquantity ,--累计借方发生数量
                                SUM(case when substr(a.prepareddatev,1,10) >=  ''' ||
              vi_sdate || '''
                                                   and substr(a.prepareddatev,1,10) <=  ''' ||
              vi_edate || '''
                                         then a.localcreditamount
                                         else 0 end
                                         )  localcreditamount ,--本期累计本币贷方发生额
                                SUM(case when substr(a.prepareddatev,1,10) >=  ''' ||
              vi_sdate || '''
                                                   and substr(a.prepareddatev,1,10) <=  ''' ||
              vi_edate || '''
                                         then a.creditquantity
                                         else 0 end
                                         )  creditquantity ,--累计贷方发生数量
                                SUM(case when a.adjustperiod >=''00''
                                         then a.localdebitamount
                                         else 0 end
                                         )  sumdebitamount,--累计本币借方发生额
                                SUM(case when  a.adjustperiod >=''00''
                                         then a.localcreditamount
                                         else 0 end
                                         )  sumcreditamount ,--累计本币贷方发生额
                                SUM( case when substr(a.prepareddatev,1,10) < ''' ||
              vi_sdate ||
              ''' and e.balanorient= ''0'' then --借方    1:贷方 0:借方
                                                                            a.localdebitamount - a.localcreditamount
                                          when substr(a.prepareddatev,1,10) < ''' ||
              vi_sdate ||
              ''' and e.balanorient = ''1'' then --贷方
                                                                            a.localcreditamount - a.localdebitamount
                                          else 0 end
                                ) beginning_balances, --期初余额
                                SUM( case when substr(a.prepareddatev,1,10) <= ''' ||
              vi_edate ||
              ''' and e.balanorient = ''0'' then --借方
                                                                            a.localdebitamount - a.localcreditamount
                                                  when substr(a.prepareddatev,1,10) <= ''' ||
              vi_edate ||
              ''' and e.balanorient = ''1'' then --贷方
                                                                             a.localcreditamount - a.localdebitamount
                                          else 0 end
                                ) Final_balance,  --期末余额
                                SUM(case when substr(a.prepareddatev,1,10) <= ''' ||
              vi_edate || ''' and substr(a.prepareddatev,1,10) >= ''' ||
              vi_sdate || '''
                                                           and e.balanorient = ''0'' then --借方
                                                           a.localdebitamount - a.localcreditamount
                                                  when substr(a.prepareddatev,1,10) <= ''' ||
              vi_edate || ''' and substr(a.prepareddatev,1,10) >= ''' ||
              vi_sdate || '''
                                                           and e.balanorient = ''1'' then --贷方
                                                           a.localcreditamount - a.localdebitamount
                                          else 0 end
                                ) Final_amount --发生额
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
              ' --账簿
                                       and e.code ' ||
              v_account ||
              ' --科目
                                       and a.adjustperiod >= ''00''
                                       and a.yearv >= ''' ||
              begin_year || '''
                              group by   c.code  ,  --所属业务单元
                                                d.code  , --所属集团
                                                b.code    --所属核算账簿
                                                ' ||
              v_accountcodefilegroup ||
              ' --所属会计科目
                                                --辅助核算字段名
                                                ' ||
              v_sqlgroup || '
                           ';
    --DBMS_OUTPUT.put_line(v_cSql);
    OPEN rowrecord FOR v_cSql;
    LOOP
      FETCH rowrecord
        into L_RESULT;
      EXIT WHEN rowrecord%NOTFOUND;
      PIPE ROW(L_RESULT); --依次返回行
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
