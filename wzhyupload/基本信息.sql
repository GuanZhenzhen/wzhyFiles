--   create or replace view hz_baseinfo_view as
--   select * from hz_e_baseinfo  
--   where REPORTTYPE <>'个体工商户' and REPORTTYPE <>'集团'  and pripid  not in ( select 内部序号 from (select 内部序号,详细名称 from baseinfo_view where REGSTATE in 
--             ('吊销已注销','已注销','注销')) a 
--             where exists
--     (select 1 from baseinfo_view b where b.REGSTATE='开业' and a.详细名称=b.详细名称)
--             )
/            
 重码表：create table recode(
pripid varchar2(36),
社会信用代码 varchar2(18),
REGSTATE varchar2(50),
重码 varchar2(1),
PRIMARY KEY (pripid)
)
insert into recode select  pripid,UNISCID as 社会信用代码 ,REGSTATE,'1' as 重码 from mv_baseinfo where   UNISCID in (
  select  UNISCID from mv_baseinfo where REGSTATE not like '%销%'
    group by UNISCID having count(*)>1) 
/
create or replace view ent_info as 
 SELECT
    a.pripid,
    '01' as rtype,
    cast(null as varchar2(32))  as uuid,
    nvl(a.UNISCID,'--') as 社会信用代码,
    decode(trim(a.REGSTATE),
          '已注销','6',
          '注销','6',
          '吊销已注销','6',
          '吊销未注销','6',
          '1' )  
    as 工商业务类型,
    substr(trim(a.UNISCID),9,9) as 组织机构代码,
    nvl(a.REGNO ,'--') AS 注册号,
    a.ENTNAME AS 详细名称,
    e.代码 AS 企业类型,
    e.单位类型 AS 单位类型,
    e.登记注册类型 AS 登记注册类型,
    e.控股情况 AS 控股情况,
    e.机构类型 AS 机构类型,
    e.执行会计标准类别 AS 执行会计标准类别,
    decode(SUBSTR(trim(a.DOMDISTRICT),0,6),
            '330621','330603' ,
            '330183','330111',
            '330682','330604' ,
            '330198','330160' ,
            '332501','331102' ,
            '330196','330106' ,
            '332526','331122' ,
            '330508','330560' ,
            '332522','331121' ,
            '332502','331181' ,
            '332527','331123' ,
            '332501','331100' ,
            '332528','331124' ,
            '332529','331127' ,
            '332523','331125' ,
            '330194','330161' ,
            '330322','330305' ,
            '332525','331126' ,
            '330509','330500',
            '330682','330600',
            '330605','330600',
            '330508','330500',
            '330606','330600',
            '330198','330100',
            '332500','331100',
            '330306','330300',
            '330307','330300',
            '330621','330600',
            '332522','331100',
            '330196','330100',
            '330183','330100',
            '332526','331100',
            '332527','331100',
            '330322','330300',
            '330906','330900', 
            '330905','330900',
            '330194','330100',
            '332502','331100',
            '332523','331100',
            '330907','330900',
            '332528','331100',  
            '332529','331100',
            '332525','331100',
            '330214','330200',
            SUBSTR(trim(a.DOMDISTRICT),0,6)) 
    as  企业登记机关,
    (case 
        when a.DOMDISTRICT is null and a.YIEDISTRICT is null then '330000' 
        when a.DOMDISTRICT is null and a.YIEDISTRICT is not null then decode(SUBSTR(trim(a.YIEDISTRICT),0,6),
            '330621','330603' ,
            '330183','330111',
            '330682','330604' ,
            '330198','330160' ,
            '332501','331102' ,
            '330196','330106' ,
            '332526','331122' ,
            '330508','330560' ,
            '332522','331121' ,
            '332502','331181' ,
            '332527','331123' ,
            '332501','331100' ,
            '332528','331124' ,
            '332529','331127' ,
            '332523','331125' ,
            '330194','330161' ,
            '330322','330305' ,
            '332525','331126' ,
            '330509','330500',
            '330682','330600',
            '330605','330600',
            '330508','330500',
            '330606','330600',
            '330198','330100',
            '332500','331100',
            '330306','330300',
            '330307','330300',
            '330621','330600',
            '332522','331100',
            '330196','330100',
            '330183','330100',
            '332526','331100',
            '332527','331100',
            '330322','330300',
            '330906','330900', 
            '330905','330900',
            '330194','330100',
            '332502','331100',
            '332523','331100',
            '330907','330900',
            '332528','331100',  
            '332529','331100',
            '332525','331100',
            '330214','330200',
            SUBSTR(trim(a.YIEDISTRICT),0,6)) 
        else  decode(SUBSTR(trim(a.DOMDISTRICT),0,6),
            '330621','330603' ,
            '330183','330111',
            '330682','330604' ,
            '330198','330160' ,
            '332501','331102' ,
            '330196','330106' ,
            '332526','331122' ,
            '330508','330560' ,
            '332522','331121' ,
            '332502','331181' ,
            '332527','331123' ,
            '332501','331100' ,
            '332528','331124' ,
            '332529','331127' ,
            '332523','331125' ,
            '330194','330161' ,
            '330322','330305' ,
            '332525','331126' ,
            '330509','330500',
            '330682','330600',
            '330605','330600',
            '330508','330500',
            '330606','330600',
            '330198','330100',
            '332500','331100',
            '330306','330300',
            '330307','330300',
            '330621','330600',
            '332522','331100',
            '330196','330100',
            '330183','330100',
            '332526','331100',
            '332527','331100',
            '330322','330300',
            '330906','330900', 
            '330905','330900',
            '330194','330100',
            '332502','331100',
            '332523','331100',
            '330907','330900',
            '332528','331100',  
            '332529','331100',
            '332525','331100',
            '330214','330200',
            SUBSTR(trim(a.DOMDISTRICT),0,6)) 
        end)
    as  数据处理地代码,
    to_char(a.ESTDATE,'yyyyMMdd') as 开业日期,
    to_char(a.OPFROM,'yyyyMMdd') as 经营期限自,
    to_char(a.OPTO,'yyyyMMdd') as 经营期限止,
    cast(((to_char(a.OPTO,'yyyy')-to_char(a.OPFROM,'yyyy'))) as varchar2(6))as 经营期限,
    a.dom as 住所,
    decode(nvl2(translate(a.POSTALCODE,'\1234567890 ', '\'),'err','ok'),
    'err','',
    a.POSTALCODE) as 邮政编码,
    decode(f.MOBTEL||';'||f.TEL,
        ';',null,
        f.MOBTEL||';',f.MOBTEL,
        ';'||f.TEL,f.TEL,
        f.MOBTEL)
         as 联系电话,
    a.ProLoc as 生产经营地址,
    a.OPSCOPE as 经营范围,
    to_char(a.REGCAP,'99999999999999990.999999') as 注册资本,
    cast(decode(trim(a.REGCAPCUR),
        '澳大利亚元','36',
        '奥地利先令','40',
        '新加坡元','702',
        '人民币','156',
        '港币','344',
        '港元','344',
        '意大利里拉','380',
        '日元','392',
        '韩元','410',
        '荷兰盾','528',
        '新西兰元','554',
        '挪威克朗','578',
        '欧元','954',
        '瑞典克朗','752',
        '瑞士法郎','756',
        '英镑','826',
        '美元','840',
        '比利时法郎','56',
        '加拿大元','124',
        '丹麦克朗','208',
        '法国法郎','250',
        '德国马克','280',
    '   ') as varchar2(3)) as 货币种类,
    cast(null as varchar2(24)) as 货币金额,
    '0' as 信息操作类型,
    cast(null as varchar2(14)) as 数据修改时间,
    decode(SUBSTR(trim(a.DOMDISTRICT),0,6),
            '330621','330603' ,
            '330183','330111',
            '330682','330604' ,
            '330198','330160' ,
            '332501','331102' ,
            '330196','330106' ,
            '332526','331122' ,
            '330508','330560' ,
            '332522','331121' ,
            '332502','331181' ,
            '332527','331123' ,
            '332501','331100' ,
            '332528','331124' ,
            '332529','331127' ,
            '332523','331125' ,
            '330194','330161' ,
            '330322','330305' ,
            '332525','331126' ,
            '330509','330500',
            '330682','330600',
            '330605','330600',
            '330508','330500',
            '330606','330600',
            '330198','330100',
            '332500','331100',
            '330306','330300',
            '330307','330300',
            '330621','330600',
            '332522','331100',
            '330196','330100',
            '330183','330100',
            '332526','331100',
            '332527','331100',
            '330322','330300',
            '330906','330900', 
            '330905','330900',
            '330194','330100',
            '332502','331100',
            '332523','331100',
            '330907','330900',
            '332528','331100',  
            '332529','331100',
            '332525','331100',
            '330214','330200',
            SUBSTR(trim(a.DOMDISTRICT),0,6))  AS 行政区划代码,
    cast(null as varchar2(20)) as 数据包编码,
    (CASE
            WHEN a.REGSTATE = '吊销已注销'  THEN '1'
            WHEN a.REGSTATE = '吊销未注销'  THEN '1'
            WHEN a.REGSTATE = '已注销'  THEN '1'
            WHEN a.REGSTATE = '注销'  THEN '1'
            ELSE '0'
    END) AS 是否注销,
    cast(null as varchar2(32)) as 批次号,
    cast(null as varchar2(100)) as 上级主管部门名称,
    a.name as 法定代表人,
    c.NAME AS 财务负责人,
    cast((select count(*) from hz_e_inv b where b.pripid =a.pripid) as varchar2(6)) as 投资人数量,
    cast((select count(*) from hz_e_inv b where b.inv=a.entname ) as varchar2(10)) as 下级子公司数量,
    cast(null as varchar2(10)) as 变更时间,
    '999' as 状态,
    decode(d.重码,
     '1','1',
     '0') as 是否重码,
    '0' as 人工处理结果类型,
    '0' as 是否已审核,
    '1' as 是否已推送到名录库,
    cast(null as varchar2(14)) as 数据上传时间,
    cast(null as varchar2(14)) as 审核时间
 FROM mv_baseinfo  a 
  LEFT JOIN CDETRS_ONE e on a.REPORTTYPE =e.名称 
        LEFT JOIN hz_dzhy_e_contact f on a.pripid =f.pripid 
        LEFT JOIN HZ_DZHY_E_FIN_LEADER c ON a.pripid = c.pripid
        left JOIN recode d
         on a.pripid=d.pripid 
 WHERE NOT EXISTS
             (SELECT 1
                FROM mv_baseinfo_exzx  bb
               WHERE bb.pripid = a.pripid)
         
 ;
   / 
    create or replace view 法人信息表 as
      select
        pripid,
       '02' as rtype,
       cast(null as varchar2(32)) as zuuid,
        rawtohex(sys_guid())  AS uuid,
        NAME as 法人姓名,
        CERTYPE as 证件类型,
        CERNO as 证件编号,
        TELNUMBER as 固话,
        MOBTEL as 移动电话,
        EMAIL as 电子邮件
    from hz_e_pri_person;
/
    create or replace view 财务负责人 as
       select
          pripid,
          '03' as rtype,
          cast(null as varchar2(32)) as zuuid,
          rawtohex(sys_guid())  AS uuid,
          NAME as 负责人姓名,
          CERTYPE as 证件类型,
          CERNO as 证件编号,
          TEL as 固话,
          MOBTEL as 移动电话,
          EMAIL as 电子邮件
    from hz_dzhy_e_fin_leader ;  
/
    create or replace view 上级主管部门信息表 as
       select
          pripid,
         '04'  as rtype,
          cast(null as varchar2(32)) as zuuid,
          rawtohex(sys_guid())  AS uuid
    from hz_dzhy_e_fin_leader where pripid='无用';
  /  
    create or replace view  投资人信息 as
       select 
           pripid,
          '05' as rtype,
          cast(null as varchar2(32)) as zuuid,
          rawtohex(sys_guid())  AS uuid,
          INVTYPE as 投资类型,
          INV as 投资方名称,
          BLICTYPE as 证件类型,
          BLICNO as 证件编号,
          cast(null as varchar2(10)) as 证照类型,
          cast(null as varchar2(60)) as 证照号码,
          to_char(SUBCONAM,'999999990.9999') as 认缴出资额,
          cast(null as varchar2(20)) as 认缴出资时间,
          cast(null as varchar2(100)) as 认缴出资方式,
          to_char(SUBCONPROP,'9999999990') as 认缴出资比例,
          COUNTRY as 国籍,
          cast(null as varchar2(10)) as 币种
    from hz_e_inv;
/
    create or replace view 下级子公司信息表 as
      select
          pripid,
         '04'  as rtype,
          cast(null as varchar2(32)) as zuuid,
          rawtohex(sys_guid())  AS uuid
    from hz_dzhy_e_fin_leader where pripid='无用';
/
    create or replace view  变更信息表 as
       select 
           pripid,
          '07' as rtype,
          cast(null as varchar2(32)) as zuuid,
          rawtohex(sys_guid())  AS uuid,
          ALTITEM as 变更事项,
          ALTBE as 变更前内容,
          ALTAF as 变更后内容,
          to_char(ALTDATE,'yyyyMMdd') as 变更日期,
          cast(null as varchar2(20)) as 数据包编号
    from hz_dzhy_e_alter_recoder;
/
    create or replace view  注销信息 as
       select 
           pripid,
          '08' as rtype,
          cast(null as varchar2(32)) as zuuid,
          rawtohex(sys_guid())  AS uuid,
          to_char(CANDATE,'yyyyMMdd') as 注销日期,
          cast(null as varchar2(200)) as 注销原因
    from hz_e_cancel;
/
    create or replace view  吊销信息 as
       select 
           pripid,
          '08' as rtype,
          cast(null as varchar2(32)) as zuuid,
          rawtohex(sys_guid())  AS uuid,
          to_char(REVDATE,'yyyyMMdd') as 注销日期,
          REVBASIS as 注销原因
    from hz_e_revoke; 
/
		