  create or replace view hz_baseinfo_view as
  select * from hz_e_baseinfo  
  where REPORTTYPE <>'个体工商户' and REPORTTYPE <>'集团'  and pripid  not in ( select 内部序号 from (select 内部序号,详细名称 from baseinfo_view where REGSTATE in 
            ('撤销中','吊销未注销','吊销已注销','已吊销企业备案中','已注销','注销','注销中')) a 
            where exists
    (select 1 from baseinfo_view b where b.REGSTATE='开业' and a.详细名称=b.详细名称)
            )
/            
create view 重码表 as
  select pripid,uniscid,REGSTATE,'1' as 重码 from hz_baseinfo_view where uniscid in (
  select uniscid from hz_baseinfo_view 
    group by uniscid having count(*)>1) 
select * from 重码表 a where UNISCID not in  (select UNISCID from 重码表 b where b.REGSTATE like '%销%' )
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
          '1' )  as 工商业务类型,
    substr(trim(a.UNISCID),9,9) as 组织机构代码,
    nvl(a.REGNO,'--') AS 注册号,
    a.ENTNAME AS 名称,
    e.代码 AS 企业类型,
    e.单位类型 AS 单位类型,
    e.登记注册类型 AS 登记注册类型,
    e.控股情况 AS 控股情况,
    e.机构类型 AS 机构类型,
    e.执行会计标准类别 AS 执行会计标准类别,
    SUBSTR(trim(a.DOMDISTRICT),0,6) as 企业登记机关,
    trim(a.DOMDISTRICT) AS 数据处理地代码,
    to_char(a.ESTDATE,'yyyyMMdd') as 开业日期,
    to_char(a.OPFROM,'yyyyMMdd') as 经营期限自,
    to_char(a.OPTO,'yyyyMMdd') as 经营期限止,
    cast(((to_char(a.OPTO,'yyyy')-to_char(a.OPFROM,'yyyy'))) as varchar2(6))as 经营期限,
    a.DOM as 住所,
    decode(nvl2(translate(a.POSTALCODE,'\1234567890 ', '\'),'err','ok'),
    'err','',
    a.POSTALCODE) as 邮政编码,
    decode(f.MOBTEL||';'||f.TEL,
        ';',null,
        f.MOBTEL||';',f.MOBTEL,
        ';'||f.TEL,f.TEL,
        f.MOBTEL)
         as 联系电话,
    a.PROLOC as 生产经营地址,
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
    trim(a.DOMDISTRICT)  AS 行政区划代码,
    cast(null as varchar2(20)) as 数据包编码,
    (CASE
            WHEN a.REGSTATE = '吊销未注销'  THEN '1'
            WHEN a.REGSTATE = '吊销已注销'  THEN '1'
            WHEN a.REGSTATE = '已吊销企业备案中'  THEN '1'
            WHEN a.REGSTATE = '已注销'  THEN '1'
            WHEN a.REGSTATE = '注销中'  THEN '1'
            WHEN a.REGSTATE = '注销'  THEN '1'
            ELSE '0'
    END) AS 是否注销,
    cast(null as varchar2(32)) as 批次号,
    cast(null as varchar2(100)) as 上级主管部门名称,
    a.NAME as 法定代表人,
    c.NAME AS 财务负责人,
    cast((select count(*) from hz_e_inv b where b.pripid =a.pripid) as varchar2(6)) as 投资人数量,
    cast(null as varchar2(10)) as 下级子公司数量,
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
 FROM ((((hz_baseinfo_view  a 
  LEFT JOIN (select min(代码) 代码,min(单位类型) 单位类型,min(登记注册类型) 登记注册类型,
        min(控股情况) 控股情况,min(机构类型) 机构类型,min(执行会计标准类别) 执行会计标准类别,名称 
        from CDETRS group by 名称) e on a.REPORTTYPE =e.名称) 
        LEFT JOIN hz_dzhy_e_contact f on a.pripid =f.pripid )
        LEFT JOIN HZ_DZHY_E_FIN_LEADER c ON a.pripid = c.pripid)
        left JOIN (select * from 重码表 s where s.UNISCID not in (select g.UNISCID from 重码表 g where g.REGSTATE like '%销%' )) d
         on a.pripid=d.pripid);
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
          to_char(SUBCONPROP) as 认缴出资比例,
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
create or replace view illegal_info as 
 SELECT
    illid,
    '01' as rtype,
    cast(null as varchar2(32))  as uuid,
    nvl(UNISCID,'--') AS 社会信用代码,
    ENTNAME AS 名称,
    nvl(REGNO,'--') AS 注册号,
    to_char(ABNTIME,'yyyyMMdd') as 列入日期,
    SERILLREA_CN as 列入原因,
    DECORG_CN as 决定机关,
    cast(null as varchar2(32)) as 批次号,
    cast(null as varchar2(6)) AS 行政区划代码,
    cast(null as varchar2(6)) AS 季度,
    cast(null as varchar2(14)) as 数据上传时间,
    cast(null as varchar2(14)) as 修改时间,
    '999' as 状态,
    '0' as 是否重码,
    '0' as 是否已审核,
    '1' as 是否已推送到名录库,
    cast(null as varchar2(14)) as 排序,
    cast(null as varchar2(14)) as 审核时间
 from hz_e_li_illdisdetail;
/
create or replace view abnormal_info as
  SELECT
    a.busexclist,
    '01' as rtype,
    cast(null as varchar2(32)) as uuid,
    nvl(a.UNISCID,'--') AS 社会信用代码,
    a.ENTNAME AS 名称,
    nvl(a.REGNO,'--') AS 注册号,
    to_char(a.ABNTIME,'yyyyMMdd') as 列入日期,
    (case
       when instr(a.SPECAUSE_CN, '《企业信息公示暂行条例》第八条规定')>0 then '1'
       when instr(a.SPECAUSE_CN, '《企业信息公示暂行条例》第十条规定')>0 then '2'
       when instr(a.SPECAUSE_CN, '公示企业信息隐瞒真实情况、弄虚作假的')>0 then '3'
       when instr(a.SPECAUSE_CN, '通过登记的住所')>0 then '4'
       else null
       end) as 列入原因,
       decode(SUBSTR(trim(b.DOMDISTRICT),0,6),
            '330621','330603' ,
            '330183','330111' ,
            '330682','330604',
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
            SUBSTR(trim(b.DOMDISTRICT),0,6)) as 决定机关,
       decode(SUBSTR(trim(b.DOMDISTRICT),0,6),
            '330621','330603' ,
            '330183','330111' ,
            '330682','330604',
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
            SUBSTR(trim(b.DOMDISTRICT),0,6)) AS 行政区划代码,
    cast(null as varchar2(32)) as 批次号,
    cast(null as varchar2(6)) AS 季度,
    cast(null as varchar2(14)) as 数据上传时间,
    cast(null as varchar2(14)) as 修改时间,
    '999' as 状态,
    '0' as 是否重码,
    '0' as 是否已审核,
    '1' as 是否已推送到名录库,
    cast(null as varchar2(14)) as 排序,
    cast(null as varchar2(14)) as 审核时间 
  from hz_ao_opa_detail a left join hz_e_baseinfo b on a.pripid=b.pripid;
/
create view 年报重码表 as
 select UNISCID ,'1' as 重码 from AN_BASEINFO_SEND group by UNISCID having count(*)>1

/
create or replace view annualreport_info as
 SELECT
    a.ancheid,

    '01' as rtype,
    cast(null as varchar2(32)) as uuid,
    nvl(a.REGNO,'--') AS 注册号,
    nvl(a.UNISCID,'--') AS 社会信用代码,
    a.ENTNAME AS 名称,
    to_char(to_date(a.ANCHEDATE,'yyyy-MM-dd HH24:mi:SS'),'yyyyMMdd') as 年报时间,
    trim(a.ANCHEYEAR) as 年报年度,
    a.TEL as 联系电话,
    trim(a.ADDR) as 通信地址,
    a.POSTALCODE as 邮政编码,
    a.EMAIL as 电子邮箱,
    a.BUSST as 经营状态,
    a.EMPNUM as 从业人数,
    a.ENTTYPE as 企业类型,
    b.单位类型 as 单位类型,
    substr(trim(a.UNISCID),9,9) as 组织机构代码,
    to_char(h.ASSGRO,'99999999999999990.999999') as 资产总额,
    to_char(h.LIAGRO,'99999999999999990.999999')   as 负债总额,
    to_char(h.VENDINC,'99999999999999990.999999')  as 销售收入,
    to_char(h.MAIBUSINC,'99999999999999990.999999') as 主营业务收入,
    to_char(h.PROGRO,'99999999999999990.999999') as 利润总额,
    to_char(h.NETINC,'99999999999999990.999999') as 净利润,
    to_char(h.RATGRO,'99999999999999990.999999') as 纳税总额,
    to_char(h.TOTEQU,'99999999999999990.999999') as 所有者权益合计,
    a.MAINBUSIACT as 主营业务活动,
    a.WOMEMPNUM as 女性从业人员,
    a.HOLDINGSMSG_CN as 控股情况,
    cast(s.uniscid as varchar2(18)) as 上级社会信用代码,
    cast(substr(trim(s.UNISCID),9,9) as varchar2(9)) as 上级组织机构,
    f.NAME as 姓名,
    f.MOBTEL as 移动电话,
    decode((select count(*) from an_forinvestment e where a. ancheid= e.ancheid),
     0,'0',
    '1') as 是否有对外投资信息,
    decode((select count(*) from an_websiteinfo e where a. ancheid= e.ancheid),
     0,'0',
    '1') as 是否有网站网店信息,
    decode((select count(*) from an_alterstockinfo e where a. ancheid= e.ancheid),
     0,'0',
    '1') as 是否有股权变更信息,
    cast(null as varchar2(32)) as 批次号,
    cast(null as varchar2(6)) as 行政区划,
    cast(null as varchar2(14)) as 数据上传时间,
    to_char(to_date(a.LASTUPDATETIME,'yyyy-MM-dd HH24:mi:SS'),'yyyyMMddHHmiSS') as 修改时间,
    '999' as 状态,
    '0' as 是否已审核,
    '1' as 是否已推送到名录库,
    cast(null as varchar2(16)) as 排序,
    cast((select count(*) from an_subcapital n where a.ANCHEID=n.ANCHEID and to_number(n.lisubconam)>0) as varchar2(8)) as 认缴信息计数,
    cast((select count(*) from an_subcapital n where a.ANCHEID=n.ANCHEID and to_number(n.lisubconam)>0) as varchar2(8)) as 实缴信息计数,
    cast((select count(*) from an_forinvestment e where a. ancheid= e.ancheid) as varchar2(8)) as 投资信息计数,
    cast((select count(*) from an_websiteinfo e where a. ancheid= e.ancheid) as varchar2(8)) as 网站信息计数,
    cast((select count(*) from an_alterstockinfo e where a. ancheid= e.ancheid) as varchar2(8)) as 股权变更计数,
    cast(null as varchar2(14)) as 审核时间,
    decode(g.重码,
     '1','1',
    '0')  as 社会信用代码是否重复
 from (((((AN_BASEINFO_SEND a
 Left join CDETRS b on a.ENTTYPE=b.代码)
  LEFT JOIN hz_dzhy_e_contact f on a.pripid =f.pripid)
  LEFT join 年报重码表 g on a.UNISCID=g.UNISCID )
  LEFT join hz_an_baseinfo h on a.ancheid=h.ancheid)
  LEFT join hz_e_sub s on a.pripid =s.brpripid and b.单位类型='2');
 /
   create or replace view 企业认缴出资信息 as
      select
          ancheid,
          '02' as rtype,
          cast(null as varchar2(32)) as zuuid,
          rawtohex(sys_guid())  AS uuid
    from  hz_an_alterstockinfo where ancheid='无用';
    /
    create or replace view 企业实缴出资信息 as
      select
          ancheid,
          '03' as rtype,
          cast(null as varchar2(32)) as zuuid,
          rawtohex(sys_guid())  AS uuid
    from  hz_an_alterstockinfo where ancheid='无用';
/
    create or replace view 企业对外投资信息 as
       select
          ancheid,
          '04' as rtype,
          cast(null as varchar2(32)) as zuuid,
          rawtohex(sys_guid())  AS uuid,
          cast(null as varchar2(1))as 是否有其他公司股权,
          entname as 所投资企业名称,
          cast(null as varchar2(200))as 所投资企业注册号
    from an_forinvestment;
/
    create or replace view 网站或网店信息 as
      select
          ancheid,
          '05' as rtype,
          cast(null as varchar2(32)) as zuuid,
          rawtohex(sys_guid())  AS uuid,
          cast(null as varchar2(1))as 是否有网站或网店,
          webtype as 网站网店类型,
          websitname as 网站网店名称,
          website as 网站网店网址
    from  an_websiteinfo;
    /
    create or replace view 股权变更信息 as
      select
          ancheid,
          '06' as rtype,
          cast(null as varchar2(32)) as zuuid,
          rawtohex(sys_guid()) AS uuid,
          cast(null as varchar2(2000)) as 股权是否转让,
          INV as 股东名称,
          to_char(TRANSAMPR,'990.00')  as 转让前股权比例,
          to_char(TRANSAMAFT,'990.00') as 转让后股权比例,
          to_char(to_date(ALTDATE,'yyyy-MM-dd HH24:mi:SS'),'yyyyMMdd') as 股权变更日期
    from an_alterstockinfo ;
    /
    create table cn_forinvestment as select ANCHEID,count(*) as cn from an_forinvestment group by ANCHEID;
create table cn_websiteinfo as select ANCHEID,count(*) as cn from an_websiteinfo group by ANCHEID;
create table cn_alterstockinfo as select ANCHEID,count(*) as cn from an_alterstockinfo group by ANCHEID;

create table cn_asubcapital as 
select ANCHEID,
      sum((case when lisubconam>0 then 1 else 0 end)) as cn_rj,
      sum((case when liacconam>0 then 1 else 0 end)) as cn_sj from an_subcapital group by ANCHEID



insert /*+append*/  into annualreport_info SELECT 
a.ancheid,
'01' as rtype,
cast(null as varchar2(32)) as uuid,
nvl(a.REGNO,'--') AS 注册号,
nvl(a.UNISCID,'--') AS 社会信用代码,
a.ENTNAME AS 名称,
to_char(to_date(a.ANCHEDATE,'yyyy-MM-dd HH24:mi:SS'),'yyyyMMdd') as 年报时间,
trim(a.ANCHEYEAR) as 年报年度,

substr(a.TEL,0,30)  as 联系电话,

substr(a.ADDR,0,100) as 通信地址,

substr(a.POSTALCODE,0,6) as 邮政编码,
a.EMAIL as 电子邮箱,
a.BUSST as 经营状态,
a.EMPNUM as 从业人数,
a.ENTTYPE as 企业类型,
b.单位类型 as 单位类型,
substr(trim(a.UNISCID),9,9) as 组织机构代码,
to_char(h.ASSGRO,'99999999999999990.999999') as 资产总额,
to_char(h.LIAGRO,'99999999999999990.999999') as 负债总额,
to_char(h.VENDINC,'99999999999999990.999999') as 销售收入,
to_char(h.MAIBUSINC,'99999999999999990.999999') as 主营业务收入,
to_char(h.PROGRO,'99999999999999990.999999') as 利润总额,
to_char(h.NETINC,'99999999999999990.999999') as 净利润,
to_char(h.RATGRO,'99999999999999990.999999') as 纳税总额,
to_char(h.TOTEQU,'99999999999999990.999999') as 所有者权益合计,
substr(a.MAINBUSIACT,0,2000)as 主营业务活动,
a.WOMEMPNUM as 女性从业人员,
a.HOLDINGSMSG_CN as 控股情况,
cast(s.uniscid as varchar2(18)) as 上级社会信用代码,
cast(substr(trim(s.UNISCID),9,9) as varchar2(9)) as 上级组织机构,
substr(f.NAME,0,30) as 姓名,

substr(f.MOBTEL,0,30) as 移动电话,
NVL2(o.cn,'1','0') as 是否有对外投资信息,

NVL2(p.cn,'1','0') as 是否有网站网店信息,

NVL2(q.cn,'1','0') as 是否有股权变更信息,
cast(null as varchar2(32)) as 批次号,
SUBSTR(trim(m.DOMDISTRICT),0,6) as 行政区划,
cast(null as varchar2(14)) as 数据上传时间,
to_char(to_date(a.LASTUPDATETIME,'yyyy-MM-dd HH24:mi:SS'),'yyyyMMddHHmiSS') as 修改时间,
'999' as 状态,
'0' as 是否已审核,
'1' as 是否已推送到名录库,
cast(null as varchar2(16)) as 排序,
cast(n.cn_rj as varchar2(8)) as 认缴信息计数,
cast(n.cn_sj as varchar2(8)) as 实缴信息计数,
cast(o.cn as varchar2(8)) as 投资信息计数,
cast(p.cn as varchar2(8)) as 网站信息计数,
cast(q.cn as varchar2(8)) as 股权变更计数,
cast(null as varchar2(14)) as 审核时间,
decode(g.重码,
'1','1',
'0') as 社会信用代码是否重复
from ((((((((((AN_BASEINFO_SEND a
Left join CDETRS b on a.ENTTYPE=b.代码)
LEFT JOIN hz_dzhy_e_contact f on a.pripid =f.pripid)
LEFT join 年报重码表 g on a.UNISCID=g.UNISCID )
LEFT join hz_an_baseinfo h on a.ancheid=h.ancheid)
LEFT join hz_e_sub s on a.pripid =s.pripid and b.单位类型='2')
left join hz_e_baseinfo m on a.pripid=m.pripid)
left join cn_asubcapital n on a.ancheid=n.ancheid)
left join cn_forinvestment o on a.ancheid=o.ancheid)
left join cn_websiteinfo p on a.ancheid=p.ancheid)
left join cn_alterstockinfo q on a.ancheid=q.ancheid)


