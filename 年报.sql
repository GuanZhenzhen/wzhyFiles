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
    substr(a.TEL,0,30) as 联系电话,
    substr(a.ADDR,0,100) as 通信地址,
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
    SUBSTR(trim(m.DOMDISTRICT),0,6) as 行政区划,
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
 from ((((((AN_BASEINFO_SEND a
 Left join CDETRS b on a.ENTTYPE=b.代码)
  LEFT JOIN hz_dzhy_e_contact f on a.pripid =f.pripid)
  LEFT join 年报重码表 g on a.UNISCID=g.UNISCID )
  LEFT join hz_an_baseinfo h on a.ancheid=h.ancheid)
  LEFT join hz_e_sub s on a.pripid =s.brpripid and b.单位类型='2')
  left join hz_e_baseinfo m on a.pripid=m.pripid);
 /
   create or replace view 企业认缴出资信息 as
      select
          ancheid,
          '02' as rtype,
          cast(null as varchar2(32)) as zuuid,
          rawtohex(sys_guid())  AS uuid,
          substr(invname,0,100) as 股东发起人名称,
          lisubconam as 累计认缴额,
          to_char(to_date(subcondate ,'yyyy-MM-dd HH24:mi:SS'),'yyyyMMdd') as 认缴出资日期,
          subconform as 出资方式,
          cast(null as varchar2(3)) as 币种
    from an_subcapital;
    /
    create or replace view 企业实缴出资信息 as
      select
          ancheid,
          '03' as rtype,
          cast(null as varchar2(32)) as zuuid,
          rawtohex(sys_guid())  AS uuid,
          substr(invname,0,100) as 股东发起人名称,
          liacconam as 累计实缴额,
          to_char(to_date(accondate ,'yyyy-MM-dd HH24:mi:SS'),'yyyyMMdd') as 实缴出资日期,
          acconform as 出资方式,
          cast(null as varchar2(3)) as 币种
    from  an_subcapital;
/
    create or replace view 企业对外投资信息 as
       select
          ancheid,
          '04' as rtype,
          cast(null as varchar2(32)) as zuuid,
          rawtohex(sys_guid())  AS uuid,
          '1' as 是否有其他公司股权,
          entname as 所投资企业名称,
          uniscid as 所投资企业注册号
    from an_forinvestment;
/
    create or replace view 网站或网店信息 as
      select
          ancheid,
          '05' as rtype,
          cast(null as varchar2(32)) as zuuid,
          rawtohex(sys_guid())  AS uuid,
          '1' as 是否有网站或网店,
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
          '1' as 股权是否转让,
          INV as 股东名称,
          to_char(TRANSAMPR,'990.00')  as 转让前股权比例,
          to_char(TRANSAMAFT,'990.00') as 转让后股权比例,
          to_char(to_date(ALTDATE,'yyyy-MM-dd HH24:mi:SS'),'yyyyMMdd') as 股权变更日期
    from an_alterstockinfo ;


    /
    create view annualreport_infoview as
SELECT
/*+ PARALLEL (16) */
trim(a.ancheid) as ancheid,
'01' as rtype,
cast(null as varchar2(32)) as uuid,
nvl(a.REGNO,'--') AS 注册号,
nvl(a.UNISCID,'--') AS 社会信用代码,
a.ENTNAME AS 名称,
to_char(to_date(a.ANCHEDATE,'yyyy-MM-dd HH24:mi:SS'),'yyyyMMdd') as 年报时间,
trim(a.ANCHEYEAR) as 年报年度,
a.TEL as 联系电话,
substr(trim(a.ADDR),0,100) as 通信地址,
a.POSTALCODE as 邮政编码,
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
a.MAINBUSIACT as 主营业务活动,
a.WOMEMPNUM as 女性从业人员,
(case when a.HOLDINGSMSG_CN='7' then '3' when a.HOLDINGSMSG_CN='6' then '9' else a.HOLDINGSMSG_CN end ) as 控股情况,
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