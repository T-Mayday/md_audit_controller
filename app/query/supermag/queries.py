
STORES_QUERY = """
-- Список магазинов
SELECT
    r.title as region     -- Регион
    , T1.ID as SMSTORE        -- Ид магазина Супермаг
    , T1.name                 -- Название магазина
    , T1.address              -- Адрес магазина
    --, T2.PROPVAL as CLOSEDATE -- дата закрытия
    , ci.inn                  -- ИНН
    , t1.kpp                  -- КПП
    , (select propval from smstoreproperties where propid = 'EGAIS.FSRARID' and storeloc = t1.id) FSRARID
    , (select propval from smstoreproperties where propid = 'REP.UKMStoreId' and storeloc = t1.id) as UKM4STORE
    , (select propval from smstoreproperties where propid = 'REP.UKMSERVER' and storeloc = t1.id) as UKM4IP
    , (select propval from smstoreproperties where propid = 'REP.UKMSERVER5' and storeloc = t1.id) as UKM5IP
    , (select propval from smstoreproperties where propid = 'REP.STORE.Latitude' and storeloc = t1.id) as Latitude
    , (select propval from smstoreproperties where propid = 'REP.STORE.Longitude' and storeloc = t1.id) as Longitude
    , (select propval from smstoreproperties where propid = 'REP.DBNAME' and storeloc = t1.id) as DBNAME
    , nvl(f.title, '') as format
    , case
        when sc.tree like '1.1.1.%' then 'Николаевские'
        when sc.tree like '1.2.1.%' then 'ГХ'
        when sc.tree like '2.1.1.%' then 'Спутники'
        else ''
      end as subformat
    , sc.name as subformat1
    , (
        select upper(A.name)
        from Supermag.SAStoresAssort A, Supermag.SMStoresAssort M
        where A.Tree like '2.2.%'
          and A.ID = M.IDAssort
          and M.IDLoc = t1.ID
      ) as subformat2
FROM SMSTORELOCATIONS T1,
     SMREGIONS r,
     smownclientlocs ol,
     smclientinfo ci,
     sastoreformats f,
     sastoreclass sc,
     (SELECT STORELOC, PROPVAL FROM SMSTOREPROPERTIES WHERE PROPID = 'REP.CLOSEDATE') T2
WHERE
        T1.ID = T2.STORELOC(+)
    AND r.rgnid = T1.rgnid
    AND ol.locid = t1.id
    AND ci.id = ol.clientid
    AND f.id = t1.formatid
    AND sc.id = t1.idclass
    AND upper(T1.name) not like 'Я %'
    AND T1.name not like '%ТЕСТ%'
    AND t1.idclass in (
        select id
        from supermag.sastoreclass
        where tree like '1.1.%'
           or tree like '1.2.%'
           or tree like '1.3.%'
           or tree like '1.4.%'
           or tree like '2.1.%'
           or tree like '2.2.%'
    )
    AND T1.accepted = 1
    AND T1.loctype = 4
    AND (t2.PROPVAL is null or to_date(t2.PROPVAL, 'DD.MM.YYYY') >= to_date(sysdate))
ORDER BY r.title, T1.NAME
""";