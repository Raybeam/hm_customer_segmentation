COPY INTO 's3://hermanmiller/data/segmentation/rmf_2021_03_21/rfm_query.csv.gz'
FROM
(SELECT
    CAST(entity_id AS STRING) entity_id,
    CAST(nt_emails AS STRING) nt_emails,
    CAST(c_emails AS STRING) c_emails,
    CAST(shipaddress AS STRING) shipaddress,
    CAST(first_order AS STRING) first_order,
    CAST(CAST(first_order as DATE) AS STRING) first_order_date,
    CAST(DATE_PART('YEAR', first_order) AS STRING) first_order_year,
    CAST(last_order AS STRING) last_order,
    CAST(DATEDIFF('day', last_order, CURRENT_DATE()) AS STRING) recency,
    CAST(DIV0(num_orders,  DATEDIFF('day', first_order, last_order)) AS STRING) frequency,
    CAST(num_orders AS STRING) num_orders,
    CAST(total_revenue AS STRING) total_revenue,
    CAST(num_hm_orders_first_180_days AS STRING) num_hm_orders_first_180_days,
    CAST(num_dwr_orders_first_180_days AS STRING) num_dwr_orders_first_180_days,
    CAST(num_hay_orders_first_180_days AS STRING) num_hay_orders_first_180_days,
    CAST(hm_rev_first_180_days AS STRING) hm_rev_first_180_days,
    CAST(dwr_rev_first_180_days AS STRING) dwr_rev_first_180_days,
    CAST(hay_rev_first_180_days AS STRING) hay_rev_first_180_days,
    CAST(hm_items_bought_first_180_days AS STRING) hm_items_bought_first_180_days,
    CAST(dwr_items_bought_first_180_days AS STRING) dwr_items_bought_first_180_days,
    CAST(hay_items_bought_first_180_days AS STRING) hay_items_bought_first_180_days,
    CAST(Contract_orders_first_180_days AS STRING) Contract_orders_first_180_days,
    CAST(Corporate_orders_first_180_days AS STRING) Corporate_orders_first_180_days,
    CAST(Studio_orders_first_180_days AS STRING) Studio_orders_first_180_days,
    CAST(Wholesale_orders_first_180_days AS STRING) Wholesale_orders_first_180_days,
    CAST(HM_SF_Chestnut_orders_first_180_days AS STRING) HM_SF_Chestnut_orders_first_180_days,
    CAST(Call_Center_orders_first_180_days AS STRING) Call_Center_orders_first_180_days,
    CAST(Fabric_orders_first_180_days AS STRING) Fabric_orders_first_180_days,
    CAST(International_orders_first_180_days AS STRING) International_orders_first_180_days,
    CAST(Outlet_orders_first_180_days AS STRING) Outlet_orders_first_180_days,
    CAST(Warehouses_orders_first_180_days AS STRING) Warehouses_orders_first_180_days,
    CAST(Offsite_Inventory_orders_first_180_days AS STRING) Offsite_Inventory_orders_first_180_days,
    CAST(HM_Retail_orders_first_180_days AS STRING) HM_Retail_orders_first_180_days,
    CAST(Web_orders_first_180_days AS STRING) Web_orders_first_180_days,
    CAST(Contract_rev_first_180_days AS STRING) Contract_rev_first_180_days,
    CAST(Corporate_rev_first_180_days AS STRING) Corporate_rev_first_180_days,
    CAST(Studio_rev_first_180_days AS STRING) Studio_rev_first_180_days,
    CAST(Wholesale_rev_first_180_days AS STRING) Wholesale_rev_first_180_days,
    CAST(HM_SF_Chestnut_rev_first_180_days AS STRING) HM_SF_Chestnut_rev_first_180_days,
    CAST(Call_Center_rev_first_180_days AS STRING) Call_Center_rev_first_180_days,
    CAST(Fabric_rev_first_180_days AS STRING) Fabric_rev_first_180_days,
    CAST(International_rev_first_180_days AS STRING) International_rev_first_180_days,
    CAST(Outlet_rev_first_180_days AS STRING) Outlet_rev_first_180_days,
    CAST(Warehouses_rev_first_180_days AS STRING) Warehouses_rev_first_180_days,
    CAST(Offsite_Inventory_rev_first_180_days AS STRING) Offsite_Inventory_rev_first_180_days,
    CAST(HM_Retail_rev_first_180_days AS STRING) HM_Retail_rev_first_180_days,
    CAST(Web_rev_first_180_days AS STRING) Web_rev_first_180_days,
    CAST(num_hm_orders_after_first_180_days AS STRING) num_hm_orders_after_first_180_days,
    CAST(num_dwr_orders_after_first_180_days AS STRING) num_dwr_orders_after_first_180_days,
    CAST(num_hay_orders_after_first_180_days AS STRING) num_hay_orders_after_first_180_days,
    CAST(hm_rev_after_first_180_days AS STRING) hm_rev_after_first_180_days,
    CAST(dwr_rev_after_first_180_days AS STRING) dwr_rev_after_first_180_days,
    CAST(hay_rev_after_first_180_days AS STRING) hay_rev_after_first_180_days,
    CAST(hm_items_bought_after_first_180_days AS STRING) hm_items_bought_after_first_180_days,
    CAST(dwr_items_bought_after_first_180_days AS STRING) dwr_items_bought_after_first_180_days,
    CAST(hay_items_bought_after_first_180_days AS STRING) hay_items_bought_after_first_180_days,
    CAST(Contract_orders_after_first_180_days AS STRING) Contract_orders_after_first_180_days,
    CAST(Corporate_orders_after_first_180_days AS STRING) Corporate_orders_after_first_180_days,
    CAST(Studio_orders_after_first_180_days AS STRING) Studio_orders_after_first_180_days,
    CAST(Wholesale_orders_after_first_180_days AS STRING) Wholesale_orders_after_first_180_days,
    CAST(HM_SF_Chestnut_orders_after_first_180_days AS STRING) HM_SF_Chestnut_orders_after_first_180_days,
    CAST(Call_Center_orders_after_first_180_days AS STRING) Call_Center_orders_after_first_180_days,
    CAST(Fabric_orders_after_first_180_days AS STRING) Fabric_orders_after_first_180_days,
    CAST(International_orders_after_first_180_days AS STRING) International_orders_after_first_180_days,
    CAST(Outlet_orders_after_first_180_days AS STRING) Outlet_orders_after_first_180_days,
    CAST(Warehouses_orders_after_first_180_days AS STRING) Warehouses_orders_after_first_180_days,
    CAST(Offsite_Inventory_orders_after_first_180_days AS STRING) Offsite_Inventory_orders_after_first_180_days,
    CAST(HM_Retail_orders_after_first_180_days AS STRING) HM_Retail_orders_after_first_180_days,
    CAST(Web_orders_after_first_180_days AS STRING) Web_orders_after_first_180_days,
    CAST(Contract_rev_after_first_180_days AS STRING) Contract_rev_after_first_180_days,
    CAST(Corporate_rev_after_first_180_days AS STRING) Corporate_rev_after_first_180_days,
    CAST(Studio_rev_after_first_180_days AS STRING) Studio_rev_after_first_180_days,
    CAST(Wholesale_rev_after_first_180_days AS STRING) Wholesale_rev_after_first_180_days,
    CAST(HM_SF_Chestnut_rev_after_first_180_days AS STRING) HM_SF_Chestnut_rev_after_first_180_days,
    CAST(Call_Center_rev_after_first_180_days AS STRING) Call_Center_rev_after_first_180_days,
    CAST(Fabric_rev_after_first_180_days AS STRING) Fabric_rev_after_first_180_days,
    CAST(International_rev_after_first_180_days AS STRING) International_rev_after_first_180_days,
    CAST(Outlet_rev_after_first_180_days AS STRING) Outlet_rev_after_first_180_days,
    CAST(Warehouses_rev_after_first_180_days AS STRING) Warehouses_rev_after_first_180_days,
    CAST(Offsite_Inventory_rev_after_first_180_days AS STRING) Offsite_Inventory_rev_after_first_180_days,
    CAST(HM_Retail_rev_after_first_180_days AS STRING) HM_Retail_rev_after_first_180_days,
    CAST(Web_rev_after_first_180_days AS STRING) Web_rev_after_first_180_days,

    CAST(DIV0(hm_rev_first_180_days, num_hm_orders_first_180_days) AS STRING) hm_aov_first_180_days,
    CAST(DIV0(dwr_rev_first_180_days, num_dwr_orders_first_180_days) AS STRING) dwr_aov_first_180_days,
    CAST(DIV0(hay_rev_first_180_days, num_hay_orders_first_180_days) AS STRING) hay_aov_first_180_days,

    CAST(DIV0(hm_rev_after_first_180_days, num_hm_orders_after_first_180_days) AS STRING) hm_aov_after_first_180_days,
    CAST(DIV0(dwr_rev_after_first_180_days, num_dwr_orders_after_first_180_days) AS STRING) dwr_aov_after_first_180_days,
    CAST(DIV0(hay_rev_after_first_180_days, num_hay_orders_after_first_180_days) AS STRING) hay_aov_after_first_180_days

FROM
(SELECT
    entity_id entity_id,
    listagg(DISTINCT nt_email, ',') nt_emails,
    listagg(DISTINCT c_email, ',') c_emails,
    MAX(shipaddress) shipaddress,
    MIN(trandate) first_order,
    MAX(trandate) last_order,
    listagg(brand, ',') within group (order by trandate asc) brands,
    COUNT(DISTINCT transaction_id) num_orders,
    SUM(income_amount_usd) total_revenue,
    COUNT(DISTINCT CASE WHEN BRAND = 'Herman Miller' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN transaction_id ELSE NULL END ) num_hm_orders_first_180_days,
    COUNT(DISTINCT CASE WHEN BRAND = 'Design Within Reach' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN transaction_id ELSE NULL END ) num_dwr_orders_first_180_days,
    COUNT(DISTINCT CASE WHEN BRAND = 'HAY' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN transaction_id ELSE NULL END ) num_hay_orders_first_180_days,

    SUM(CASE WHEN BRAND = 'Herman Miller' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN income_amount_usd ELSE 0 END ) hm_rev_first_180_days,
    SUM(CASE WHEN BRAND = 'Design Within Reach' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN income_amount_usd ELSE 0 END ) dwr_rev_first_180_days,
    SUM(CASE WHEN BRAND = 'HAY' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN income_amount_usd ELSE 0 END ) hay_rev_first_180_days,

    SUM(CASE WHEN BRAND = 'Herman Miller' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN total_item_count ELSE 0 END ) hm_items_bought_first_180_days,
    SUM(CASE WHEN BRAND = 'Design Within Reach' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN total_item_count ELSE 0 END ) dwr_items_bought_first_180_days,
    SUM(CASE WHEN BRAND = 'HAY' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN total_item_count ELSE 0 END ) hay_items_bought_first_180_days,

    SUM(CASE WHEN channel='Contract' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180  
        THEN 1 ELSE 0 END) Contract_orders_first_180_days,
    SUM(CASE WHEN channel='Corporate' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN 1 ELSE 0 END) Corporate_orders_first_180_days,
    SUM(CASE WHEN channel='Studio' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN 1 ELSE 0 END) Studio_orders_first_180_days,
    SUM(CASE WHEN channel='Wholesale' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN 1 ELSE 0 END) Wholesale_orders_first_180_days,
    SUM(CASE WHEN channel='HM SF Chestnut St|354' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN 1 ELSE 0 END) HM_SF_Chestnut_orders_first_180_days,
    SUM(CASE WHEN channel='Call Center' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN 1 ELSE 0 END) Call_Center_orders_first_180_days,
    SUM(CASE WHEN channel='Fabric' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN 1 ELSE 0 END) Fabric_orders_first_180_days,
    SUM(CASE WHEN channel='International' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN 1 ELSE 0 END) International_orders_first_180_days,
    SUM(CASE WHEN channel='Outlet' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN 1 ELSE 0 END) Outlet_orders_first_180_days,
    SUM(CASE WHEN channel='Warehouses' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN 1 ELSE 0 END) Warehouses_orders_first_180_days,
    SUM(CASE WHEN channel='Offsite Inventory' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN 1 ELSE 0 END) Offsite_Inventory_orders_first_180_days,
    SUM(CASE WHEN channel='HM Retail' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN 1 ELSE 0 END) HM_Retail_orders_first_180_days,
    SUM(CASE WHEN channel='Web' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN 1 ELSE 0 END) Web_orders_first_180_days,

    SUM(CASE WHEN channel='Contract' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180  
        THEN income_amount_usd  ELSE 0 END) Contract_rev_first_180_days,
    SUM(CASE WHEN channel='Corporate' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN income_amount_usd  ELSE 0 END) Corporate_rev_first_180_days,
    SUM(CASE WHEN channel='Studio' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN income_amount_usd  ELSE 0 END) Studio_rev_first_180_days,
    SUM(CASE WHEN channel='Wholesale' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN income_amount_usd  ELSE 0 END) Wholesale_rev_first_180_days,
    SUM(CASE WHEN channel='HM SF Chestnut St|354' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN income_amount_usd  ELSE 0 END) HM_SF_Chestnut_rev_first_180_days,
    SUM(CASE WHEN channel='Call Center' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN income_amount_usd  ELSE 0 END) Call_Center_rev_first_180_days,
    SUM(CASE WHEN channel='Fabric' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN income_amount_usd  ELSE 0 END) Fabric_rev_first_180_days,
    SUM(CASE WHEN channel='International' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN income_amount_usd  ELSE 0 END) International_rev_first_180_days,
    SUM(CASE WHEN channel='Outlet' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN income_amount_usd  ELSE 0 END) Outlet_rev_first_180_days,
    SUM(CASE WHEN channel='Warehouses' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN income_amount_usd  ELSE 0 END) Warehouses_rev_first_180_days,
    SUM(CASE WHEN channel='Offsite Inventory' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN income_amount_usd  ELSE 0 END) Offsite_Inventory_rev_first_180_days,
    SUM(CASE WHEN channel='HM Retail' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN income_amount_usd  ELSE 0 END) HM_Retail_rev_first_180_days,
    SUM(CASE WHEN channel='Web' AND DATEDIFF('day', min_trandate, trandate) BETWEEN 0 AND 180 
        THEN income_amount_usd  ELSE 0 END) Web_rev_first_180_days,

    COUNT(DISTINCT CASE WHEN BRAND = 'Herman Miller' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN transaction_id ELSE NULL END ) num_hm_orders_after_first_180_days,
    COUNT(DISTINCT CASE WHEN BRAND = 'Design Within Reach' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN transaction_id ELSE NULL END ) num_dwr_orders_after_first_180_days,
    COUNT(DISTINCT CASE WHEN BRAND = 'HAY' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN transaction_id ELSE NULL END ) num_hay_orders_after_first_180_days,

    SUM(CASE WHEN BRAND = 'Herman Miller' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN income_amount_usd ELSE 0 END ) hm_rev_after_first_180_days,
    SUM(CASE WHEN BRAND = 'Design Within Reach' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN income_amount_usd ELSE 0 END ) dwr_rev_after_first_180_days,
    SUM(CASE WHEN BRAND = 'HAY' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN income_amount_usd ELSE 0 END ) hay_rev_after_first_180_days,

    SUM(CASE WHEN BRAND = 'Herman Miller' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN total_item_count ELSE 0 END ) hm_items_bought_after_first_180_days,
    SUM(CASE WHEN BRAND = 'Design Within Reach' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN total_item_count ELSE 0 END ) dwr_items_bought_after_first_180_days,
    SUM(CASE WHEN BRAND = 'HAY' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN total_item_count ELSE 0 END ) hay_items_bought_after_first_180_days,

    SUM(CASE WHEN channel='Contract' AND DATEDIFF('day', min_trandate, trandate) > 180 
        THEN 1 ELSE 0 END) Contract_orders_after_first_180_days,
    SUM(CASE WHEN channel='Corporate' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN 1 ELSE 0 END) Corporate_orders_after_first_180_days,
    SUM(CASE WHEN channel='Studio' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN 1 ELSE 0 END) Studio_orders_after_first_180_days,
    SUM(CASE WHEN channel='Wholesale' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN 1 ELSE 0 END) Wholesale_orders_after_first_180_days,
    SUM(CASE WHEN channel='HM SF Chestnut St|354' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN 1 ELSE 0 END) HM_SF_Chestnut_orders_after_first_180_days,
    SUM(CASE WHEN channel='Call Center' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN 1 ELSE 0 END) Call_Center_orders_after_first_180_days,
    SUM(CASE WHEN channel='Fabric' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN 1 ELSE 0 END) Fabric_orders_after_first_180_days,
    SUM(CASE WHEN channel='International' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN 1 ELSE 0 END) International_orders_after_first_180_days,
    SUM(CASE WHEN channel='Outlet' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN 1 ELSE 0 END) Outlet_orders_after_first_180_days,
    SUM(CASE WHEN channel='Warehouses' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN 1 ELSE 0 END) Warehouses_orders_after_first_180_days,
    SUM(CASE WHEN channel='Offsite Inventory' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN 1 ELSE 0 END) Offsite_Inventory_orders_after_first_180_days,
    SUM(CASE WHEN channel='HM Retail' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN 1 ELSE 0 END) HM_Retail_orders_after_first_180_days,
    SUM(CASE WHEN channel='Web' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN 1 ELSE 0 END) Web_orders_after_first_180_days,

    SUM(CASE WHEN channel='Contract' AND DATEDIFF('day', min_trandate, trandate) > 180 
        THEN income_amount_usd ELSE 0 END) Contract_rev_after_first_180_days,
    SUM(CASE WHEN channel='Corporate' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN income_amount_usd ELSE 0 END) Corporate_rev_after_first_180_days,
    SUM(CASE WHEN channel='Studio' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN income_amount_usd ELSE 0 END) Studio_rev_after_first_180_days,
    SUM(CASE WHEN channel='Wholesale' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN income_amount_usd ELSE 0 END) Wholesale_rev_after_first_180_days,
    SUM(CASE WHEN channel='HM SF Chestnut St|354' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN income_amount_usd ELSE 0 END) HM_SF_Chestnut_rev_after_first_180_days,
    SUM(CASE WHEN channel='Call Center' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN income_amount_usd ELSE 0 END) Call_Center_rev_after_first_180_days,
    SUM(CASE WHEN channel='Fabric' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN income_amount_usd ELSE 0 END) Fabric_rev_after_first_180_days,
    SUM(CASE WHEN channel='International' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN income_amount_usd ELSE 0 END) International_rev_after_first_180_days,
    SUM(CASE WHEN channel='Outlet' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN income_amount_usd ELSE 0 END) Outlet_rev_after_first_180_days,
    SUM(CASE WHEN channel='Warehouses' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN income_amount_usd ELSE 0 END) Warehouses_rev_after_first_180_days,
    SUM(CASE WHEN channel='Offsite Inventory' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN income_amount_usd ELSE 0 END) Offsite_Inventory_rev_after_first_180_days,
    SUM(CASE WHEN channel='HM Retail' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN income_amount_usd ELSE 0 END) HM_Retail_rev_after_first_180_days,
    SUM(CASE WHEN channel='Web' AND DATEDIFF('day', min_trandate, trandate) > 180
        THEN income_amount_usd ELSE 0 END) Web_rev_after_first_180_days
FROM
  (SELECT nt.transaction_id, 
          nt.entity_id, 
          nt.location_id,
          fc.fiscal_day trandate,
          nt.email nt_email, 
          le.list_item_name brand,
          pl.name channel,
          mt.min_trandate,
          SUM(case when collate(gl.ACCOUNTTYPE, 'en-ci') = 'Income' then ntl.AMOUNT * -1  * nt.EXCHANGE_RATE else 0 end) as income_amount_usd,
          SUM(case when collate(gl.ACCOUNTTYPE, 'en-ci') = 'Tax' then ntl.AMOUNT * -1  * nt.EXCHANGE_RATE else 0 end) as tax_amount_usd,
          SUM(case when collate(gl.ACCOUNTTYPE, 'en-ci') = 'Freight' then ntl.AMOUNT * -1 * nt.EXCHANGE_RATE else 0 end) as freight_amount_usd,
          SUM(case when collate(gl.ACCOUNTTYPE, 'en-ci') = 'Income' then ntl.ITEM_COUNT * -1 else 0 end) as total_item_count,
          MAX(c.shipaddress) shipaddress,
          MAX(c.email) c_email
  FROM "NORTHSTAR"."RAW"."NETSUITE_TRANSACTIONS" nt
  JOIN (SELECT  entity_id, MIN(trandate) min_trandate
        FROM "NORTHSTAR"."RAW"."NETSUITE_TRANSACTIONS"
        GROUP BY 1) mt
  ON nt.entity_id = mt.entity_id
  JOIN "NORTHSTAR"."RAW"."NETSUITE_TRANSACTION_LINES" ntl
      ON nt.transaction_id=ntl.transaction_id
  LEFT JOIN RAW.NETSUITE_CUSTOMERS c
      ON nt.entity_id = c.customer_id
  LEFT JOIN RAW.NETSUITE_ACCOUNTS a
      ON ntl.ACCOUNT_ID = a.ACCOUNT_ID 
  LEFT JOIN (select * from RAW.NETSUITE_GLAccountNumbers where SourceSystem_ID = 5)  gl
      ON a.accountnumber = gl.GLAccountNumber
  LEFT JOIN RAW.NETSUITE_LOCATIONS l
      ON l.location_id = nt.location_id
  LEFT JOIN RAW.NETSUITE_LOCATION_ENTITY le 
      ON l.location_entity_id = le.list_id
  LEFT JOIN "NORTHSTAR"."BO"."FISCAL_DAY_CALENDAR" fc
      ON nt.trandate = fc.fiscal_day
  LEFT JOIN ( SELECT * 
             FROM "NORTHSTAR"."RAW"."NETSUITE_LOCATIONS"
             WHERE parent_id IS NULL) pl
      ON l.parent_id = pl.location_id
  WHERE (nt.transaction_type = 'Sales Order' OR nt.transaction_type = 'Cash Sale')
  GROUP BY nt.transaction_id, nt.entity_id, mt.min_trandate, nt.location_id, fc.fiscal_day, nt.email, le.list_item_name,  pl.name) tl
GROUP BY entity_id) a)
FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY='"')
SINGLE = TRUE
MAX_FILE_SIZE = 167772160
OVERWRITE = TRUE;
