COPY INTO 's3://hermanmiller/data/queries/2021_04_12/email_brand_differences_between_engagement.csv.gz'
FROM
    (SELECT * FROM 
      (SELECT CAST(email_address AS STRING) email_address,
              CAST(brand AS STRING) brand,
              CAST(vendor_envelope_no AS STRING) vendor_envelope_no,
              CAST(email_category AS STRING) email_category,
              CAST(send_date AS STRING) first_send_date,
              CAST(engage_date AS STRING) first_engage_date,
              CAST(num_opens AS STRING) num_opens,
              CAST(DATEDIFF('day', lag(engage_date) over (partition by email_address order by engage_date), engage_date) AS STRING) engage_diff
      FROM
        (SELECT email_address, 
                brand,
                vendor_envelope_no, 
                email_category,
                DATE(MIN(CASE WHEN hm_event_supertype = 'SEND' THEN event_timestamp ELSE NULL END)) send_date,
                DATE(MIN(CASE WHEN hm_event_type in ('OPEN', 'CLICK') THEN event_timestamp ELSE NULL END)) engage_date,
                SUM(CASE WHEN hm_event_type = 'OPEN' THEN 1 ELSE 0 END ) num_opens
        FROM "NORTHSTAR"."MKTG"."EMAIL_BASE"
        GROUP BY email_address, brand, vendor_envelope_no, email_category) a
      WHERE send_date IS NOT NULL and engage_date IS NOT NULL
      ORDER BY email_address, engage_date asc))
FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY='"')
SINGLE = TRUE
OVERWRITE = TRUE
MAX_FILE_SIZE = 1677721600;
