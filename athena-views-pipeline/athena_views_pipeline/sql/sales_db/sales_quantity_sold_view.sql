-- SCHEDULE: "cron(*/10 * * * ? *)"
SELECT *
FROM "sales_db"."sales_db_table"
where "quantity_sold" > 40