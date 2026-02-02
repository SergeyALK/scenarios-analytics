import sqlite3
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

# Выгрузка необходимых таблиц и справочников из базы данных

def import_data():
    dbfile = 'data/db_full.sqlite'
    conn = sqlite3.connect(dbfile)
    df_main = pd.read_sql_query("""WITH tmp_1 AS (
        SELECT
            contract_id,
            customer_id,
            department_id,
            tnbd_id,
            amount,
            price_id,
            strftime('%m', date) AS month,
            'plan' AS operation_name
        FROM plan

        UNION ALL

        SELECT
            contract_id,
            customer_id,
            department_id,
            tnbd_id,
            amount,
            price_id,
            strftime('%m', date) AS month,
            'production' AS operation_name
        FROM production
    ),

    tmp_2 AS (
        SELECT 
            department,
            contracts.contract,
            contract_attribute,
            customer,
            name AS product_name,
            full_tech_group,
            tmp_1.price_id,
            amount,
            price_without_tax,
            operation_name,
            month,
            CASE WHEN operation_name = 'plan' AND month = '01' THEN amount ELSE 0 END AS january_plan,
            CASE WHEN operation_name = 'plan' AND month = '02' THEN amount ELSE 0 END AS february_plan,
            CASE WHEN operation_name = 'plan' AND month = '03' THEN amount ELSE 0 END AS march_plan,
            CASE WHEN operation_name = 'plan' AND month = '04' THEN amount ELSE 0 END AS april_plan,
            CASE WHEN operation_name = 'plan' AND month = '05' THEN amount ELSE 0 END AS may_plan,
            CASE WHEN operation_name = 'plan' AND month = '06' THEN amount ELSE 0 END AS june_plan,
            CASE WHEN operation_name = 'plan' AND month = '07' THEN amount ELSE 0 END AS july_plan,
            CASE WHEN operation_name = 'plan' AND month = '08' THEN amount ELSE 0 END AS august_plan,
            CASE WHEN operation_name = 'plan' AND month = '09' THEN amount ELSE 0 END AS september_plan,
            CASE WHEN operation_name = 'plan' AND month = '10' THEN amount ELSE 0 END AS october_plan,
            CASE WHEN operation_name = 'plan' AND month = '11' THEN amount ELSE 0 END AS november_plan,
            CASE WHEN operation_name = 'plan' AND month = '12' THEN amount ELSE 0 END AS december_plan,
            CASE WHEN operation_name = 'production' AND month = '01' THEN amount ELSE 0 END AS january_production,
            CASE WHEN operation_name = 'production' AND month = '02' THEN amount ELSE 0 END AS february_production,
            CASE WHEN operation_name = 'production' AND month = '03' THEN amount ELSE 0 END AS march_production,
            CASE WHEN operation_name = 'production' AND month = '04' THEN amount ELSE 0 END AS april_production,
            CASE WHEN operation_name = 'production' AND month = '05' THEN amount ELSE 0 END AS may_production,
            CASE WHEN operation_name = 'production' AND month = '06' THEN amount ELSE 0 END AS june_production,
            CASE WHEN operation_name = 'production' AND month = '07' THEN amount ELSE 0 END AS july_production,
            CASE WHEN operation_name = 'production' AND month = '08' THEN amount ELSE 0 END AS august_production,
            CASE WHEN operation_name = 'production' AND month = '09' THEN amount ELSE 0 END AS september_production,
            CASE WHEN operation_name = 'production' AND month = '10' THEN amount ELSE 0 END AS october_production,
            CASE WHEN operation_name = 'production' AND month = '11' THEN amount ELSE 0 END AS november_production,
            CASE WHEN operation_name = 'production' AND month = '12' THEN amount ELSE 0 END AS december_production
        FROM tmp_1
        INNER JOIN tnbd ON tmp_1.tnbd_id = tnbd.tnbd_id
        INNER JOIN contracts ON tmp_1.contract_id = contracts.contract_id
        INNER JOIN customers ON tmp_1.customer_id = customers.customer_id
        INNER JOIN departments ON tmp_1.department_id = departments.department_id
        INNER JOIN price ON tmp_1.price_id = price.price_id
    ),

    tmp_3 AS (
        SELECT
            orders.product_order,
            contract,
            full_tech_group
        FROM orders
        INNER JOIN contracts ON orders.contract_id = contracts.contract_id
        INNER JOIN tnbd ON orders.tnbd_id = tnbd.tnbd_id
    )

    SELECT
        department,
        tmp_2.contract,
        contract_attribute,
        customer,
        product_name,
        tmp_2.full_tech_group,
        price_id,
        tmp_3.product_order,
        SUM(
            january_plan + february_plan + march_plan + april_plan + may_plan + june_plan +
            july_plan + august_plan + september_plan + october_plan + november_plan + december_plan
        ) AS plan,
        SUM(january_plan) AS january_plan,
        SUM(february_plan) AS february_plan,
        SUM(march_plan) AS march_plan,
        SUM(april_plan) AS april_plan,
        SUM(may_plan) AS may_plan,
        SUM(june_plan) AS june_plan,
        SUM(july_plan) AS july_plan,
        SUM(august_plan) AS august_plan,
        SUM(september_plan) AS september_plan,
        SUM(october_plan) AS october_plan,
        SUM(november_plan) AS november_plan,
        SUM(december_plan) AS december_plan,
        SUM(
            january_production + february_production + march_production + april_production + may_production +
            june_production + july_production + august_production + september_production +
            october_production + november_production + december_production
        ) AS production,
        SUM(january_production) AS january_production,
        SUM(february_production) AS february_production,
        SUM(march_production) AS march_production,
        SUM(april_production) AS april_production,
        SUM(may_production) AS may_production,
        SUM(june_production) AS june_production,
        SUM(july_production) AS july_production,
        SUM(august_production) AS august_production,
        SUM(september_production) AS september_production,
        SUM(october_production) AS october_production,
        SUM(november_production) AS november_production,
        SUM(december_production) AS december_production
    FROM tmp_2
    INNER JOIN tmp_3 ON
        tmp_2.contract = tmp_3.contract
        AND tmp_2.full_tech_group = tmp_3.full_tech_group
    GROUP BY
        department,
        tmp_2.contract,
        contract_attribute,
        customer,
        product_name,
        tmp_2.full_tech_group,
        price_id,
        tmp_3.product_order""", conn)

    df_rri = pd.read_sql_query("SELECT * FROM rri", conn)
    df_costs_items = pd.read_sql_query("SELECT * FROM costs_items", conn)
    df_departments = pd.read_sql_query("SELECT * FROM departments", conn)
    df_fact = pd.read_sql_query('''
    SELECT
        product_order,
        cost_item, department,
        total_sum AS fact,
        CASE WHEN month = 1 THEN total_sum ELSE 0 END AS january_fact,
        CASE WHEN month = 2 THEN total_sum ELSE 0 END AS february_fact,
        CASE WHEN month = 3 THEN total_sum ELSE 0 END AS march_fact,
        CASE WHEN month = 4 THEN total_sum ELSE 0 END AS april_fact,
        CASE WHEN month = 5 THEN total_sum ELSE 0 END AS may_fact,
        CASE WHEN month = 6 THEN total_sum ELSE 0 END AS june_fact,
        CASE WHEN month = 7 THEN total_sum ELSE 0 END AS july_fact,
        CASE WHEN month = 8 THEN total_sum ELSE 0 END AS august_fact,
        CASE WHEN month = 9 THEN total_sum ELSE 0 END AS september_fact,
        CASE WHEN month = 10 THEN total_sum ELSE 0 END AS october_fact,
        CASE WHEN month = 11 THEN total_sum ELSE 0 END AS november_fact,
        CASE WHEN month = 12 THEN total_sum ELSE 0 END AS december_fact,
        0 AS plan,
        0 AS january_plan,
        0 AS february_plan,
        0 AS march_plan,
        0 AS april_plan,
        0 AS may_plan,
        0 AS june_plan,
        0 AS july_plan,
        0 AS august_plan,
        0 AS september_plan,
        0 AS october_plan,
        0 AS november_plan,
        0 AS december_plan,
        0 AS wip
    FROM fact
    INNER JOIN orders
    ON fact.order_id = orders.order_id
    INNER JOIN costs_items
    ON fact.cost_item_id = costs_items.cost_item_id
    INNER JOIN departments
    ON fact.department_id = departments.department_id''', conn)

    df_wip = pd.read_sql_query('''
    SELECT product_order, cost_item, department, total_sum as wip,
    0 AS fact,
    0 AS january_fact,
    0 AS february_fact,
    0 AS march_fact,
    0 AS april_fact,
    0 AS may_fact,
    0 AS june_fact,
    0 AS july_fact,
    0 AS august_fact,
    0 AS september_fact,
    0 AS october_fact,
    0 AS november_fact,
    0 AS december_fact,
    0 AS plan,
    0 AS january_plan,
    0 AS february_plan,
    0 AS march_plan,
    0 AS april_plan,
    0 AS may_plan,
    0 AS june_plan,
    0 AS july_plan,
    0 AS august_plan,
    0 AS september_plan,
    0 AS october_plan,
    0 AS november_plan,
    0 AS december_plan
    FROM wip
    INNER JOIN orders
    ON wip.order_id = orders.order_id
    INNER JOIN costs_items
    ON wip.cost_item_id = costs_items.cost_item_id
    INNER JOIN departments
    ON wip.department_id = departments.department_id ''', conn)

    df_calculations = pd.read_sql_query("""
    SELECT
        price_id,
        department,
        cost_item,
        amount_per_unit,
        calculations.department_id,
        calculations.cost_item_id
    FROM calculations
    INNER JOIN departments
    ON calculations.department_id = departments.department_id
    INNER JOIN costs_items
    ON calculations.cost_item_id = costs_items.cost_item_id""", conn)

    conn.close()
    return df_main, df_rri, df_costs_items, df_departments, df_fact, df_wip, df_calculations
    # На выходе контрактная база (df_main), нормативы (df_rri), справочник калькуляционных статей (df_costs_items),
    # справочник подразделений (df_departments), факт выпуска на текущую дату (df_fact), незавершенное производство
    # на текущую дату (df_wip) и реестр утвержденных калькуляций (df_calculations)

