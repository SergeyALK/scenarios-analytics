import warnings
warnings.filterwarnings('ignore')

def make_control():

    from modules.scenario_2 import start_scenario_2
    from modules.preprocessing import start_preprocessing

    df_main, _, completed_month = start_preprocessing()
    df_dir_fin = start_scenario_2()

    # Закрытые заказы уже не подлежат корректировке, но позволяют отследить ситуацию по изделиям. Для этого необходимо
    # выделить те закрытые заказы, фактическая рентабельность которых превышает допустимый порог в 25 % или же она
    # отрицательная. Дальнейшая информация передавалась в отдельное ведомство, занимающееся контролем за
    # производством и курирующая списание затрат по каждому производственному подразделению

    # Для этого рассчитывается накопительный план и факт в зависимости от периода

    months = ['january', 'february', 'march', 'april',
              'may', 'june', 'july', 'august', 'september',
              'october', 'november', 'december']

    plan_columns = [f'{month}_plan' for month in months[:completed_month]]
    sum_plan = df_dir_fin[plan_columns].sum(axis=1)
    df_dir_fin['accumulative_plan'] = sum_plan

    fact_columns = [f'{month}_fact' for month in months[:completed_month]]
    sum_fact = df_dir_fin[fact_columns].sum(axis=1)
    df_dir_fin['accumulative_fact'] = sum_fact

    # Отбираются закрытые заказы

    df_tmp = df_dir_fin[df_dir_fin['order_status'] == "Закрыт"][
        ['main_performer', 'contract', 'contract_attribute', 'customer', 'full_tech_group', 'product_order',
         'order_status']
    ].drop_duplicates()

    # Выделяется фактическая рентабельность производства

    df_profit_fact = df_dir_fin[df_dir_fin['cost_item'] == 'Прибыль'].groupby('product_order')[['accumulative_fact']].sum()
    df_total_costs_fact = df_dir_fin[df_dir_fin['cost_item'] == 'Полная себестоимость'].groupby('product_order')[
        ['accumulative_fact']].sum()
    df_tmp['income_fact'] = df_tmp['product_order'].map(
        df_profit_fact.groupby('product_order')['accumulative_fact'].sum()).fillna(0)
    df_tmp['total_costs_fact'] = df_tmp['product_order'].map(
        df_total_costs_fact.groupby('product_order')['accumulative_fact'].sum()).fillna(0)
    df_tmp['profitability_fact'] = df_tmp['income_fact'] / df_tmp['total_costs_fact']
    df_tmp = df_tmp[(df_tmp['profitability_fact'] < 0) | (df_tmp['profitability_fact'] > 0.25)]

    columns = ['main_performer', 'contract', 'contract_attribute', 'customer', 'full_tech_group',
               'product_order', 'order_fact_closing_date', 'cost_item', 'department', 'accumulative_plan',
               'accumulative_fact']

    df_report = df_dir_fin[columns]

    # делается отбор необходимых заказов

    df_report['to_work'] = df_report['product_order'].isin(df_tmp['product_order']).replace({True: 'yes', False: 'no'})

    df_report = df_report[df_report['to_work'] == 'yes']

    # В качестве дополнительной информации выводится количество штук по каждому заказу. Это позволит оценить
    # усредненные показатели

    actual_columns = [f'{month}_actual' for month in months[:completed_month]]
    sum_accumulative_amount = df_main[actual_columns].sum(axis=1)
    df_main['accumulative_amount'] = sum_accumulative_amount

    df_report['amount'] = df_report['product_order'].map(df_main.groupby('product_order')['accumulative_amount'].sum()).fillna(
        0)

    df_report.drop(columns=['to_work'], inplace=True)

    return df_report, df_dir_fin
    # На выходе перечень закрытых заказов с рентабельностью > 0 или > 25% (df_report) а также нормативный расчет бдр с
    # накопительным планом и фактом (df_dir_fin), который потребуется для формирования ориентиров по списанию затрат
    # для подразделений





