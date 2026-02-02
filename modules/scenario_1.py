import pandas as pd
import warnings
warnings.filterwarnings('ignore')

def start_scenario_1():

    from modules.load import import_data
    from modules.preprocessing import start_preprocessing

    _, df_rri, df_costs_items, df_departments, df_fact, df_wip, df_calculations = import_data()
    df_main, df_closing_dates, completed_month = start_preprocessing()

    # Первый сценарий расчета - БДР по утвержденным калькуляциям. По коду калькуляции к полученному оперативному
    # плану по штукам притягиваются соответствующие калькуляции по подразделениям-соисполнителям и калькуляционным
    # статьям. План, факт и незавершенное производство приводятся в единый формат. Далее производится конкатенация

    columns = ['department', 'contract', 'contract_attribute', 'customer', 'product_name', 'full_tech_group',
               'price_id',
               'product_order', 'plan_actual', 'january_actual', 'february_actual', 'march_actual', 'april_actual',
               'may_actual', 'june_actual', 'july_actual', 'august_actual', 'september_actual', 'october_actual',
               'november_actual', 'december_actual', 'non_completion']
    df_calc_1 = df_main[columns]

    df_calculations['tmp'] = df_calculations['department_id'].astype(str) + '_' + df_calculations[
        'cost_item_id'].astype(str)
    new_columns = df_calculations['tmp'].drop_duplicates().tolist()
    df_calculations.drop(columns='tmp', inplace=True)

    for column in new_columns:
        department_id_str, cost_item_id_str = column.split('_')
        department_id = int(department_id_str)
        cost_item_id = int(cost_item_id_str)

        df_filtered = df_calculations[
            (df_calculations['department_id'] == department_id) &
            (df_calculations['cost_item_id'] == cost_item_id)]

        df_calc_1[column] = df_calc_1['price_id'].map(df_filtered.groupby('price_id')['amount_per_unit'].sum()).fillna(
            0)

    # Выбор необходимых числовых колонок с исключением
    
    columns_list = [
        col for col in df_calc_1.select_dtypes(include='number').columns
        if col not in [
            'price_id', 'plan_actual', 'january_actual', 'february_actual',
            'march_actual', 'april_actual', 'may_actual', 'june_actual',
            'july_actual', 'august_actual', 'september_actual', 'october_actual',
            'november_actual', 'december_actual', 'non_completion'
        ]
    ]

    # Транспонирование таблицы

    df_calc_2 = pd.melt(df_calc_1,
                        id_vars=['department', 'contract', 'contract_attribute', 'customer',
                                 'product_name', 'full_tech_group', 'price_id', 'product_order',
                                 'plan_actual', 'january_actual', 'february_actual', 'march_actual',
                                 'april_actual', 'may_actual', 'june_actual', 'july_actual', 'august_actual',
                                 'september_actual', 'october_actual', 'november_actual', 'december_actual',
                                 'non_completion'],
                        value_vars=columns_list,
                        var_name='columns',
                        value_name='amount_per_unit')
    df_calc_2 = df_calc_2[df_calc_2['amount_per_unit'] != 0]

    # Расчет объемов по каждому месяцу, расшифровка статей затрат и подразделений-исполнителей

    df_calc_2['plan'] = df_calc_2['plan_actual'] * df_calc_2['amount_per_unit']
    df_calc_2['january_plan'] = df_calc_2['january_actual'] * df_calc_2['amount_per_unit']
    df_calc_2['february_plan'] = df_calc_2['february_actual'] * df_calc_2['amount_per_unit']
    df_calc_2['march_plan'] = df_calc_2['march_actual'] * df_calc_2['amount_per_unit']
    df_calc_2['april_plan'] = df_calc_2['april_actual'] * df_calc_2['amount_per_unit']
    df_calc_2['may_plan'] = df_calc_2['may_actual'] * df_calc_2['amount_per_unit']
    df_calc_2['june_plan'] = df_calc_2['june_actual'] * df_calc_2['amount_per_unit']
    df_calc_2['july_plan'] = df_calc_2['july_actual'] * df_calc_2['amount_per_unit']
    df_calc_2['august_plan'] = df_calc_2['august_actual'] * df_calc_2['amount_per_unit']
    df_calc_2['september_plan'] = df_calc_2['september_actual'] * df_calc_2['amount_per_unit']
    df_calc_2['october_plan'] = df_calc_2['october_actual'] * df_calc_2['amount_per_unit']
    df_calc_2['november_plan'] = df_calc_2['november_actual'] * df_calc_2['amount_per_unit']
    df_calc_2['december_plan'] = (df_calc_2['december_actual'] * df_calc_2['amount_per_unit']
                                  + df_calc_2['non_completion'] * df_calc_2['amount_per_unit'])

    df_calc_2 = pd.merge(df_calc_2,
                         df_closing_dates[['product_order', 'order_status', 'order_plan_closing_date',
                                           'order_fact_closing_date']], on='product_order', how='inner')

    df_calc_2['department_id'] = df_calc_2['columns'].apply(lambda x: int(x.split('_')[0]))
    df_calc_2['cost_item_id'] = df_calc_2['columns'].apply(lambda x: int(x.split('_')[1]))
    df_calc_2.rename(columns={'department': 'main_performer'}, inplace=True)
    df_calc_2 = pd.merge(df_calc_2, df_costs_items[['cost_item_id', 'cost_item']], on='cost_item_id', how='inner')
    df_calc_2 = pd.merge(df_calc_2, df_departments[['department_id', 'department']], on='department_id', how='inner')

    # Удаление ненужных признаков

    columns_to_drop = ['plan_actual', 'january_actual', 'february_actual', 'march_actual',
                       'april_actual', 'may_actual', 'june_actual', 'july_actual',
                       'august_actual', 'september_actual', 'october_actual',
                       'november_actual', 'december_actual', 'non_completion',
                       'amount_per_unit', 'columns', 'price_id', 'department_id', 'cost_item_id',
                       'product_name']
    df_calc_2.drop(columns=columns_to_drop, inplace=True)

    # Схлопывание таблицы. Информация о литерах не требуется, так как присутствует номенклатурная группа изделий

    positions = ['main_performer', 'contract', 'contract_attribute', 'customer', 'full_tech_group',
                 'product_order', 'order_status', 'order_plan_closing_date', 'order_fact_closing_date',
                 'cost_item', 'department', 'plan', 'january_plan', 'february_plan', 'march_plan', 'april_plan',
                 'may_plan', 'june_plan', 'july_plan', 'august_plan', 'september_plan', 'october_plan',
                 'november_plan', 'december_plan']

    df_calc_fin = pd.pivot_table(df_calc_2,
                                 values=['plan', 'january_plan', 'february_plan', 'march_plan',
                                         'april_plan', 'may_plan', 'june_plan', 'july_plan', 'august_plan',
                                         'september_plan', 'october_plan', 'november_plan', 'december_plan'],
                                 index=['main_performer', 'contract', 'contract_attribute',
                                        'customer', 'full_tech_group', 'product_order', 'order_status',
                                        'order_plan_closing_date', 'order_fact_closing_date', 'cost_item',
                                        'department'],
                                 aggfunc='sum').reset_index()
    df_calc_fin = df_calc_fin[positions]

    # Приведение к единому формату. Для этого добавляются недостающие колонки

    new_columns = ['fact', 'january_fact', 'february_fact', 'march_fact', 'april_fact', 'may_fact',
                   'june_fact', 'july_fact', 'august_fact', 'september_fact', 'october_fact',
                   'november_fact', 'december_fact', 'wip']

    for column in new_columns:
        df_calc_fin[column] = 0.0


    df_tmp = df_calc_fin[['main_performer', 'contract', 'contract_attribute',
                          'customer', 'full_tech_group', 'product_order', 'order_status',
                          'order_plan_closing_date', 'order_fact_closing_date']].drop_duplicates()
    df_fact = pd.merge(df_fact, df_tmp, on='product_order', how='left')
    df_wip = pd.merge(df_wip, df_tmp, on='product_order', how='left')

    df_calc_fin = pd.concat([df_calc_fin, df_fact, df_wip])

    positions = ['main_performer', 'contract', 'contract_attribute', 'customer',
                 'full_tech_group', 'product_order', 'order_status', 'order_plan_closing_date',
                 'order_fact_closing_date', 'cost_item', 'department', 'plan', 'january_plan',
                 'february_plan', 'march_plan', 'april_plan', 'may_plan', 'june_plan', 'july_plan',
                 'august_plan', 'september_plan', 'october_plan', 'november_plan', 'december_plan',
                 'fact', 'january_fact', 'february_fact', 'march_fact', 'april_fact', 'may_fact',
                 'june_fact', 'july_fact', 'august_fact', 'september_fact', 'october_fact',
                 'november_fact', 'december_fact', 'wip']

    df_calc_fin = pd.pivot_table(df_calc_fin,
                                 values=['plan', 'january_plan', 'february_plan', 'march_plan', 'april_plan',
                                         'may_plan',
                                         'june_plan', 'july_plan', 'august_plan', 'september_plan', 'october_plan',
                                         'november_plan', 'december_plan', 'fact', 'january_fact', 'february_fact',
                                         'march_fact', 'april_fact', 'may_fact', 'june_fact', 'july_fact',
                                         'august_fact',
                                         'september_fact', 'october_fact', 'november_fact', 'december_fact', 'wip'],
                                 index=['main_performer', 'contract', 'contract_attribute', 'customer',
                                        'full_tech_group', 'product_order', 'order_status', 'order_plan_closing_date',
                                        'order_fact_closing_date', 'cost_item', 'department'],
                                 aggfunc='sum').reset_index()

    df_calc_fin = df_calc_fin[positions]

    df_calc_fin['completed_month'] = completed_month
    df_calc_fin['scenario'] = 'Утвержденный расчет'


    return df_calc_fin
    # На выходе первый расчетный сценарий - БДР, посчитанный по утвержденным калькуляциям, фактом и незавершенным
    # производством со всей необходимой детализацией

