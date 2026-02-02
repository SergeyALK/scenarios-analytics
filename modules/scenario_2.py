import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

def start_scenario_2():

    from modules.load import import_data
    from modules.preprocessing import start_preprocessing

    _, df_rri, df_costs_items, df_departments, df_fact, df_wip, df_calculations = import_data()
    df_main, df_closing_dates, completed_month = start_preprocessing()

    # Второй сценарий расчета - БДР по расчетным калькуляциям. Общая структура работы аналогично первому сценарию,
    # однако, здесь произведен расчет калькуляций по вводимой рентабельности и текущим нормативам по каждому
    # подразделению. Значение "profitability_input" может быть любым, однако на моей практике каноном считался расчет
    # 20+1, потому зафиксирую именно это значение

    profitability_input = 20

    columns = ['department', 'contract', 'contract_attribute', 'customer', 'product_name', 'full_tech_group',
               'price_id',
               'product_order', 'plan_actual', 'january_actual', 'february_actual', 'march_actual', 'april_actual',
               'may_actual', 'june_actual', 'july_actual', 'august_actual', 'september_actual', 'october_actual',
               'november_actual', 'december_actual', 'non_completion']
    df_dir_1 = df_main[columns]

    # Добавление нормативов

    list_tmp = list(df_rri['department_id'].unique())
    new_columns_rri = []
    for i in df_rri.columns[1:]:
        for j in list_tmp:
            new_columns_rri.append(str(j) + '&' + i)

    for column in new_columns_rri:
        department_id = int(column.split('&')[0])
        item_rri = column.split('&')[1]
        df_dir_1[column] = df_rri[df_rri['department_id'] == department_id][item_rri].values[0]

    # Добавление блока затрат из утвержденных калькуляций. Материалы, комплектация, периодические испытания,
    # трудоемкость, расходы на коммерцию, а также цена в двух сценариях не отличаются. Изменениям подвергается
    # добавленная стоимость

    df_tmp = df_calculations[df_calculations['cost_item_id'].isin([1, 2, 9, 8, 12, 13])]

    df_tmp['tmp'] = df_tmp['department_id'].astype(str) + '_' + df_tmp['cost_item_id'].astype(str)
    new_columns_dir = df_tmp['tmp'].drop_duplicates().tolist()
    df_tmp.drop(columns='tmp', inplace=True)

    for column in new_columns_dir:
        department_id_str, cost_item_id_str = column.split('_')
        department_id = int(department_id_str)
        cost_item_id = int(cost_item_id_str)

        df_filtered = df_tmp[
            (df_tmp['department_id'] == department_id) &
            (df_tmp['cost_item_id'] == cost_item_id)]

        df_dir_1[column] = df_dir_1['price_id'].map(df_filtered.groupby('price_id')['amount_per_unit'].sum()).fillna(0)

    # Расчет основной зарплаты

    df_tmp = df_calculations[df_calculations['cost_item_id'] == 3]
    df_tmp['tmp'] = df_tmp['department_id'].astype(str) + '_' + df_tmp['cost_item_id'].astype(str)
    new_columns_basic_salary = df_tmp['tmp'].drop_duplicates().tolist()
    df_tmp.drop(columns='tmp', inplace=True)

    new_columns_shr = [col for col in new_columns_rri if 'standard_hour_rate' in col]

    columns_labor = [col for col in df_dir_1 if '_13' in col]

    for column in new_columns_basic_salary:
        department_id = column.split('_')[0]
        labor = next((item for item in columns_labor if item.split('_')[0] == department_id), None)
        shr = next((item for item in new_columns_shr if item.split('&')[0] == department_id), None)
        df_dir_1[column] = df_dir_1[labor] * df_dir_1[shr]

    # Расчет дополнительной зарплаты

    df_tmp = df_calculations[df_calculations['cost_item_id'] == 4]
    df_tmp['tmp'] = df_tmp['department_id'].astype(str) + '_' + df_tmp['cost_item_id'].astype(str)
    new_columns_additional_salary = df_tmp['tmp'].drop_duplicates().tolist()
    df_tmp.drop(columns='tmp', inplace=True)

    for column in new_columns_additional_salary:
        department_id = column.split('_')[0]
        basic_salary = next((item for item in new_columns_basic_salary if item.split('_')[0] == department_id), None)
        df_dir_1[column] = df_dir_1[basic_salary] * 0.0961

    # Расчет начислений во внебюджетные фонды

    df_tmp = df_calculations[df_calculations['cost_item_id'] == 5]
    df_tmp['tmp'] = df_tmp['department_id'].astype(str) + '_' + df_tmp['cost_item_id'].astype(str)
    new_columns_accruals = df_tmp['tmp'].drop_duplicates().tolist()
    df_tmp.drop(columns='tmp', inplace=True)

    for column in new_columns_accruals:
        department_id = column.split('_')[0]
        basic_salary = next((item for item in new_columns_basic_salary if item.split('_')[0] == department_id), None)
        additional_salary = next((item for item in new_columns_additional_salary if item.split('_')[0] == department_id), None)
        df_dir_1[column] = (df_dir_1[basic_salary] + df_dir_1[additional_salary]) * 0.0835

    # Расчет общепроизводственных затрат

    df_tmp = df_calculations[df_calculations['cost_item_id'] == 6]
    df_tmp['tmp'] = df_tmp['department_id'].astype(str) + '_' + df_tmp['cost_item_id'].astype(str)
    new_columns_overhead_costs = df_tmp['tmp'].drop_duplicates().tolist()
    df_tmp.drop(columns='tmp', inplace=True)

    columns_overhead_costs = [col for col in df_dir_1 if 'overhead_costs' in col]

    for column in new_columns_overhead_costs:
        department_id = column.split('_')[0]
        basic_salary = next((item for item in new_columns_basic_salary if item.split('_')[0] == department_id), None)
        overhead_costs = next((item for item in columns_overhead_costs if item.split('&')[0] == department_id), None)

        df_dir_1[column] = (df_dir_1[basic_salary] * df_dir_1[overhead_costs]) / 100

    # Расчет административно-управленческих затрат (они же общехозяйственные)

    df_tmp = df_calculations[df_calculations['cost_item_id'] == 7]
    df_tmp['tmp'] = df_tmp['department_id'].astype(str) + '_' + df_tmp['cost_item_id'].astype(str)
    new_columns_administrative_costs= df_tmp['tmp'].drop_duplicates().tolist()
    df_tmp.drop(columns='tmp', inplace=True)

    columns_administrative_costs = [col for col in df_dir_1 if 'administrative_costs' in col]

    for column in new_columns_administrative_costs:
        department_id = column.split('_')[0]
        basic_salary = next((item for item in new_columns_basic_salary if item.split('_')[0] == department_id), None)
        administrative_costs = next((item for item in columns_administrative_costs if item.split('&')[0] == department_id), None)
        df_dir_1[column] = (df_dir_1[basic_salary] * df_dir_1[administrative_costs]) / 100

    # Полная себестоимость

    df_tmp = df_calculations[df_calculations['cost_item_id'] == 10]
    df_tmp['tmp'] = df_tmp['department_id'].astype(str) + '_' + df_tmp['cost_item_id'].astype(str)
    new_columns_total_costs = df_tmp['tmp'].drop_duplicates().tolist()
    df_tmp.drop(columns='tmp', inplace=True)

    list_total = ([i for i in new_columns_dir if int(i.split('_')[1]) not in (12, 13)] +
                  new_columns_basic_salary + new_columns_additional_salary +
                  new_columns_accruals + new_columns_overhead_costs +
                  new_columns_administrative_costs)

    df_dir_1[new_columns_total_costs] = 0.0
    for column in new_columns_total_costs:
        department_id = column.split('_')[0]
        for i in list_total:
            if i.split('_')[0] == department_id:
                df_dir_1[column] += df_dir_1[i]

    # Расчетная прибыль

    list_total_value = [col for col in new_columns_dir if '_12' in col]
    df_dir_1['total_value'] = df_dir_1[[col for col in list_total_value]].sum(axis=1)

    list_subassemblies = [col for col in new_columns_dir if '_2' in col]
    df_dir_1['total_subassemblies'] = df_dir_1[[col for col in list_subassemblies]].sum(axis=1)

    df_dir_1['profit_calc'] = ((df_dir_1['total_subassemblies'] * 0.01) +
                               (df_dir_1['total_value'] - df_dir_1['total_subassemblies']) * profitability_input /
                               (profitability_input + 100))

    # Расчет постоянных затрат

    list_product_testing = [col for col in new_columns_dir if '_9' in col]
    df_dir_1['product_testing'] = df_dir_1[[col for col in list_product_testing]].sum(axis=1)
    list_materials = [col for col in new_columns_dir if col.endswith('_1')]
    df_dir_1['total_materials'] = df_dir_1[[col for col in list_materials]].sum(axis=1)
    df_dir_1['fixed_costs'] = (
                df_dir_1['total_materials'] + df_dir_1['total_subassemblies'] + df_dir_1['product_testing'])

    # Расчет базового и расчетного показателя добавленной стоимости. Отношение расчетного показателя к базовому выводит
    # коэффициент пересчета для основной зарплаты

    df_dir_1['basic_salary'] = df_dir_1[[col for col in new_columns_basic_salary]].sum(axis=1)
    df_dir_1['additional_salary'] = df_dir_1[[col for col in new_columns_additional_salary]].sum(axis=1)
    df_dir_1['accruals'] = df_dir_1[[col for col in new_columns_accruals]].sum(axis=1)
    df_dir_1['overhead_costs'] = df_dir_1[[col for col in new_columns_overhead_costs]].sum(axis=1)
    df_dir_1['administrative_costs'] = df_dir_1[[col for col in new_columns_administrative_costs]].sum(axis=1)

    list_business_expenses = [col for col in new_columns_dir if '_8' in col]
    df_dir_1['business_expenses'] = df_dir_1[[col for col in list_business_expenses]].sum(axis=1)

    df_dir_1['basic_added_value'] = (df_dir_1['basic_salary'] + df_dir_1['additional_salary'] + df_dir_1['accruals'] +
                                     df_dir_1['overhead_costs'] + df_dir_1['administrative_costs'])

    df_dir_1['estimated_added_value'] = (
                df_dir_1['total_value'] - df_dir_1['profit_calc'] - df_dir_1['business_expenses'] -
                df_dir_1['fixed_costs'])

    df_dir_1['K_recalculation'] = np.where(df_dir_1['basic_added_value'] == 0, 1,
                                           df_dir_1['estimated_added_value'] / df_dir_1['basic_added_value'])

    # Далее производится пересчет под полученный коэффициент
    # Пересчет основной зарплаты

    new_columns_basic_salary_dir = [i + '_dir' for i in new_columns_basic_salary]
    for column in new_columns_basic_salary_dir:
        department_id = column.split('_')[0]
        basic_salary = next((item for item in new_columns_basic_salary if item.split('_')[0] == department_id), None)
        df_dir_1[column] = df_dir_1[basic_salary] * df_dir_1['K_recalculation']

    # Дополнительная зарплата считается процентом от основной

    new_columns_additional_salary_dir = [i + '_dir' for i in new_columns_additional_salary]
    for column in new_columns_additional_salary_dir:
        department_id = column.split('_')[0]
        basic_salary_dir = next((item for item in new_columns_basic_salary_dir if item.split('_')[0] == department_id), None)
        df_dir_1[column] = df_dir_1[basic_salary_dir] * 0.0961

    # Пересчет начислений во внебюджетные фонды

    new_columns_accruals_dir = [i + '_dir' for i in new_columns_accruals]
    for column in new_columns_accruals_dir:
        department_id = column.split('_')[0]
        basic_salary_dir = next((item for item in new_columns_basic_salary_dir if item.split('_')[0] == department_id), None)
        additional_salary_dir = next((item for item in new_columns_additional_salary_dir if item.split('_')[0] == department_id), None)
        df_dir_1[column] = (df_dir_1[basic_salary_dir] + df_dir_1[additional_salary_dir]) * 0.0835

    # Пересчет общепроизводственных затрат

    new_columns_overhead_costs_dir = [i + '_dir' for i in new_columns_overhead_costs]

    for column in new_columns_overhead_costs_dir:
        department_id = column.split('_')[0]
        basic_salary_dir = next((item for item in new_columns_basic_salary_dir if item.split('_')[0] == department_id), None)
        overhead_costs = next((item for item in columns_overhead_costs if item.split('&')[0] == department_id), None)
        df_dir_1[column] = (df_dir_1[basic_salary_dir] * df_dir_1[overhead_costs]) / 100

    # Пересчет административно-управленческих затрат

    new_columns_administrative_costs_dir = [i + '_dir' for i in new_columns_administrative_costs]

    for column in new_columns_administrative_costs_dir:
        department_id = column.split('_')[0]
        basic_salary_dir = next((item for item in new_columns_basic_salary_dir if item.split('_')[0] == department_id), None)
        administrative_costs = next((item for item in columns_administrative_costs if item.split('&')[0] == department_id), None)
        df_dir_1[column] = (df_dir_1[basic_salary_dir] * df_dir_1[administrative_costs]) / 100

    # Вывод полной себестоимости

    new_columns_total_costs_dir = [i + '_dir' for i in new_columns_total_costs]
    list_total_dir = [i + '_dir' for i in list_total]

    df_dir_1.rename(columns={col: col + '_dir' for col in new_columns_dir}, inplace=True)

    df_dir_1[new_columns_total_costs_dir] = 0.0
    for column in new_columns_total_costs_dir:
        department_id = column.split('_')[0]
        for i in list_total_dir:
            if i.split('_')[0] == department_id:
                df_dir_1[column] += df_dir_1[i]

    # Прибыль рассчитывается обратным счетом

    df_dir_1['total_costs'] = df_dir_1[[col for col in new_columns_total_costs_dir]].sum(axis=1)
    df_dir_1['profit_dir'] = df_dir_1['total_value'] - df_dir_1['total_costs']

    # Итоговые колонки для дальнейшей работы

    columns_list = [col for col in df_dir_1.select_dtypes(include='number').columns if 'dir' in col]

    # Транспонирование таблицы

    df_dir_2 = pd.melt(df_dir_1,
                       id_vars=['department', 'contract', 'contract_attribute', 'customer',
                                'product_name', 'full_tech_group', 'price_id', 'product_order',
                                'plan_actual', 'january_actual', 'february_actual', 'march_actual',
                                'april_actual', 'may_actual', 'june_actual', 'july_actual', 'august_actual',
                                'september_actual', 'october_actual', 'november_actual', 'december_actual',
                                'non_completion'],
                       value_vars=columns_list,
                       var_name='columns',
                       value_name='amount_per_unit')
    df_dir_2 = df_dir_2[df_dir_2['amount_per_unit'] != 0]

    # Добавление нужной аналитики

    df_dir_2['plan'] = df_dir_2['plan_actual'] * df_dir_2['amount_per_unit']
    df_dir_2['january_plan'] = df_dir_2['january_actual'] * df_dir_2['amount_per_unit']
    df_dir_2['february_plan'] = df_dir_2['february_actual'] * df_dir_2['amount_per_unit']
    df_dir_2['march_plan'] = df_dir_2['march_actual'] * df_dir_2['amount_per_unit']
    df_dir_2['april_plan'] = df_dir_2['april_actual'] * df_dir_2['amount_per_unit']
    df_dir_2['may_plan'] = df_dir_2['may_actual'] * df_dir_2['amount_per_unit']
    df_dir_2['june_plan'] = df_dir_2['june_actual'] * df_dir_2['amount_per_unit']
    df_dir_2['july_plan'] = df_dir_2['july_actual'] * df_dir_2['amount_per_unit']
    df_dir_2['august_plan'] = df_dir_2['august_actual'] * df_dir_2['amount_per_unit']
    df_dir_2['september_plan'] = df_dir_2['september_actual'] * df_dir_2['amount_per_unit']
    df_dir_2['october_plan'] = df_dir_2['october_actual'] * df_dir_2['amount_per_unit']
    df_dir_2['november_plan'] = df_dir_2['november_actual'] * df_dir_2['amount_per_unit']
    df_dir_2['december_plan'] = (df_dir_2['december_actual'] * df_dir_2['amount_per_unit']
                                 + df_dir_2['non_completion'] * df_dir_2['amount_per_unit'])

    df_dir_2 = pd.merge(df_dir_2,
                        df_closing_dates[['product_order', 'order_status', 'order_plan_closing_date',
                                          'order_fact_closing_date']], on='product_order', how='inner')

    # Расшифровка статей затрат и подразделений-исполнителей из справочников

    df_dir_2['cost_item_id'] = np.where(df_dir_2['columns'].apply(lambda x: x.split('_')[1]) == 'dir',
                                        11,
                                        df_dir_2['columns'].apply(lambda x: x.split('_')[1]))

    df_dir_2 = pd.merge(df_dir_2, df_departments[['department_id', 'department']], on='department', how='inner')
    df_dir_2.rename(columns={'department_id': 'department_id_tmp'}, inplace=True)

    df_dir_2['department_id'] = df_dir_2['columns'].apply(lambda x: x.split('_')[0])
    df_dir_2['department_id'] = df_dir_2['department_id_tmp'].where(df_dir_2['department_id'] == 'profit',
                                                                    df_dir_2['department_id'])

    df_dir_2.rename(columns={'department': 'main_performer'}, inplace=True)

    df_dir_2['cost_item_id'] = df_dir_2['cost_item_id'].astype(int)
    df_dir_2['department_id'] = df_dir_2['department_id'].astype(int)

    df_dir_2 = pd.merge(df_dir_2, df_costs_items[['cost_item_id', 'cost_item']], on='cost_item_id', how='inner')
    df_dir_2 = pd.merge(df_dir_2, df_departments[['department_id', 'department']], on='department_id', how='inner')

    # Удаление ненужных признаков

    columns_to_drop = ['plan_actual', 'january_actual', 'february_actual', 'march_actual',
                       'april_actual', 'may_actual', 'june_actual', 'july_actual',
                       'august_actual', 'september_actual', 'october_actual',
                       'november_actual', 'december_actual', 'non_completion',
                       'amount_per_unit', 'columns', 'price_id', 'department_id', 'cost_item_id',
                       'product_name', 'department_id_tmp']
    df_dir_2.drop(columns=columns_to_drop, inplace=True)

    # Преобразуем данные и формируем итоговый сценарий

    positions = ['main_performer', 'contract', 'contract_attribute', 'customer', 'full_tech_group',
                 'product_order', 'order_status', 'order_plan_closing_date', 'order_fact_closing_date',
                 'cost_item', 'department', 'plan', 'january_plan', 'february_plan', 'march_plan', 'april_plan',
                 'may_plan', 'june_plan', 'july_plan', 'august_plan', 'september_plan', 'october_plan',
                 'november_plan', 'december_plan']

    df_dir_fin = pd.pivot_table(df_dir_2,
                                values=['plan', 'january_plan', 'february_plan', 'march_plan',
                                        'april_plan', 'may_plan', 'june_plan', 'july_plan', 'august_plan',
                                        'september_plan', 'october_plan', 'november_plan', 'december_plan'],
                                index=['main_performer', 'contract', 'contract_attribute',
                                       'customer', 'full_tech_group', 'product_order', 'order_status',
                                       'order_plan_closing_date', 'order_fact_closing_date', 'cost_item', 'department'],
                                aggfunc='sum').reset_index()
    df_dir_fin = df_dir_fin[positions]

    new_columns = ['fact', 'january_fact', 'february_fact', 'march_fact', 'april_fact', 'may_fact',
                   'june_fact', 'july_fact', 'august_fact', 'september_fact', 'october_fact',
                   'november_fact', 'december_fact', 'wip']

    for column in new_columns:
        df_dir_fin[column] = 0.0

    df_tmp = df_dir_fin[['main_performer', 'contract', 'contract_attribute',
                          'customer', 'full_tech_group', 'product_order', 'order_status',
                          'order_plan_closing_date', 'order_fact_closing_date']].drop_duplicates()
    df_fact = pd.merge(df_fact, df_tmp, on='product_order', how='left')
    df_wip = pd.merge(df_wip, df_tmp, on='product_order', how='left')

    df_dir_fin = pd.concat([df_dir_fin, df_fact, df_wip])

    positions = ['main_performer', 'contract', 'contract_attribute', 'customer',
                 'full_tech_group', 'product_order', 'order_status', 'order_plan_closing_date',
                 'order_fact_closing_date', 'cost_item', 'department', 'plan', 'january_plan',
                 'february_plan', 'march_plan', 'april_plan', 'may_plan', 'june_plan', 'july_plan',
                 'august_plan', 'september_plan', 'october_plan', 'november_plan', 'december_plan',
                 'fact', 'january_fact', 'february_fact', 'march_fact', 'april_fact', 'may_fact',
                 'june_fact', 'july_fact', 'august_fact', 'september_fact', 'october_fact',
                 'november_fact', 'december_fact', 'wip']

    df_dir_fin = pd.pivot_table(df_dir_fin,
                                values=['plan', 'january_plan', 'february_plan', 'march_plan', 'april_plan', 'may_plan',
                                        'june_plan', 'july_plan', 'august_plan', 'september_plan', 'october_plan',
                                        'november_plan', 'december_plan', 'fact', 'january_fact', 'february_fact',
                                        'march_fact', 'april_fact', 'may_fact', 'june_fact', 'july_fact', 'august_fact',
                                        'september_fact', 'october_fact', 'november_fact', 'december_fact', 'wip'],
                                index=['main_performer', 'contract', 'contract_attribute', 'customer',
                                       'full_tech_group', 'product_order', 'order_status', 'order_plan_closing_date',
                                       'order_fact_closing_date', 'cost_item', 'department'],
                                aggfunc='sum').reset_index()

    df_dir_fin = df_dir_fin[positions]

    df_dir_fin['completed_month'] = completed_month
    df_dir_fin['scenario'] = 'Нормативный расчет'

    return df_dir_fin
    # На выходе второй расчетный сценарий - БДР, посчитанный по нормативам, фактом и незавершенным
    # производством со всей необходимой детализацией
