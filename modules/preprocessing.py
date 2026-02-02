import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

from modules.load import import_data

df_main, _, _, _, _, _, _ = import_data()

def start_preprocessing():

    # Переменная "completed_month" - месяц, под который будет рассчитываться оперативный план. Плановые значения меньше
    # или равные "completed_month", учтены как закрытые периоды по приходу продукции на склад. Следовательно, план будет
    # заменен на фактический выпуск. Отрицательные отклонения будут распределены по оставшимся периодам уменьшаемым
    # остатком, положительные отклонения - выведены отдельной колонкой как невыполнение (non_completion). При расчете
    # модели, невыполнения будут включены в план декабря. Переменная "completed_month" может быть любой в диапазоне
    # (1-12). Я зафиксирую ее на значении 10, поскольку слепок базы данных составлялся на середину ноября

    completed_month = 10

    # Создаем новые признаки для дальнейшей формульной работы

    new_columns = ['plan_actual', 'january_actual', 'february_actual', 'march_actual', 'april_actual', 'may_actual',
                   'june_actual', 'july_actual', 'august_actual', 'september_actual', 'october_actual',
                   'november_actual',
                   'december_actual', 'non_completion', 'quantity', 'january_plan_prod', 'february_plan_prod',
                   'march_plan_prod', 'april_plan_prod', 'may_plan_prod', 'june_plan_prod', 'july_plan_prod',
                   'august_plan_prod', 'september_plan_prod', 'october_plan_prod', 'november_plan_prod',
                   'december_plan_prod', 'deviation', '1_month_dev', '2_month_dev', '3_month_dev', '4_month_dev',
                   '5_month_dev', '6_month_dev', '7_month_dev', '8_month_dev', '9_month_dev', '10_month_dev',
                   '11_month_dev', '12_month_dev', 'january_conv', 'february_conv', 'march_conv', 'april_conv',
                   'may_conv', 'june_conv', 'july_conv', 'august_conv', 'september_conv', 'october_conv',
                   'november_conv',
                   'december_conv']

    df_main[new_columns] = None

    # Отклонение в зависимости от значения completed_month

    months = ['january', 'february', 'march', 'april',
              'may', 'june', 'july', 'august', 'september',
              'october', 'november', 'december']

    sum_plans = 0
    sum_production = 0

    for i in range(completed_month):
        plan_col = f'{months[i]}_plan'
        prod_col = f'{months[i]}_production'
        sum_plans += df_main[plan_col]
        sum_production += df_main[prod_col]

    df_main['deviation'] = sum_plans - sum_production

    # Первый ряд промежуточных расчетов

    df_main['january_plan_prod'] = df_main['january_production'] if completed_month >= 1 else df_main['january_plan']
    df_main['february_plan_prod'] = df_main['february_production'] if completed_month >= 2 else df_main['february_plan']
    df_main['march_plan_prod'] = df_main['march_production'] if completed_month >= 3 else df_main['march_plan']
    df_main['april_plan_prod'] = df_main['april_production'] if completed_month >= 4 else df_main['april_plan']
    df_main['may_plan_prod'] = df_main['may_production'] if completed_month >= 5 else df_main['may_plan']
    df_main['june_plan_prod'] = df_main['june_production'] if completed_month >= 6 else df_main['june_plan']
    df_main['july_plan_prod'] = df_main['july_production'] if completed_month >= 7 else df_main['july_plan']
    df_main['august_plan_prod'] = df_main['august_production'] if completed_month >= 8 else df_main['august_plan']
    df_main['september_plan_prod'] = df_main['september_production'] if completed_month >= 9 else df_main[
        'september_plan']
    df_main['october_plan_prod'] = df_main['october_production'] if completed_month >= 10 else df_main['october_plan']
    df_main['november_plan_prod'] = df_main['november_production'] if completed_month >= 11 else df_main[
        'november_plan']
    df_main['december_plan_prod'] = df_main['december_production'] if completed_month >= 12 else df_main[
        'december_plan']

    df_main['quantity'] = (df_main['january_plan_prod'] + df_main['february_plan_prod'] + df_main['march_plan_prod'] +
                           df_main['april_plan_prod'] + df_main['may_plan_prod'] + df_main['june_plan_prod'] +
                           df_main['july_plan_prod'] + df_main['august_plan_prod'] + df_main['september_plan_prod'] +
                           df_main['november_plan_prod'] + df_main['december_plan_prod'])

    # Второй ряд промежуточных расчетов

    df_main['january_conv'] = 0 if completed_month >= 1 else df_main['january_plan_prod']
    df_main['february_conv'] = 0 if completed_month >= 2 else df_main['february_plan_prod']
    df_main['march_conv'] = 0 if completed_month >= 3 else df_main['march_plan_prod']
    df_main['april_conv'] = 0 if completed_month >= 4 else df_main['april_plan_prod']
    df_main['may_conv'] = 0 if completed_month >= 5 else df_main['may_plan_prod']
    df_main['june_conv'] = 0 if completed_month >= 6 else df_main['june_plan_prod']
    df_main['july_conv'] = 0 if completed_month >= 7 else df_main['july_plan_prod']
    df_main['august_conv'] = 0 if completed_month >= 8 else df_main['august_plan_prod']
    df_main['september_conv'] = 0 if completed_month >= 9 else df_main['september_plan_prod']
    df_main['october_conv'] = 0 if completed_month >= 10 else df_main['october_plan_prod']
    df_main['november_conv'] = 0 if completed_month >= 11 else df_main['november_plan_prod']
    df_main['december_conv'] = 0 if completed_month >= 12 else df_main['december_plan_prod']

    # Третий ряд промежуточных расчетов

    df_main['1_month_dev'] = (df_main['january_conv'] + df_main['deviation'])
    df_main['2_month_dev'] = (df_main['january_conv'] + df_main['february_conv'] + df_main['deviation'])
    df_main['3_month_dev'] = (
                df_main['january_conv'] + df_main['february_conv'] + df_main['march_conv'] + df_main['deviation'])
    df_main['4_month_dev'] = (df_main['january_conv'] + df_main['february_conv'] + df_main['march_conv'] +
                              df_main['april_conv'] + df_main['deviation'])
    df_main['5_month_dev'] = (df_main['january_conv'] + df_main['february_conv'] + df_main['march_conv'] +
                              df_main['april_conv'] + df_main['may_conv'] + df_main['deviation'])
    df_main['6_month_dev'] = (df_main['january_conv'] + df_main['february_conv'] + df_main['march_conv'] +
                              df_main['april_conv'] + df_main['may_conv'] + df_main['june_conv'] + df_main['deviation'])
    df_main['7_month_dev'] = (df_main['january_conv'] + df_main['february_conv'] + df_main['march_conv'] +
                              df_main['april_conv'] + df_main['may_conv'] + df_main['june_conv'] + df_main[
                                  'july_conv'] +
                              df_main['deviation'])
    df_main['8_month_dev'] = (df_main['january_conv'] + df_main['february_conv'] + df_main['march_conv'] +
                              df_main['april_conv'] + df_main['may_conv'] + df_main['june_conv'] + df_main[
                                  'july_conv'] +
                              df_main['august_conv'] + df_main['deviation'])
    df_main['9_month_dev'] = (df_main['january_conv'] + df_main['february_conv'] + df_main['march_conv'] +
                              df_main['april_conv'] + df_main['may_conv'] + df_main['june_conv'] + df_main[
                                  'july_conv'] +
                              df_main['august_conv'] + df_main['september_conv'] + df_main['deviation'])
    df_main['10_month_dev'] = (
                df_main['january_conv'] + df_main['february_conv'] + df_main['march_conv'] + df_main['april_conv'] +
                df_main['may_conv'] + df_main['june_conv'] + df_main['july_conv'] + df_main['august_conv'] +
                df_main['september_conv'] + df_main['october_conv'] + df_main['deviation'])
    df_main['11_month_dev'] = (
                df_main['january_conv'] + df_main['february_conv'] + df_main['march_conv'] + df_main['april_conv'] +
                df_main['may_conv'] + df_main['june_conv'] + df_main['july_conv'] + df_main['august_conv'] +
                df_main['september_conv'] + df_main['october_conv'] + df_main['november_conv'] + df_main['deviation'])
    df_main['12_month_dev'] = (
                df_main['january_conv'] + df_main['february_conv'] + df_main['march_conv'] + df_main['april_conv'] +
                df_main['may_conv'] + df_main['june_conv'] + df_main['july_conv'] + df_main['august_conv'] +
                df_main['september_conv'] + df_main['october_conv'] + df_main['november_conv'] +
                df_main['december_conv'] + df_main['deviation'])

    # Расчет оперативного плана - итоговый вариант для дальнейшей работы

    df_main['january_actual'] = np.where(df_main['deviation'] < 0,
                                         np.where(completed_month >= 1, df_main['january_plan_prod'],
                                                  np.where(df_main['1_month_dev'] < 0, 0, df_main['1_month_dev'])),
                                         df_main['january_plan_prod'])

    df_main['february_actual'] = np.where(df_main['deviation'] < 0,
                                          np.where(completed_month >= 2, df_main['february_plan_prod'],
                                                   np.where(df_main['1_month_dev'] >= 0, df_main['february_plan_prod'],
                                                            np.where(df_main['2_month_dev'] < 0, 0,
                                                                     df_main['2_month_dev']))),
                                          df_main['february_plan_prod'])

    df_main['march_actual'] = np.where(df_main['deviation'] < 0,
                                       np.where(completed_month >= 3, df_main['march_plan_prod'],
                                                np.where(df_main['2_month_dev'] >= 0, df_main['march_plan_prod'],
                                                         np.where(df_main['3_month_dev'] < 0, 0,
                                                                  df_main['3_month_dev']))),
                                       df_main['march_plan_prod'])

    df_main['april_actual'] = np.where(df_main['deviation'] < 0,
                                       np.where(completed_month >= 4, df_main['april_plan_prod'],
                                                np.where(df_main['3_month_dev'] >= 0, df_main['april_plan_prod'],
                                                         np.where(df_main['4_month_dev'] < 0, 0,
                                                                  df_main['4_month_dev']))),
                                       df_main['april_plan_prod'])

    df_main['may_actual'] = np.where(df_main['deviation'] < 0,
                                     np.where(completed_month >= 5, df_main['may_plan_prod'],
                                              np.where(df_main['4_month_dev'] >= 0, df_main['may_plan_prod'],
                                                       np.where(df_main['5_month_dev'] < 0, 0,
                                                                df_main['5_month_dev']))),
                                     df_main['may_plan_prod'])

    df_main['june_actual'] = np.where(df_main['deviation'] < 0,
                                      np.where(completed_month >= 6, df_main['june_plan_prod'],
                                               np.where(df_main['5_month_dev'] >= 0, df_main['june_plan_prod'],
                                                        np.where(df_main['6_month_dev'] < 0, 0,
                                                                 df_main['6_month_dev']))),
                                      df_main['june_plan_prod'])

    df_main['july_actual'] = np.where(df_main['deviation'] < 0,
                                      np.where(completed_month >= 7, df_main['july_plan_prod'],
                                               np.where(df_main['6_month_dev'] >= 0, df_main['july_plan_prod'],
                                                        np.where(df_main['7_month_dev'] < 0, 0,
                                                                 df_main['7_month_dev']))),
                                      df_main['july_plan_prod'])

    df_main['august_actual'] = np.where(df_main['deviation'] < 0,
                                        np.where(completed_month >= 8, df_main['august_plan_prod'],
                                                 np.where(df_main['7_month_dev'] >= 0, df_main['august_plan_prod'],
                                                          np.where(df_main['8_month_dev'] < 0, 0,
                                                                   df_main['8_month_dev']))),
                                        df_main['august_plan_prod'])

    df_main['september_actual'] = np.where(df_main['deviation'] < 0,
                                           np.where(completed_month >= 9, df_main['september_plan_prod'],
                                                    np.where(df_main['8_month_dev'] >= 0,
                                                             df_main['september_plan_prod'],
                                                             np.where(df_main['9_month_dev'] < 0, 0,
                                                                      df_main['9_month_dev']))),
                                           df_main['september_plan_prod'])

    df_main['october_actual'] = np.where(df_main['deviation'] < 0,
                                         np.where(completed_month >= 10, df_main['october_plan_prod'],
                                                  np.where(df_main['9_month_dev'] >= 0, df_main['october_plan_prod'],
                                                           np.where(df_main['10_month_dev'] < 0, 0,
                                                                    df_main['10_month_dev']))),
                                         df_main['october_plan_prod'])

    df_main['november_actual'] = np.where(df_main['deviation'] < 0,
                                          np.where(completed_month >= 11, df_main['november_plan_prod'],
                                                   np.where(df_main['10_month_dev'] >= 0, df_main['november_plan_prod'],
                                                            np.where(df_main['11_month_dev'] < 0, 0,
                                                                     df_main['11_month_dev']))),
                                          df_main['november_plan_prod'])

    df_main['december_actual'] = np.where(df_main['deviation'] < 0,
                                          np.where(completed_month >= 12, df_main['december_plan_prod'],
                                                   np.where(df_main['11_month_dev'] >= 0, df_main['december_plan_prod'],
                                                            np.where(df_main['12_month_dev'] < 0, 0,
                                                                     df_main['12_month_dev']))),
                                          df_main['december_plan_prod'])

    df_main['non_completion'] = np.where(df_main['deviation'] > 0, df_main['deviation'], 0)

    df_main['plan_actual'] = (df_main['january_actual'] + df_main['february_actual'] + df_main['march_actual'] +
                              df_main['april_actual'] + df_main['may_actual'] + df_main['june_actual'] +
                              df_main['july_actual'] + df_main['august_actual'] + df_main['september_actual'] +
                              df_main['october_actual'] + df_main['november_actual'] + df_main['december_actual'] +
                              df_main['non_completion'])

    # Необходимая аналитика - срок закрытия заказа (плановый и фактический), а также текущий статус. Если в рамках
    # заказа сумма прихода соответствует плановой, заказ считается закрытым в периоде последнего прихода
    # на склад

    positions = ['plan', 'january_plan', 'february_plan', 'march_plan', 'april_plan', 'may_plan',
                 'june_plan', 'july_plan', 'august_plan', 'september_plan', 'october_plan',
                 'november_plan', 'december_plan', 'production', 'january_production',
                 'february_production', 'march_production', 'april_production', 'may_production',
                 'june_production', 'july_production', 'august_production', 'september_production',
                 'october_production', 'november_production', 'december_production']

    df_closing_dates = pd.pivot_table(df_main,
                                      values=['plan', 'january_plan', 'february_plan', 'march_plan', 'april_plan',
                                              'may_plan',
                                              'june_plan', 'july_plan', 'august_plan', 'september_plan', 'october_plan',
                                              'november_plan', 'december_plan', 'production', 'january_production',
                                              'february_production', 'march_production', 'april_production',
                                              'may_production',
                                              'june_production', 'july_production', 'august_production',
                                              'september_production',
                                              'october_production', 'november_production', 'december_production'],
                                      index='product_order',
                                      aggfunc='sum')
    df_closing_dates = df_closing_dates[positions]
    df_closing_dates = df_closing_dates.reset_index()

    new_columns = ['production_month', 'january_production_month', 'february_production_month',
                   'march_production_month', 'april_production_month', 'may_production_month',
                   'june_production_month', 'july_production_month', 'august_production_month',
                   'september_production_month', 'october_production_month', 'november_production_month',
                   'december_production_month', 'order_status', 'order_plan_closing_date', 'order_fact_closing_date']

    df_closing_dates[new_columns] = None

    df_closing_dates['january_production_month'] = df_closing_dates['january_production'] if completed_month >= 1 else 0
    df_closing_dates['february_production_month'] = df_closing_dates[
        'february_production'] if completed_month >= 2 else 0
    df_closing_dates['march_production_month'] = df_closing_dates['march_production'] if completed_month >= 3 else 0
    df_closing_dates['april_production_month'] = df_closing_dates['april_production'] if completed_month >= 4 else 0
    df_closing_dates['may_production_month'] = df_closing_dates['may_production'] if completed_month >= 5 else 0
    df_closing_dates['june_production_month'] = df_closing_dates['june_production'] if completed_month >= 6 else 0
    df_closing_dates['july_production_month'] = df_closing_dates['july_production'] if completed_month >= 7 else 0
    df_closing_dates['august_production_month'] = df_closing_dates['august_production'] if completed_month >= 8 else 0
    df_closing_dates['september_production_month'] = df_closing_dates[
        'september_production'] if completed_month >= 9 else 0
    df_closing_dates['october_production_month'] = df_closing_dates[
        'october_production'] if completed_month >= 10 else 0
    df_closing_dates['november_production_month'] = df_closing_dates[
        'november_production'] if completed_month >= 11 else 0
    df_closing_dates['december_production_month'] = df_closing_dates[
        'december_production'] if completed_month >= 12 else 0

    df_closing_dates['production_month'] = (df_closing_dates['january_production_month'] +
                                            df_closing_dates['february_production_month'] +
                                            df_closing_dates['march_production_month'] +
                                            df_closing_dates['april_production_month'] +
                                            df_closing_dates['may_production_month'] +
                                            df_closing_dates['june_production_month'] +
                                            df_closing_dates['july_production_month'] +
                                            df_closing_dates['august_production_month'] +
                                            df_closing_dates['september_production_month'] +
                                            df_closing_dates['october_production_month'] +
                                            df_closing_dates['november_production_month'] +
                                            df_closing_dates['december_production_month'])

    df_closing_dates['order_status'] = np.where(df_closing_dates['plan'] == df_closing_dates['production_month'],
                                                'Закрыт',
                                                'Не закрыт')

    conditions_plan = [
        df_closing_dates['december_plan'] != 0,
        df_closing_dates['november_plan'] != 0,
        df_closing_dates['october_plan'] != 0,
        df_closing_dates['september_plan'] != 0,
        df_closing_dates['august_plan'] != 0,
        df_closing_dates['july_plan'] != 0,
        df_closing_dates['june_plan'] != 0,
        df_closing_dates['may_plan'] != 0,
        df_closing_dates['april_plan'] != 0,
        df_closing_dates['march_plan'] != 0,
        df_closing_dates['february_plan'] != 0,
        df_closing_dates['january_plan'] != 0]

    conditions_fact = [
        df_closing_dates['december_production_month'] != 0,
        df_closing_dates['november_production_month'] != 0,
        df_closing_dates['october_production_month'] != 0,
        df_closing_dates['september_production_month'] != 0,
        df_closing_dates['august_production_month'] != 0,
        df_closing_dates['july_production_month'] != 0,
        df_closing_dates['june_production_month'] != 0,
        df_closing_dates['may_production_month'] != 0,
        df_closing_dates['april_production_month'] != 0,
        df_closing_dates['march_production_month'] != 0,
        df_closing_dates['february_production_month'] != 0,
        df_closing_dates['january_production_month'] != 0]

    results = [
        'Декабрь',
        'Ноябрь',
        'Октябрь',
        'Сентябрь',
        'Август',
        'Июль',
        'Июнь',
        'Май',
        'Апрель',
        'Март',
        'Февраль',
        'Январь']

    df_closing_dates['order_plan_closing_date'] = np.select(conditions_plan, results, default=None)
    df_closing_dates['order_fact_closing_date'] = np.where(df_closing_dates['order_status'] == 'Не закрыт', 'Не закрыт',
                                                           np.select(conditions_fact, results, default=None))

    return df_main, df_closing_dates, completed_month
    # На выходе преобразованная контрактная база (df_main) с пересчитанным планом по штукам, справочник сроков
    # закрытия заказов (df_closing_dates) и переменная - месяц для расчета (completed_month)
