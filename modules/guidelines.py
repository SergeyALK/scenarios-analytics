import warnings
warnings.filterwarnings('ignore')

def get_guidelines():

    from modules.control import make_control

    df_report, df_dir_fin = make_control()

    # По итогам расчетов требуется предоставить ориентиры для подразделений - информацию, сколько затрат они должны
    # списать на открытые заказы, чтобы скомпенсировать возникший перекос по закрытым заказам. В моем случае, контроль
    # финансового результата производился по изделиям, а не по кодам заказов. Алгоритм следующий - выводим отклонение
    # по изделиям тех закрытых заказов, которые не вписались в допустимый порог рентабельности и распределяем их на те
    # же изделия по действующим заказам, пропорционально плановому расчету. В таком случае, будет выдержана логика
    # распределения затрат по подразделениям согласно их плановым нормативам, а общая годовая рентабельность также
    # будет стремиться к плановому показателю

    # Данные по закрытым заказам с рентабельностью вне допустимых пределов уже подготовлена на этапе "control"

    df_report = df_report[df_report['cost_item'] != 'Трудоемкость']

    # Расчет отклонения план-факт. Этот показатель необходимо распределить по действующим заказам
    df_report['deviation_closed'] = df_report['accumulative_plan'] - df_report['accumulative_fact']
    df_deviation_closed = df_report.groupby('full_tech_group')[['deviation_closed']].sum().reset_index()

    # По действующим заказам также выводится отклонение, но с учетом незавершенного производства. Для расчета общей
    # потребности также необходимо учесть будущие затраты
    df_deviation_opened = df_dir_fin[df_dir_fin['order_status'] == 'Не закрыт']
    df_deviation_opened['deviation_1'] = (df_deviation_opened['accumulative_plan'] -
                                          df_deviation_opened['accumulative_fact'] -
                                          df_deviation_opened['wip'])

    df_deviation_opened['deviation_2'] = df_deviation_opened['plan'] - df_deviation_opened['accumulative_plan']
    df_deviation_opened['deviation_3'] = df_deviation_opened['deviation_1'] + df_deviation_opened['deviation_2']

    columns = ['main_performer', 'contract', 'contract_attribute', 'customer', 'full_tech_group',
               'product_order', 'order_plan_closing_date', 'cost_item', 'department', 'plan', 'deviation_3']
    df_deviation_opened = df_deviation_opened[
        ~ df_deviation_opened['cost_item'].isin(['Прибыль', 'Полная себестоимость',
                                                 'Трудоемкость', 'Оптовая цена'])]
    df_deviation_opened = df_deviation_opened[columns]

    # Далее отклонения по закрытым заказам, сгруппированные по изделию переносятся в единый датасет и распределяются
    # по заказам, статьям затрат и подразделениям пропорционально плану

    df_result = df_deviation_opened.merge(df_deviation_closed, on='full_tech_group', how='left')
    df_result['deviation_closed'] = df_result['deviation_closed'].fillna(0)

    plan_sum = df_result.groupby('full_tech_group')[['plan']].sum()
    plan_sum.rename(columns={'plan': 'total_plan'}, inplace=True)
    df_result = df_result.merge(plan_sum, on='full_tech_group', how='left')

    df_result['deviation_distributed'] = df_result['deviation_closed'] * (df_result['plan'] / df_result['total_plan'])
    df_result['deviation'] = df_result['deviation_3'] + df_result['deviation_distributed']
    df_result.drop(columns=['plan', 'deviation_3', 'deviation_closed', 'total_plan', 'deviation_distributed'],
                   inplace=True)

    return df_result
    # На выходе ориентиры для всех производственных подразделений - потребность в затратах до конца года. Однако,
    # важная ремарка - это лишь ориентиры. Непосредственная работа с подразделениями происходит в несколько итераций
    # с контролем промежуточных результатов

