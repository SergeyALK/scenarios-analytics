import pandas as pd
from modules.load import import_data
from modules.scenario_1 import start_scenario_1
from modules.scenario_2 import start_scenario_2
from modules.control import make_control
from modules.guidelines import get_guidelines

_, _, df_costs_items, df_departments, _, _, _ = import_data()
df_calc_fin = start_scenario_1()
df_dir_fin = start_scenario_2()
df_report, _ = make_control()
df_result = get_guidelines()

def export_dbase():

    df_full_fin = pd.concat([df_calc_fin, df_dir_fin])

    df_departments.to_csv('export/departments.csv', index=False)
    df_costs_items.to_csv('export/costs_items.csv', index=False)
    df_full_fin.to_csv('export/scenarios.csv', index=False)
    df_report.to_excel('results/report.xlsx', index=False)



    departments_list = list(df_result['department'].unique())

    for i in departments_list:
        df = df_result[df_result['department'] == i]
        df.to_excel(f"results/guidelines/{i}.xlsx", index=False)






