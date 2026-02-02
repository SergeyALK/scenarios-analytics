from modules.load import import_data
from modules.preprocessing import start_preprocessing
from modules.export import export_dbase


def main():
    import_data()
    start_preprocessing()
    export_dbase()

if __name__ == '__main__':
    main()







