from processor.dataprocessor_service import DataProcessorService


"""
    Main-модуль, т.е. модуль запуска приложений ("точка входа" приложения)
"""


if __name__ == '__main__':
    service = DataProcessorService(datasource="atp_matches_2022.csv",
                                   db_connection_url="sqlite:///database.db")
    service.run_service()
