# This is a sample Python script.
import os

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import get_evn_variables as gev
from create_spark_session import get_spark_object
from validate import get_current_date
from load_data import load_data, display_df, df_count
from transform_data import transform_data
import logging
import logging.config
import sys

logging.config.fileConfig('properties/configuaration/logging_config.ini')
logger = logging.getLogger('validate')


def main():
    try:
        logging.info('starting')
        spark = get_spark_object(gev.envn, gev.appName)

        logging.info('validating spark object')

        get_current_date(spark)

        for file in os.listdir(gev.src_olap):
            print('file is' + file)

            file_dir = gev.src_olap + '\\' + file
            #print(file_dir)
            if file.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

            elif file.endswith('.csv'):
                file_format = 'csv'
                header = gev.header
                inferSchema = gev.inferSchema

            logging.info('reading data file which is of > {}'.format(file_format))
            df_city = load_data(spark=spark,
                                file_dir=file_dir,
                                file_format=file_format,
                                header=header,
                                inferSchema=inferSchema)
            logging.info('displaying the data')
            display_df(df=df_city, dfName='df_city')
            df_count(df=df_city, dfName='df_city')
            # read fact table
            # fact
            for file2 in os.listdir(gev.src_oltp):
                print('file2 is' + file2)

                file_dir = gev.src_oltp + '\\' + file2
                # print(file_dir)
                if file2.endswith('.parquet'):
                    file_format = 'parquet'
                    header = 'NA'
                    inferSchema = 'NA'

                elif file2.endswith('.csv'):
                    file_format = 'csv'
                    header = gev.header
                    inferSchema = gev.inferSchema

            df_fact = load_data(spark=spark,
                                file_dir=file_dir,
                                file_format=file_format,
                                header=header,
                                inferSchema=inferSchema)
            logging.info('displaying the data')
            display_df(df=df_fact, dfName='df_fact')
            df_count(df=df_fact, dfName='df_fact')

            logging.info('processing data.....')
            df_city_sel, df_precs = transform_data(df_city, df_fact)

            logging.info('displaying the transformed data ')

            display_df(df=df_city_sel, dfName='df_city_sel')
            df_count(df=df_city_sel, dfName='df_city_sel')

            display_df(df=df_precs, dfName='df_precs')
            df_count(df=df_precs, dfName='df_precs')

    except Exception as e:
        logging.error("An error occurred in main() ", str(e))
        sys.exit(1)


if __name__ == '__main__':
    main()
    logging.info('Application done')
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
