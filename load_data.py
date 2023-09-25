import logging.config

logging.config.fileConfig('properties/configuaration/logging_config.ini')
logger = logging.getLogger('load_data')


def load_data(spark, file_dir, file_format, header, inferSchema):
    try:
        logger.warning('load_data starting ..... ')

        if file_format == 'parquet':
            df = spark.read.format(file_format).load(file_dir)

        elif file_format == 'csv':
            df = spark.read.format(file_format) \
                .options(header=header) \
                .options(inferSchema=inferSchema) \
                .load(file_dir)

    except Exception as e:
        logger.error('An error occurred in get_current_date', str(e))
        raise

    else:
        logger.warning('dataframe created successfully')
    return df


def display_df(df, dfName):
    df_show = df.show()
    return df_show
def df_count(df, dfName):
    try:
        logger.warning('count the records in the {}'.format(dfName))
        df_c = df.count()
    except Exception as e:
        raise
    else:
        logger.warning('Numbers of records in the {} are:: {}'.format(df, df_c))