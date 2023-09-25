from pyspark.sql import SparkSession
import logging.config
import sys
logging.config.fileConfig('properties/configuaration/logging_config.ini')
logger = logging.getLogger('Create_spark_session')

def get_spark_object(envn, appName):
    try:
            logger.info('get_spark_object started')
            if envn == "DEV":
                master = 'local'
            else:
                master = 'Yarn'
            logger.info('master is {}'.format(master))
            spark = SparkSession.builder.master(master).appName(appName).getOrCreate()

    except Exception as e:
        logger.error('An error occurred in get_spark_object', str(e))
        raise
    else:
            logger.info('spark object created')
    return spark