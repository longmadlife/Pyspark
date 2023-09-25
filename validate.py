import logging
import logging.config

logging.config.fileConfig('properties/configuaration/logging_config.ini')
logger = logging.getLogger('validate')


def get_current_date(spark):
    try :
        logger.warning("started the get_current_date")
        output = spark.sql(''' select current_date''')
        logger.warning('validating spark object with current date-' + str(output.collect()))
    except Exception as e:
        logger.error('An error occurred in get_current_date', str(e))

        raise
    else:
        logger.warning('validation done...')