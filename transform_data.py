import logging.config
from pyspark.sql.functions import *
from pyspark.sql.types import *

logging.config.fileConfig('properties/configuaration/logging_config.ini')
logger = logging.getLogger('transform_data')


def transform_data(df1, df2):
    try:
        logger.warning('transform_data starting')

        df_city_selection = df1.select(upper(df1.city).alias('city'), df1.state_id,
                                       upper(df1.state_name).alias('state_name'),
                                       upper(df1.county_name).alias('county_name'), df1.population, df1.zips)
        df_presc_selection = df2.select(df2.npi.alias('presc_npi'), df2.nppes_provider_last_org_name.alias('last_name'),
                                        df2.nppes_provider_first_name.alias('first_name'),
                                        df2.nppes_provider_city.alias('precs_city'),
                                        df2.nppes_provider_state.alias('precs_state'),
                                        df2.specialty_description.alias('presc_specialty'),
                                        df2.drug_name, df2.total_claim_count, df2.total_day_supply, df2.total_drug_cost,
                                        df2.years_of_exp)

    except Exception as e:
        logger.error('An error occurred in transform_data', str(e))
        raise

    else:
        logger.warning('transform data successfully')

    return df_city_selection, df_presc_selection
