import os

from etl.import_real_estate_data import import_real_estate_data
from etl.load_stage_dim_geography import load_stage_dim_geography
from etl.load_stage_dim_agency import load_stage_dim_agency
from etl.load_stage_fact_flat import load_stage_fact_flat
from etl.merge_dim_geography import merge_dim_geography
from etl.merge_dim_agency import merge_dim_agency
from etl.merge_fact_flat import merge_fact_flat

SRC_FILE_PATH = r'C:\repos\GitHub\real-estate-etl\data\immobilienscout24_berlin_20190113_15.json'
FILE_DATE_KEY = os.path.splitext(SRC_FILE_PATH)[0].split('_')[-2]
FILE_TIME_KEY = '{}:00:00'.format(os.path.splitext(SRC_FILE_PATH)[0].split('_')[-1])


def lambda_handler(event=None, context=None):
    datetime_args = {"file_date_key": FILE_DATE_KEY, "file_time_key": FILE_TIME_KEY}

    import_real_estate_data(src_filepath=SRC_FILE_PATH, **datetime_args)

    load_stage_dim_geography(**datetime_args)
    load_stage_dim_agency(**datetime_args)
    load_stage_fact_flat(**datetime_args)

    merge_dim_geography()
    merge_dim_agency()
    merge_fact_flat()


if __name__ == "__main__":
    lambda_handler()
