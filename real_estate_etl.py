import os

from etl.import_real_estate_data import import_real_estate_data
from etl.load_stage_dim_geography import load_stage_dim_geography
from etl.load_stage_dim_agency import load_stage_dim_agency
from etl.load_stage_fact_flat import load_stage_fact_flat
from etl.merge_dim_geography import merge_dim_geography
from etl.merge_dim_agency import merge_dim_agency
from etl.merge_fact_flat import merge_fact_flat


def main(event=None, context=None):
    """Main method for Real Estate ETL process
    :param event: event from AWS Lamda
    :type event: dict
    :param context: event from AWS Lamda
    :type context: dict
    """
    file_bucket = event["Records"][0]['s3']['bucket']['name']
    file_name = event["Records"][0]['s3']['object']['key']

    file_date_key = file_name.split('_')[-2]
    file_time_key = '{}:00:00'.format(os.path.splitext(file_name)[0].split('_')[-1])
    datetime_args = {"file_date_key": file_date_key, "file_time_key": file_time_key}

    import_real_estate_data(src_file_s3_bucket=file_bucket, src_filename=file_name, **datetime_args)

    load_stage_dim_geography(**datetime_args)
    load_stage_dim_agency(**datetime_args)
    load_stage_fact_flat(**datetime_args)

    merge_dim_geography()
    merge_dim_agency()
    merge_fact_flat()

    return 'SUCCESS'


if __name__ == "__main__":
    main()
