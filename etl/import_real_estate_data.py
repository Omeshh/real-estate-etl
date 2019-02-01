import json

from models import RealEstate
from utils.db import provide_session
import boto3


@provide_session
def import_real_estate_data(src_file_s3_bucket, src_filename, file_date_key, file_time_key,
                            flush_every=1000, session=None):
    """Function to import the raw json file from S3 into database.
    :param src_file_s3_bucket: Source file S3 bucket name
    :type src_file_s3_bucket: str
    :param src_filename: Source file name
    :type src_filename: str
    :param file_date_key: File date key
    :param file_date_key: integer
    :param file_time_key: File time key in 'HH:MM:SS' format
    :type file_time_key: str
    :param flush_every: Flush data to table after number of rows
    :type flush_every: integer
    :param session: Session object for connecting to database
    :type session: SQL Alchemy session object
    """
    row_count = 0
    bad_row_count = 0

    print('Deleting rows from table {0} for file date key {1} and time key {2}'.
          format(RealEstate.__tablename__, file_date_key, file_time_key))

    session.query(RealEstate).filter(RealEstate.file_date_key == file_date_key
                                     and RealEstate.file_time_key == file_time_key).delete(synchronize_session=False)
    session.commit()

    print('Reading file {0} from S3 bucket {1}'.format(src_filename, src_file_s3_bucket))
    result = boto3.client("s3").get_object(Bucket=src_file_s3_bucket, Key=src_filename)

    for i, line in enumerate(result["Body"].iter_lines()):
        json_data = json.loads(line.decode('utf-8'))
        if json_data['ok']:
            stage_data = RealEstate(
                file_date_key=file_date_key,
                file_time_key=file_time_key,
                data=json_data['data'])

            session.add(stage_data)
            row_count = row_count + 1

            if row_count % flush_every == 0:
                session.flush()
                print('Inserted total {0} rows in table {1}'.format(row_count, RealEstate.__tablename__))
        else:
            bad_row_count = bad_row_count + 1

    session.commit()
    print('Inserted total {0} rows in table {1}'.format(row_count, RealEstate.__tablename__))
    print('Skipped importing bad rows count {}'.format(bad_row_count))
    print('Finished importing data to table {}'.format(RealEstate.__tablename__))
