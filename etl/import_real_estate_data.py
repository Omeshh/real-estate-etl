import json

from models import RealEstate
from utils.db import provide_session


@provide_session
def import_real_estate_data(src_filepath, file_date_key, file_time_key, flush_every=1000, session=None):
    print('Deleting rows from table {0} for file date key {1} and time key {2}'.
          format(RealEstate.__tablename__, file_date_key, file_time_key))

    session.query(RealEstate).filter(RealEstate.file_date_key == file_date_key
                                     and RealEstate.file_time_key == file_time_key).delete(synchronize_session=False)
    session.commit()

    row_count = 0
    bad_row_count = 0
    with open(src_filepath) as f:
        for i, line in enumerate(f):
            json_data = json.loads(line)
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
