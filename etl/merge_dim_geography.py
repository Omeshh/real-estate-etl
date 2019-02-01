from utils.db import provide_session
from models import DimGeography


@provide_session
def merge_dim_geography(session):
    print('Loading table: {0}'.format(DimGeography.__table__.fullname))
    merge_query = '''
        INSERT INTO dwh.dim_geography
        (
            city_name, country_code, country_name, postal_code
        )
        select  distinct city_name, country_code, country_name, postal_code
        from 	etl.stage_dim_geography
        ON CONFLICT DO NOTHING;'''

    result = session.execute(merge_query)
    session.commit()

    print('Total affected {} rows for merging data from stage table.'.format(result.rowcount))
    print('Finished merging data from stage table to {} table'.format(DimGeography.__table__.fullname))
