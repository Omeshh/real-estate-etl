from utils.db import provide_session
from models import DimAgency


@provide_session
def merge_dim_agency(session):
    print('Loading table: {0}'.format(DimAgency.__table__.fullname))
    merge_query = '''
        INSERT INTO dwh.dim_agency
        (
            contact_details_id
            , geography_key
            , customer_id
            , email
            , house_number
            , street
            , cell_phone_number
            , company
            , first_name
            , last_name
            , salutation
        )
        SELECT  sda.contact_details_id
                , COALESCE(geography_key, -1) AS geography_key
                , customer_id
                , email
                , house_number
                , street
                , cell_phone_number
                , company
                , first_name
                , last_name
                , salutation
        FROM    etl.stage_dim_agency sda
                LEFT JOIN  dwh.dim_geography dg
                ON  sda.city_name = dg.city_name
                    and sda.country_code = dg.country_code
                    and sda.country_name = dg.country_name
                    and sda.postal_code = dg.postal_code
        ON CONFLICT (contact_details_id)
        DO 
            UPDATE 
            SET		geography_key = EXCLUDED.geography_key
                    , email = EXCLUDED.email
                    , customer_id = EXCLUDED.customer_id
                    , house_number = EXCLUDED.house_number
                    , street = EXCLUDED.street
                    , cell_phone_number = EXCLUDED.cell_phone_number
                    , company = EXCLUDED.company
                    , first_name = EXCLUDED.first_name
                    , last_name = EXCLUDED.last_name
                    , salutation = EXCLUDED.salutation;'''

    result = session.execute(merge_query)
    session.commit()

    print('Total affected {} rows for merging data from stage table.'.format(result.rowcount))
    print('Finished merging data from stage table to {} table'.format(DimAgency.__table__.fullname))
