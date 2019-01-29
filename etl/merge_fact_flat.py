from utils.db import provide_session
from models import FactFlat


@provide_session
def merge_fact_flat(session):
    print('Loading table: {0}'.format(FactFlat.__table__.fullname))
    merge_query = '''
        INSERT INTO dwh.fact_flat
        (
            file_date_key 
            , file_time_key
            , real_estate_id
            , agency_key
            , geography_key
            , house_number
            , street
            , latitude
            , longitude
            , real_estate_type
            , apartment_type
            , flat_size
            , total_price
            , total_rent_scope
            , additional_costs
            , total_rooms
            , total_bathrooms
            , total_bedrooms
            , total_parking_spaces
            , has_attachments
            , status
            , created_date_key
            , created_time_key
            , modified_date_key
            , modified_time_key
        )
        SELECT  sff.file_date_key 
                , sff.file_time_key
                , sff.real_estate_id
                , COALESCE(da.agency_key, -1) AS agency_key
                , COALESCE(dg.geography_key, -1) AS geography_key
                , sff.house_number
                , sff.street
                , sff.latitude
                , sff.longitude
                , sff.real_estate_type
                , sff.apartment_type
                , sff.flat_size
                , sff.total_price
                , sff.total_rent_scope
                , sff.additional_costs
                , sff.total_rooms
                , sff.total_bathrooms
                , sff.total_bedrooms
                , sff.total_parking_spaces
                , sff.has_attachments
                , sff.status
                , sff.created_date_key
                , sff.created_time_key
                , sff.modified_date_key
                , sff.modified_time_key
        FROM    etl.stage_fact_flat sff
                LEFT JOIN dwh.dim_geography dg
                ON	sff.city_name = dg.city_name
                    AND sff.country_code = dg.country_code
                    and sff.country_name = dg.country_name
                    and sff.postal_code = dg.postal_code
                LEFT JOIN dwh.dim_agency da
                ON	sff.contact_details_id = da.contact_details_id
        ON CONFLICT (real_estate_id, file_date_key, file_time_key)
        DO 
            UPDATE 
            SET		agency_key = EXCLUDED.agency_key
                    , geography_key = EXCLUDED.geography_key
                    , house_number = EXCLUDED.house_number
                    , street = EXCLUDED.street
                    , latitude = EXCLUDED.latitude
                    , longitude = EXCLUDED.longitude
                    , real_estate_type = EXCLUDED.real_estate_type
                    , apartment_type = EXCLUDED.apartment_type
                    , flat_size = EXCLUDED.flat_size
                    , total_price = EXCLUDED.total_price
                    , total_rent_scope = EXCLUDED.total_rent_scope
                    , additional_costs = EXCLUDED.additional_costs
                    , total_rooms = EXCLUDED.total_rooms
                    , total_bathrooms = EXCLUDED.total_bathrooms
                    , total_bedrooms = EXCLUDED.total_bedrooms
                    , total_parking_spaces = EXCLUDED.total_parking_spaces
                    , has_attachments = EXCLUDED.has_attachments
                    , status = EXCLUDED.status
                    , created_date_key = EXCLUDED.created_date_key
                    , created_time_key = EXCLUDED.created_time_key
                    , modified_date_key = EXCLUDED.modified_date_key
                    , modified_time_key = EXCLUDED.modified_time_key;'''

    result = session.execute(merge_query)
    session.commit()

    print('Total affected {} rows for merging data from stage table.'.format(result.rowcount))
    print('Finished merging data from stage table {}'.format(FactFlat.__table__.fullname))
