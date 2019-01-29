from models import RealEstate, StageDimGeography
from utils.db import provide_session
from utils.etl_utils import replace_unknown_values


@provide_session
def load_stage_dim_geography(file_date_key, file_time_key, commit_every=1000, session=None):
    print('Truncating table: {0}'.format(StageDimGeography.__table__.fullname))
    session.execute('TRUNCATE TABLE {0}'.format(StageDimGeography.__table__.fullname))
    session.commit()

    rows_total = 0

    while True:
        obj_list = []

        rows = session.query(
            RealEstate.data["contactDetails_address_city"].astext.label("agency_city_name"),
            RealEstate.data["contactDetails_countryCode"].astext.label("agency_country_code"),
            RealEstate.data["contactDetails_address_postcode"].astext.label("agency_postal_code"),
            RealEstate.data["realEstate_address_geoHierarchy_city_name"].astext.label("flat_city_name"),
            RealEstate.data["realEstate_address_geoHierarchy_country_name"].astext.label("flat_country_name"),
            RealEstate.data["realEstate_address_postcode"].astext.label("flat_postal_code")).\
            filter(file_date_key == file_date_key
                   and file_time_key == file_time_key).\
            order_by(RealEstate.id).\
            offset(rows_total).\
            limit(commit_every)

        if rows.count() == 0:
            break

        rows_total = rows_total + rows.count()

        for row in rows:
            # TODO: Update the Unknown values with lookups from the master data

            agency_geography = StageDimGeography(
                city_name=replace_unknown_values(row.agency_city_name),
                country_code=replace_unknown_values(row.agency_country_code),
                country_name=replace_unknown_values(None),
                postal_code=replace_unknown_values(row.agency_postal_code))

            flat_geography = StageDimGeography(
                city_name=replace_unknown_values(row.flat_city_name),
                country_code=replace_unknown_values(None),
                country_name=replace_unknown_values(row.flat_country_name),
                postal_code=replace_unknown_values(row.flat_postal_code))

            obj_list.append(agency_geography)
            obj_list.append(flat_geography)

        session.bulk_save_objects(obj_list)
        session.flush()

        print('Inserted total {0} rows in table {1}'.format(rows_total + rows_total,
                                                            StageDimGeography.__table__.fullname))

    session.commit()
    print('Finished importing data to table {}.'.format(StageDimGeography.__table__.fullname))
