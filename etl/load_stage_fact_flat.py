from dateutil import parser

from models import RealEstate, StageFactFlat
from utils.db import provide_session
from utils.etl_utils import replace_unknown_values


@provide_session
def load_stage_fact_flat(file_date_key, file_time_key, commit_every=1000, session=None):
    print('Truncating table: {0}'.format(StageFactFlat.__table__.fullname))
    session.execute('TRUNCATE TABLE {0}'.format(StageFactFlat.__table__.fullname))
    session.commit()
    rows_total = 0

    while True:
        obj_list = []

        rows = session.query(
            RealEstate.data["realEstate_id"].astext.label("real_estate_id"),
            RealEstate.data["contactDetails_id"].astext.label("contact_details_id"),
            RealEstate.data["realEstate_address_geoHierarchy_city_name"].astext.label("city_name"),
            RealEstate.data["realEstate_address_geoHierarchy_country_name"].astext.label("country_name"),
            RealEstate.data["realEstate_address_postcode"].astext.label("postal_code"),
            RealEstate.data["realEstate_address_houseNumber"].astext.label("house_number"),
            RealEstate.data["realEstate_address_street"].astext.label("street"),
            RealEstate.data["realEstate_address_wgs84Coordinate_latitude"].astext.label("latitude"),
            RealEstate.data["realEstate_address_wgs84Coordinate_longitude"].astext.label("longitude"),
            RealEstate.data["realEstate_xsi_type"].astext.label("real_estate_type"),
            RealEstate.data["realEstate_apartmentType"].astext.label("apartment_type"),
            RealEstate.data["realEstate_livingSpace"].astext.label("flat_size"),
            RealEstate.data["realEstate_calculatedTotalRent"].astext.label("total_price"),
            RealEstate.data["realEstate_calculatedTotalRentScope"].astext.label("total_rent_scope"),
            RealEstate.data["realEstate_baseRent"].astext.label("base_rent"),
            RealEstate.data["realEstate_numberOfFloors"].astext.label("total_floors"),
            RealEstate.data["realEstate_numberOfRooms"].astext.label("total_rooms"),
            RealEstate.data["realEstate_numberOfBathRooms"].astext.label("total_bathrooms"),
            RealEstate.data["realEstate_numberOfBedRooms"].astext.label("total_bedrooms"),
            RealEstate.data["realEstate_numberOfParkingSpaces"].astext.label("total_parking_spaces"),
            RealEstate.data["realEstate_attachments"].astext.isnot(None).label("has_attachments"),
            RealEstate.data["realEstate_state"].astext.label("status"),
            RealEstate.data["realEstate_creationDate"].astext.label("created_date"),
            RealEstate.data["realEstate_lastModificationDate"].astext.label("modified_date")). \
            filter(file_date_key == file_date_key
                   and file_time_key == file_time_key).\
            order_by(RealEstate.id).\
            offset(rows_total).\
            limit(commit_every)

        if rows.count() == 0:
            break

        rows_total = rows_total + rows.count()

        for row in rows:
            stage_fact_flat = StageFactFlat(
                file_date_key=file_date_key,
                file_time_key=file_time_key,
                real_estate_id=row.real_estate_id,
                contact_details_id=row.contact_details_id,
                city_name=replace_unknown_values(row.city_name),
                country_code=replace_unknown_values(None),
                country_name=replace_unknown_values(row.country_name),
                postal_code=replace_unknown_values(row.postal_code),
                house_number=row.house_number,
                street=row.street,
                latitude=row.latitude,
                longitude=row.longitude,
                real_estate_type=row.real_estate_type,
                apartment_type=row.apartment_type,
                flat_size=row.flat_size,
                total_price=row.total_price,
                total_rent_scope=row.total_rent_scope,
                additional_costs=(float(row.total_price or 0) - float(row.base_rent or 0)),
                total_rooms=row.total_rooms,
                total_bathrooms=row.total_bathrooms,
                total_bedrooms=row.total_bedrooms,
                total_parking_spaces=row.total_parking_spaces,
                has_attachments=row.has_attachments,
                status=row.status,
                created_date_key=parser.parse(row.created_date).strftime('%Y%m%d'),
                created_time_key=parser.parse(row.created_date).strftime('%H:%M:%S'),
                modified_date_key=parser.parse(row.modified_date).strftime('%Y%m%d'),
                modified_time_key=parser.parse(row.modified_date).strftime('%H:%M:%S'))

            obj_list.append(stage_fact_flat)

        session.bulk_save_objects(obj_list)
        session.flush()

        print('Inserted {0} rows in table {1}'.format(rows.count(), StageFactFlat.__table__.fullname))

    session.commit()
    print('Total inserted {0} rows in table {1}'.format(rows_total, StageFactFlat.__table__.fullname))
