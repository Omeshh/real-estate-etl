from models import RealEstate, StageDimAgency
from utils.db import provide_session
from utils.etl_utils import replace_unknown_values


@provide_session
def load_stage_dim_agency(file_date_key, file_time_key, commit_every=1000, session=None):
    print('Truncating table: {0}'.format(StageDimAgency.__table__.fullname))
    session.execute('TRUNCATE TABLE {0}'.format(StageDimAgency.__table__.fullname))
    session.commit()
    rows_total = 0

    while True:
        obj_list = []

        rows = session.query(
            RealEstate.data["contactDetails_id"].astext.label("contact_details_id"),
            RealEstate.data["companyWideCustomerId"].astext.label("customer_id"),
            RealEstate.data["contactDetails_email"].astext.label("email"),
            RealEstate.data["contactDetails_address_city"].astext.label("city_name"),
            RealEstate.data["contactDetails_countryCode"].astext.label("country_code"),
            RealEstate.data["contactDetails_address_postcode"].astext.label("agency_postal_code"),
            RealEstate.data["contactDetails_address_houseNumber"].astext.label("house_number"),
            RealEstate.data["contactDetails_address_street"].astext.label("street"),
            RealEstate.data["contactDetails_cellPhoneNumber"].astext.label("cell_phone_number"),
            RealEstate.data["contactDetails_company"].astext.label("company"),
            RealEstate.data["contactDetails_firstname"].astext.label("first_name"),
            RealEstate.data["contactDetails_lastname"].astext.label("last_name"),
            RealEstate.data["contactDetails_salutation"].astext.label("salutation")). \
            filter(file_date_key == file_date_key
                   and file_time_key == file_time_key).\
            order_by(RealEstate.id).\
            offset(rows_total).\
            limit(commit_every)

        if rows.count() == 0:
            break

        rows_total = rows_total + rows.count()

        for row in rows:
            dim_agency = StageDimAgency(
                contact_details_id=row.contact_details_id,
                customer_id=row.customer_id,
                email=row.email,
                city_name=replace_unknown_values(row.city_name),
                country_code=replace_unknown_values(row.country_code),
                country_name=replace_unknown_values(None),
                postal_code=replace_unknown_values(row.agency_postal_code),
                house_number=row.house_number,
                street=row.street,
                cell_phone_number=row.cell_phone_number,
                company=row.company,
                first_name=row.first_name,
                last_name=row.last_name,
                salutation=row.salutation)

            obj_list.append(dim_agency)

        session.bulk_save_objects(obj_list)
        session.flush()

        print('Inserted {0} rows in table {1}'.format(rows.count(), StageDimAgency.__table__.fullname))

    session.commit()
    print('Total inserted {0} rows in table {1}'.format(rows_total, StageDimAgency.__table__.fullname))
    print('Finished importing data to table {}.'.format(StageDimAgency.__table__.fullname))
