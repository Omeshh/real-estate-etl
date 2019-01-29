from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import scoped_session, sessionmaker

SQL_CONN = "postgresql+psycopg2://airflow:airflow@localhost/real_estate"

REFLECT_METADATA_FOR = {"etl": ['stage_dim_agency', 'stage_dim_geography', 'stage_fact_flat'],
                        "dwh": ['dim_agency', 'dim_geography', 'fact_flat']}

engine = None
Session = None
metadata = {}


def configure_orm():
    global engine
    global Session

    engine_args = {"encoding": 'utf-8'}
    engine = create_engine(SQL_CONN, **engine_args, echo=False)

    Session = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine))


def configure_metadata():
    global metadata

    for schema, tables in REFLECT_METADATA_FOR.items():
        metadata[schema] = MetaData(schema=schema)
        metadata[schema].reflect(bind=engine, only=tables)


configure_orm()
configure_metadata()
