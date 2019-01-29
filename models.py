import settings

from sqlalchemy import (
    Table, Column, Integer, CHAR, Index)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class RealEstate(Base):
    __tablename__ = 'real_estate_immobilienscout24'
    __table_args__ = (Index('ix_file_date_key_time_key', 'file_date_key', 'file_time_key'),
                      {"schema": "src"})
    id = Column(Integer, primary_key=True)
    file_date_key = Column(Integer)
    file_time_key = Column(CHAR(8))
    data = Column(JSONB)

    def __repr__(self):
        return "<RealEstateStage(id='{0}', body='{0}')>".format(self.id, self.body)


class StageDimAgency(Base):
    __table__ = Table('stage_dim_agency', settings.metadata['etl'], autoload=True)


class StageDimGeography(Base):
    __table__ = Table('stage_dim_geography', settings.metadata['etl'], autoload=True)


class StageFactFlat(Base):
    __table__ = Table('stage_fact_flat', settings.metadata['etl'], autoload=True)


class DimAgency(Base):
    __table__ = Table('dim_agency', settings.metadata['dwh'], autoload=True)


class DimGeography(Base):
    __table__ = Table('dim_geography', settings.metadata['dwh'], autoload=True)


class FactFlat(Base):
    __table__ = Table('fact_flat', settings.metadata['dwh'], autoload=True)


Base.metadata.create_all(settings.engine)
