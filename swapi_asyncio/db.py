from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String

PG_DSN = 'postgresql+asyncpg://postgres:postgres@127.0.0.1:5431/swapi_people'
engine = create_async_engine(PG_DSN)
Base = declarative_base(bind=engine)
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class People(Base):
    __tablename__ = 'characters'
    id = Column(Integer, primary_key=True)
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(String)
    gender = Column(String)
    hair_color = Column(String)
    height = Column(String)
    homeworld = Column(String)
    mass = Column(String)
    name = Column(String)
    skin_color = Column(String)
    species = Column(String)
    starships = Column(String)
    vehicles = Column(String)
    