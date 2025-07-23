from sqlalchemy import Column, Integer, Float, String, Date
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class SpimexTradingResult(Base):
    __tablename__ = "trading_results"

    id = Column(Integer, primary_key=True, index=True)
    exchange_product_id = Column(String)
    exchange_product_name = Column(String)
    oil_id = Column(String)
    delivery_basis_id = Column(String)
    delivery_basis_name = Column(String)
    delivery_type_id = Column(String)
    volume = Column(Float)
    total = Column(Float)
    count = Column(Integer)
    date = Column(Date)
