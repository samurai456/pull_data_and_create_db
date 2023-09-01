from sqlalchemy import (
    Column,
    Text,
    Integer,
    create_engine,
    ForeignKey,
)
from sqlalchemy.orm import (
    relationship, 
    sessionmaker,
    declarative_base,
)
import os
import requests

Base = declarative_base()

class Offer(Base):
    __tablename__ = 'offers'
    id = Column(Integer, primary_key = True)
    name = Column(Text)
    brand = Column(Text)
    category = Column(Text)
    merchant = Column(Text)
    attributes = relationship('Attribute', back_populates = 'offer', cascade = 'all, delete')
    image = relationship('Image', back_populates='offer', uselist=False, cascade='all, delete')

    def __repr__(self):
        return f'<Offer {self.name}>'

class Attribute(Base):
    __tablename__ = 'attributes'
    id = Column(Integer, primary_key = True)
    name = Column(Text)
    value = Column(Text)
    offer_id = Column(Integer, ForeignKey('offers.id'))
    offer = relationship('Offer', back_populates = 'attributes')

    def __repr__(self):
        return f'<Attribute {self.name}>'

class Image(Base):
    __tablename__ = 'images'
    id = Column(Integer, primary_key = True)
    width = Column(Integer)
    height = Column(Integer)
    url = Column(Text)
    offer_id = Column(Integer, ForeignKey('offers.id'))
    offer = relationship('Offer', back_populates = 'image')

    def __repr__(self):
        return f'<Image {self.url}>'

class DB:
    def __init__(self):
        self.engine = create_engine(f'sqlite:///{db_filename}')
        Base.metadata.create_all(self.engine)
        self.session = sessionmaker()(bind = self.engine)

    def populate(self, offers):
        for offer in offers:
            offer_obj = self.populate_offer(offer)
            self.populate_attributes(offer['attributes'], offer_obj)
            self.populate_image(offer['image'], offer_obj)

    def populate_offer(self, data):
        new_offer = Offer(
            id = data['id'],
            name = data['name'],
            brand = data['brand'],
            category = data['category'],
            merchant = data['merchant'],
        )
        self.session.add(new_offer)
        self.session.commit()
        return new_offer

    def populate_attributes(self, attributes, offer):
        for attr in attributes:
            new_attribute = Attribute(
                name = attr['name'],
                value = attr['value'],
                offer = offer,
            )
            self.session.add(new_attribute)
            self.session.commit()

    def populate_image(self, image, offer):
        offer_image = Image(
            width = image['width'],
            height = image['height'],
            url = image['url'],
            offer = offer,
        )
        self.session.add(offer_image)
        self.session.commit()

if __name__ == '__main__':
    db_filename = 'sqlite.db'
    if(os.path.exists(db_filename)):
        os.remove(db_filename)
    link = 'https://www.kattabozor.uz/hh/test/api/v1/offers'
    response = requests.get(link)
    offers = response.json()['offers']
    db = DB()
    db.populate(offers)