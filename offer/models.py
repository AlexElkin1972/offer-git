from peewee import *

db = Proxy()


class BaseModel(Model):
    class Meta:
        database = db


class Supplier(BaseModel):
    title = CharField(unique=True)


class Price(BaseModel):
    supplier = ForeignKeyField(Supplier, backref='prices')
    partnumber = CharField()
    description = CharField(null=True)
    description_ext = CharField(null=True)  # 'Russian Description'
    price = DecimalField(decimal_places=2)  # 'Price'
    date = DateField()  # 'Price Date'
    origin = CharField(null=True)  # 'Origin'
    weight = DecimalField(decimal_places=3, null=True)  # 'Weight'
    weight_volume = DecimalField(decimal_places=3, null=True)  # 'V.Weight'
    length = DecimalField(decimal_places=1, null=True)  # 'Length'
    width = DecimalField(decimal_places=1, null=True)  # 'Width'
    height = DecimalField(decimal_places=1, null=True)  # 'Height'
    reserved = CharField(null=True)  # 'Reserved column'

    class Meta:
        indexes = (
            # create a unique on supplier / partnumber
            (('supplier', 'partnumber'), True),
        )
