import sqlalchemy
import json
from dotenv import load_dotenv
import models as m
from prettytable import PrettyTable
import os


login = os.getenv("LOGIN")
password = os.getenv("PASSWORD")
server = os.getenv("SERVER")
port = os.getenv("PORT")
db_name = os.getenv("DB_NAME")

with open("tests_data.json", "r") as json_data:
    data = json.load(json_data)

models = {
          "publisher" : m.Publisher,
          "book" : m.Book,
          "shop" : m.Shop,
          "stock" : m.Stock,
          "sale" : m.Sale
        }

DSN =f"postgresql://{login}:{password}@{server}:{port}/{db_name}"
engine = sqlalchemy.create_engine(DSN)
m.drop_tables(engine)
m.create_tables(engine)

Session = sqlalchemy.orm.sessionmaker(bind=engine)
session = Session()
for obj in data:
    session.add(models[obj.get("model")](id=obj.get("pk"), **obj.get("fields")))
session.commit()

def search_request(publisher_name="%", book_title="%", shop_name="%", sale_date="%"):
    result = (session.query(m.Book.title, m.Shop.name, m.Sale.price, m.Sale.date_sale).
    join(m.Publisher, m.Book.id_publisher == m.Publisher.id).
    join(m.Stock, m.Book.id == m.Stock.id_book).
    join(m.Shop, m.Shop.id == m.Stock.id_shop).
    join(m.Sale, m.Sale.id_stock == m.Stock.id).
    filter(m.Book.title.ilike(f'%{book_title}%'),
           m.Publisher.name.ilike(f'%{publisher_name}%'),
           m.Shop.name.ilike(f'%{shop_name}%'),
           sqlalchemy.cast(m.Sale.date_sale, sqlalchemy.String()).ilike(f'%{sale_date}%')))
    return result

table = PrettyTable()
table.field_names = ['книга', 'магазин', 'стоимость покупки', 'дата покупки']

for c in search_request(book_title="at"):
    table.add_row(c)
    table.add_divider()
print(table)
session.close()