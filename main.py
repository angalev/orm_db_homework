import sqlalchemy
import json
from dotenv import load_dotenv
import models as m
import os

load_dotenv()
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

def search_request(search):
    request_body = session.query(m.Book.title, m.Shop.name, m.Sale.price, m.Sale.date_sale).\
    join(m.Publisher, m.Book.id_publisher == m.Publisher.id).\
    join(m.Stock, m.Book.id == m.Stock.id_book).\
    join(m.Shop, m.Shop.id == m.Stock.id_shop).\
    join(m.Sale, m.Sale.id_stock == m.Stock.id)
    if search.isdigit():
        result = request_body.filter(m.Publisher.id == search).all()
    else:
        result = request_body.filter(m.Publisher.name.ilike(f'%{search}%')).all()
    session.close()
    for publisher_name, shop_name, price, sale_date in result:
        print(f"{publisher_name: <40} | {shop_name: <10} | {price: <8} | {sale_date.strftime('%d-%m-%Y')}")

if __name__ == "__main__":

    search_request(str(input()))

