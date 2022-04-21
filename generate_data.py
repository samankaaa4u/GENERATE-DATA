from dataclasses import dataclass
import random
from psycopg import connect, sql
from datetime import date, datetime
from progress.bar import Bar
import faker
import hashlib

CONSTRING = "host=localhost dbname=online_db user=postgres password=postgres123"
SEED = 1337

def connection(func):
    def _connection(*args, **kwargs):

        with connect(CONSTRING) as con:
            with con.cursor() as cur:
                sql, params = func(*args, **kwargs)
                return cur.execute(sql, params)

    return _connection

def hash_password(password: str) -> str:
    return hashlib.sha512(password.encode()).hexdigest()

@connection
def add_user(user: dict):
    return ("""INSERT INTO users
(id,firstname,lastname,contactno,birthdate,
address_country,address_province,address_city,address_street_1,address_street_2,
email,username,password
)
VALUES
(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)

"""
            ,(user['id'],
              user['firstname'],
              user['lastname'],
              user['contact_no'],
              user['birthdate'],
              user['address_country'],
              user['address_province'],
              user['address_city'],
              user['address_street_1'],
              user['address_street_2'],
              user['email'],
              user['username'],
              hash_password(user['password']),
              )
            )
    
@connection
def add_vendor(vendor: dict):
    return ("""
INSERT INTO vendors
(id,name, description)
VALUES
(%s, %s, %s)
""",
            (vendor['id'],
             vendor['name'],
             vendor['description']
             )
    )


@connection
def add_products(product: dict):
    return ("""
INSERT INTO products
(id, name, short_desc, description, vendor_id,
price, sale, stocks)
VALUES
(%s,%s,%s,%s,%s,%s,%s,%s)
""", (product['id'],
      product['name'],
      product['short_desc'],
      product['description'],
      product['vendor_id'],
      product['price'],
      product['sale'],
      product['stocks']
      ))

@connection
def add_ratings(ratings: dict):
    return ("""
INSERT INTO ratings
(id, user_id,product_id, description, rating)
VALUES
(%s, %s,%s, %s,%s)
""", (ratings['id'],
      ratings['user_id'],
      ratings['product_id'],
      ratings['description'],
      ratings['rating']
      ))


if __name__ == '__main__':
    print ("GENERATING DATA....")

    random.seed(SEED)
    faker.Faker.seed(SEED)
    
    fake = faker.Faker()
    
    fake_range  = {
        'users': 100,
        'vendors': 30,
        'products': 500,
        'ratings': 500
    }
    
    fake_users = []
    fake_products = []
    fake_vendors = []
    fake_ratings = []

    # Generate random Users
    bar = Bar('Generating Users... ', max=fake_range['users'])
    for _ in range(0,fake_range['users']):
        fake_users.append({
            'id': fake.uuid4(),
            'firstname': fake.first_name(),
            'lastname': fake.last_name(),
            'contact_no': fake.msisdn(),
            'birthdate': fake.date(),
            'address_country': fake.country(),
            'address_province': 'Bukidnon',
            'address_city': fake.city(),
            'address_street_1': fake.street_name(),
            'address_street_2': fake.street_name(),
            'email': fake.email(),
            'username': fake.word(),
            'password': '123456',
            })
        bar.next()
    bar.finish()
        
    # Generate random Vendors
    bar = Bar('Generating Vendors... ', max=fake_range['vendors'])
    for _ in range(0,fake_range['vendors'] ):
        fake_vendors.append({
            'id': fake.uuid4(),
            'name': fake.company(),
            'description': fake.catch_phrase()
        })
        bar.next()
    bar.finish()
        
    # Generate random products
    bar = Bar('Generating Products... ', max=fake_range['products'])
    for _ in range(0,fake_range['products']):
        fake_products.append({
            'id': fake.uuid4(),
            'name': fake.words(),
            'short_desc': fake.sentence(),
            'description': fake.sentence(random.randrange(5,20)),
            'vendor_id': random.choice(fake_vendors)['id'],
            'price': random.randrange(1000,99999),
            'sale': random.randrange(0,40),
            'stocks': random.randrange(100,1000)
        })
        bar.next()
    bar.finish()
        
    # Generate random ratings
    bar = Bar('Generating Ratings... ', max=fake_range['ratings'])
    for _ in range(0, fake_range['ratings']):
        fake_ratings.append({
            'id': fake.uuid4(),
            'user_id': random.choice(fake_users)['id'],
            'product_id': random.choice(fake_products)['id'],
            'description': fake.sentence(),
            'rating': random.randrange(0,5)
        })
        bar.next()
    bar.finish()

    print('Adding to database...')

    # Run in db
    bar = Bar('Adding Users to Database... ', max=len(fake_users))
    for user in fake_users:
        add_user(user)
        bar.next()
    bar.finish()
    
    bar = Bar('Adding Vendors to Database... ', max=len(fake_vendors))
    for vendor in fake_vendors:
        add_vendor(vendor)
        bar.next()
    bar.finish()
    
    bar = Bar('Adding Products to Database... ', max=len(fake_products))
    for product in fake_products:
        add_products(product)
        bar.next()
    bar.finish()
        
    bar = Bar('Adding Ratings to Database... ', max=len(fake_ratings))
    for ratings in fake_ratings:
        add_ratings(ratings)
        bar.next()
    bar.finish()

    print('Completed!')
