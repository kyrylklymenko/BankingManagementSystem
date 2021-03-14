from redis import Redis
from sqlalchemy import create_engine
import decimal, datetime
# redis_creds = Redis()
# username = redis_creds.get("keystore:postgres:username").decode("utf-8")
# password = redis_creds.get("keystore:postgres:password").decode("utf-8")
#
# dbms_url = 'postgresql://' + username + ':' + password + '@localhost:5432/dbms_cp5'
#
# engine = create_engine(dbms_url)
#
# with engine.connect() as connection:
#     result = connection.execute("select * from cp5_test")
#     for row in result:
#         print(row.id, ' ', row.name)

num = decimal.Decimal("20000.20")
multiplier = decimal.Decimal(str((1+0.05)**2))
#print((num*multiplier).quantize(decimal.Decimal("1.00"), decimal.ROUND_FLOOR))
print(type(datetime.datetime.now().year))