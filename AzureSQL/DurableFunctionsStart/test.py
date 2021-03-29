import mysql.connector

db = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="17082000",
    database="test_schema"
)

cursor = db.cursor()

cursor.execute("select * from test1")

result = cursor.fetchall()

for x in result:
    query = "insert into test2(id) values(%d)" % x[0]
    cursor.execute(query)

db.commit()