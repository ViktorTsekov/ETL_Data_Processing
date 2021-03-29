import mysql.connector

def getDatabaseConnection() -> mysql.connector:

    db = mysql.connector.connect(
        host="test-mysql-viktor.mysql.database.azure.com",
        user="Viktor@test-mysql-viktor",
        passwd="121233Vv",
        database="test_schema"
    )

    return db