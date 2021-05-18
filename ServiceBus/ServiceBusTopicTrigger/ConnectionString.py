import psycopg2

def getDatabaseConnection():
    dbname ='kymira_database' 
    user ='DataScienceAdmin@kymira-research-database' 
    host ='kymira-research-database.postgres.database.azure.com' 
    password ='121233Vv' 
    sslmode ='require'

    conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
    conn = psycopg2.connect(conn_string)

    return conn