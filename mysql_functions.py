DATABASE_CREDS = 'creds.json'

def db_connect(database, credfile=DATABASE_CREDS):

    """ Connect to specified MySQL database using host, user, password stored in credfile """

    if not database:
        raise ValueError('Database must be provided')

    import json
    from sqlalchemy import create_engine

    try:

        with open(credfile, "r") as f:

            creds = json.load(f)
            db = database
            host = creds['host']
            user, passwd = creds['user'], creds['passwd']
        
        engine = create_engine(f"mysql+pymysql://{user}:{passwd}@{host}/{db}")
        return engine

    except Exception as e:

        print (e)
        return None


def db_query(connection, query):
    
    """ Query database over connection returned by connect_to_db """

    if not connection or not query:
        raise ValueError('Connection and query cannot be empty')

    import pandas as pd
    from pandas.io import sql

    try:

        return pd.read_sql(query, connection)

    except Exception as e:

        print (e)
        return None


def df_to_table(df, database, table, method):
    
    """ Save table to database and either overwrite or append """

    if not df:
        raise ValueError('DataFrame cannot be empty')
    if not database or not table:
        raise ValueError('Database and Table must be provided')
    if method not in ['overwrite', 'append']:
        raise ValueError('Method must be either overwrite or append')

    try:

        df.to_sql(table,
                  db_connect(database),
                  if_exists=method,
                  index=False)

    except Exception as e:
        
        print (e)
        return None
