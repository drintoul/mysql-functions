# mysql_functions
MySQL functions to connect to, query and update MySQL database

## Authentication
Credentials are stored in a JSON file called creds.json. An example file is provided.

# Functions
* connect_db = connect to MySQL database
* query_db = query using connection
* df_to_table = use Pandas .to_sql method over connection
