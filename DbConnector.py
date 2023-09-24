import mysql.connector as mysql

class DbConnector:
    def __init__(self,
                 HOST='localhost',
                 DATABASE='strava_db',
                 USER='root',
                 PASSWORD='root'):
        self.connect_to_database(HOST, DATABASE, USER, PASSWORD)

    def connect_to_database(self, HOST, DATABASE, USER, PASSWORD):
        try:
            self.db_connection = mysql.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD, port=3306)
            self.cursor = self.db_connection.cursor()
            print(f"Connected to: {self.db_connection.get_server_info()}")
        except Exception as e:
            print(f"ERROR: Failed to connect to db: {e}")

    def execute_query(self, query):
        self.cursor.execute(query)
        self.db_connection.commit()

    def close_connection(self):
        self.cursor.close()
        self.db_connection.close()
        print(f"Connection to {self.db_connection.get_server_info()} is closed")
