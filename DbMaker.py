import os
from datetime import datetime
import mysql.connector as mysql


class DbMaker:
    DATASET_PATH = "dataset/"

    def __init__(self, HOST="localhost", DATABASE="db", USER="root", PASSWORD="root"):
        self.connect_to_database(HOST, DATABASE, USER, PASSWORD)
        self.labels = self.read_labels()

    def connect_to_database(self, HOST, DATABASE, USER, PASSWORD):
        try:
            self.connection = mysql.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD, port=3306)
            self.cursor = self.connection.cursor()
            print(f"Established connection to database: {self.connection.get_server_info()}")
        except Exception as e:
            print(f"ERROR: Failed to connect to db: {e}")

    def read_labels(self):
        return open(self.DATASET_PATH + "labeled_ids.txt", "r").read().split("\n")

    def create_tables(self):
        self.create_user_table()
        self.create_activity_table()
        self.create_trackpoint_table()

    def create_user_table(self):
        self.execute_query("""
            CREATE TABLE IF NOT EXISTS user (
                id VARCHAR(255) PRIMARY KEY,
                has_labels BOOLEAN
            );
        """)

    def create_activity_table(self):
        self.execute_query("""
            CREATE TABLE IF NOT EXISTS activity (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id VARCHAR(255),
                transportation_mode VARCHAR(255),
                start_date_time DATETIME,
                end_date_time DATETIME,
                FOREIGN KEY (user_id) REFERENCES user(id)
            );
        """)

    def create_trackpoint_table(self):
        self.execute_query("""
            CREATE TABLE IF NOT EXISTS trackpoint (
                id INT AUTO_INCREMENT PRIMARY KEY,
                activity_id INT,
                lat DOUBLE,
                lon DOUBLE,
                altitude INT,
                date DATETIME,
                FOREIGN KEY (activity_id) REFERENCES activity(id)
            );
        """)

    def execute_query(self, query):
        self.cursor.execute(query)
        self.connection.commit()

    def insert_users(self):
        user_ids = self.get_user_ids()
        for user_id in user_ids:
            self.insert_single_user(user_id)

    def insert_single_user(self, user_id):
        query = "INSERT INTO user(id, has_labels) VALUES (%s, %s)"
        self.cursor.execute(query, (user_id, user_id in self.labels))
        self.connection.commit()

    def get_user_ids(self):
        return sorted(os.listdir(self.DATASET_PATH + "Data"))

    def insert_activity_table(self, user_id, transportation_mode, start_date_time, end_date_time):
        query = """INSERT INTO activity(user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s,%s,%s,%s)"""
        self.cursor.execute(query, (user_id, transportation_mode, start_date_time, end_date_time))
        self.connection.commit()
        return self.cursor.lastrowid

    def insert_trackpoints(self, trackpoints):
        query = """INSERT INTO trackpoint (activity_id, lat, lon, altitude, date) VALUES (%s,%s,%s,%s,%s)"""
        self.cursor.executemany(query, trackpoints)
        self.connection.commit()

    def filter_trackpoints(self, trackpoints, activity_id):
        filtered_trackpoints = []
        for trackpoint in trackpoints:
            formated_date = datetime.strptime(trackpoint[5] + trackpoint[6], "%Y-%m-%d%H:%M:%S")
            filtered_trackpoint = (activity_id,) + tuple(trackpoint[:2]) + tuple(trackpoint[3:4]) + (formated_date,)
            filtered_trackpoints.append(filtered_trackpoint)
        return filtered_trackpoints

    def find_activity_type(self, id, start_time, end_time):
        user_labels = open(self.DATASET_PATH + "Data/" + id + "/labels.txt").read().split("\n")
        for label in user_labels[1:-1]:
            attribute = label.split("\t")
            label_start_time = datetime.strptime(attribute[0], "%Y/%m/%d %H:%M:%S")
            label_end_time = datetime.strptime(attribute[1], "%Y/%m/%d %H:%M:%S")
            if start_time == label_start_time and end_time == label_end_time: return attribute[2]
        return "NULL"

    def filter_and_insert_activity(self):
        self.cursor.execute(""" SELECT * FROM user""")
        users_tuple = self.cursor.fetchall()
        for id, is_labeled in users_tuple:
            activity_filenames = os.listdir(self.DATASET_PATH + "Data/" + id + "/Trajectory")
            if len(activity_filenames) == 0: raise TypeError("No activity files found for user", id)
            user_data = []
            for activity_filename in activity_filenames:
                activity_data = read_plt(self.DATASET_PATH + "Data/" + id + "/Trajectory/" + activity_filename)
                if activity_data == None:
                    continue
                start_time_activity = datetime.strptime(activity_data[0][5] + " " + activity_data[0][6],
                                                        "%Y-%m-%d %H:%M:%S")
                end_time_activity = datetime.strptime(activity_data[-1][5] + " " + activity_data[-1][6],
                                                      "%Y-%m-%d %H:%M:%S")
                activity_type = "NULL"
                if id in self.labels:
                    activity_type = self.find_activity_type(id, start_time_activity, end_time_activity)
                activity_id = self.insert_activity_table(id, activity_type, start_time_activity, end_time_activity)
                filtered_trackpoints = self.filter_trackpoints(activity_data, activity_id)
                self.insert_trackpoints(filtered_trackpoints)

    def close_connection(self):
        self.connection.close()


def read_plt(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
    if len(lines) > 2500 - 6:
        return None
    data = []
    for line in lines:
        data.append(line.strip().split(','))
    return data[6:]


db = DbMaker()
if db.connection:
    db.create_tables()
    db.insert_users()
    db.filter_and_insert_activity()
    db.connection.close()
    print("DbMaker has finished executing.")
