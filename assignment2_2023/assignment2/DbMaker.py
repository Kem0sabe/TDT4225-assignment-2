
import os
from datetime import datetime
import mysql.connector as mysql

class DbMaker():
    DATASET_PATH = "../../dataset/"

    def __init__(self,
                 HOST="localhost",
                 DATABASE="test_db",
                 USER="root",
                 PASSWORD="sveinung"):
        try:
            self.connection = mysql.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD, port=3306)
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)
        
        self.cursor = self.connection.cursor()
        self.labels = open(self.DATASET_PATH + "labeled_ids.txt", "r").read().split("\n")

    def create_user_table(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS User (
                id VARCHAR(255) PRIMARY KEY,
                has_labels BOOLEAN
            )
        """)
        self.connection.commit()

    def create_activity_table(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS Activity (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id VARCHAR(255),
                transportation_mode VARCHAR(255),
                start_date_time DATETIME,
                end_date_time DATETIME,
                FOREIGN KEY (user_id) REFERENCES User(id)
            )
        """)
        self.connection.commit()

    def create_trackpoint_table(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS TrackPoint (
                id INT AUTO_INCREMENT PRIMARY KEY,
                activity_id INT,
                lat DOUBLE,
                lon DOUBLE,
                altitude INT,
                date DATETIME,
                FOREIGN KEY (activity_id) REFERENCES Activity(id)
            )
        """)
        self.connection.commit()


    
    def insert_user_table(self):
        user_ids = os.listdir(self.DATASET_PATH + "Data")
        query = """INSERT INTO User(id,has_labels) VALUES (%s,%s)"""
        for user_id in user_ids:
            #self.cursor.execute(query , (user_id,user_id in self.labels))
            self.cursor.execute(query , (user_id,(user_id in self.labels)))
        self.connection.commit()

    def insert_activity_table(self,user_id,transportation_mode,start_date_time,end_date_time):
        query = """INSERT INTO Activity(user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s,%s,%s,%s)"""
        self.cursor.execute(query, (user_id,transportation_mode,start_date_time,end_date_time))
        self.connection.commit()
        return self.cursor.lastrowid

    def insert_trackpoints(self,trackpoints):
        query = """INSERT INTO Trackpoint (activity_id, lat, lon, altitude, date) VALUES (%s,%s,%s,%s,%s)"""
        self.cursor.executemany(query,trackpoints)

    def filter_trackpoints(self,trackpoints,activity_id):
        filtered_trackpoints = []
        for trackpoint in trackpoints:
            formated_date = datetime.strptime(trackpoint[5] + trackpoint[6], "%Y-%m-%d%H:%M:%S")
            filtered_trackpoint = (activity_id,) + tuple(trackpoint[:2]) + tuple(trackpoint[3:4]) + (formated_date,)
            filtered_trackpoints.append(filtered_trackpoint)
        return filtered_trackpoints

    def find_activity_type(self,id,start_time,end_time):
        user_labels = open(self.DATASET_PATH + "Data/" + id + "/labels.txt").read().split("\n")
        for label in user_labels[1:-1]:
            attribute = label.split("\t")
            print(attribute)
            label_start_time = datetime.strptime(attribute[0],"%Y/%m/%d %H:%M:%S")
            label_end_time = datetime.strptime(attribute[1],"%Y/%m/%d %H:%M:%S")
            if start_time == label_start_time and end_time == label_end_time: return attribute[2]
        return "NULL"

    def filter_and_insert_activity(self):
        self.cursor.execute(""" SELECT * FROM USER""")
        users_tuple = self.cursor.fetchall()
        for id, is_labeled in users_tuple:
            activity_filenames = os.listdir(self.DATASET_PATH + "Data/" + id + "/Trajectory")
            if len(activity_filenames) == 0: raise TypeError("No activity files found for user",id)
            user_data = []
            for activity_filename in activity_filenames:
                activity_data = read_plt(self.DATASET_PATH + "Data/" + id + "/Trajectory/" + activity_filename)
                if activity_data == None: continue
                start_time_activity = datetime.strptime(activity_data[0][5] + " " + activity_data[0][6],"%Y-%m-%d %H:%M:%S")
                end_time_activity =  datetime.strptime(activity_data[-1][5] + " " + activity_data[-1][6],"%Y-%m-%d %H:%M:%S")
                activity_type = "NULL"
                if id in self.labels:
                    activity_type = self.find_activity_type(id,start_time_activity,end_time_activity)
                activity_id = self.insert_activity_table(id,activity_type,start_time_activity,end_time_activity)
                filtered_trackpoints = self.filter_trackpoints(activity_data,activity_id)
                self.insert_trackpoints(filtered_trackpoints)
            
            
        
                
            
def read_plt(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
    if len(lines) > 2500 - 6: return None
    data = []
    for line in lines:
        data.append(line.strip().split(','))
    return data[6:]



    

#program = DbMaker()
#program.create_user_table()
#program.create_activity_table()
#program.create_trackpoint_table()
#program.insert_user_table()
#program.filter_and_insert_activity()
#program.connection.close()'''




