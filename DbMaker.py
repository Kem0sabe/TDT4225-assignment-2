import os
from datetime import datetime
from DbConnector import DbConnector

class DbMaker():
    DATASET_PATH = "dataset/"

    def __init__(self):
        self.db_connector = DbConnector()
        self.connection = self.db_connector.db_connection
        self.cursor = self.db_connector.cursor
        self.labels = self.read_labels()

    def read_labels(self):
        return open(self.DATASET_PATH + "labeled_ids.txt", "r").read().split("\n")

    def create_tables(self):
        """
        This method creates all the tables needed for the project
        """
        self.create_user_table()
        self.create_activity_table()
        self.create_trackpoint_table()

    def create_user_table(self):
        """
        Creates the user table if it does not excist
        """
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS user (
                id VARCHAR(255) PRIMARY KEY,
                has_labels BOOLEAN
            )
        """)
        self.connection.commit()

    def create_activity_table(self):
        """
        Creates the activity table if it does not excist
        """
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS activity (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id VARCHAR(255),
                transportation_mode VARCHAR(255),
                start_date_time DATETIME,
                end_date_time DATETIME,
                FOREIGN KEY (user_id) REFERENCES user(id)
            )
        """)
        self.connection.commit()

    def create_trackpoint_table(self):
        """
        Creates the trackpoint table if it does not excist
        """
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS trackpoint (
                id INT AUTO_INCREMENT PRIMARY KEY,
                activity_id INT,
                lat DOUBLE,
                lon DOUBLE,
                altitude INT,
                date DATETIME,
                FOREIGN KEY (activity_id) REFERENCES activity(id)
            )
        """)
        self.connection.commit()

    def insert_user_table(self):
        """
        This method insert users into the user tables based on the
        listed user ids in the data directory
        """
        user_ids = os.listdir(self.DATASET_PATH + "Data")
        query = """INSERT INTO user(id,has_labels) VALUES (%s,%s)"""
        for user_id in user_ids:
            # self.cursor.execute(query , (user_id,user_id in self.labels))
            self.cursor.execute(query, (user_id, (user_id in self.labels)))
        self.connection.commit()

    def insert_activity_table(self, user_id, transportation_mode, start_date_time, end_date_time):
        """
        This method insert the given paramters to the activity table

        :param user_id: The user id for the person doing the activity
        :param transportation_mode: The type of transportation mode used for this activity
        :param start_date_time: Activity start time
        :param end_date_time: Activity end time

        :return: Returns a generated activity id that can be used in trackpoint reference
        """
        query = """INSERT INTO activity(user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s,%s,%s,%s)"""
        self.cursor.execute(query, (user_id, transportation_mode, start_date_time, end_date_time))
        self.connection.commit()
        return self.cursor.lastrowid

    def insert_trackpoints(self, filtered_trackpoints):
        """
        This method insert a bulck of filtered trackpoints to the trackpoint table

        :param filtered_trackpoints: List of trackpoints where we have removed unrelevant info and added activity id
        """
        query = """INSERT INTO trackpoint (activity_id, lat, lon, altitude, date) VALUES (%s,%s,%s,%s,%s)"""
        self.cursor.executemany(query, filtered_trackpoints)

    def filter_trackpoints(self, trackpoints, activity_id):
        """
        This method filteres a bunch of trackpoits to include only relevant info and an activity id

        :param trackpoints: trackpoints used to track each user by position, elevation and time
        :param activity_id: The id for the activity the trackpoint corresponds to

        :return: List of trackpoints where we have removed unrelevant info and added activity id
        """
        filtered_trackpoints = []
        for trackpoint in trackpoints:
            formated_date = datetime.strptime(trackpoint[5] + trackpoint[6], "%Y-%m-%d%H:%M:%S")
            filtered_trackpoint = (activity_id,) + tuple(trackpoint[:2]) + tuple(trackpoint[3:4]) + (formated_date,)
            filtered_trackpoints.append(filtered_trackpoint)
        return filtered_trackpoints

    def find_activity_type(self, id, start_time, end_time):
        """
        This method filteres a bunch of trackpoits to include only relevant info and an activity id

        :param trackpoints: trackpoints used to track each user by position, elevation and time
        :param activity_id: The id for the activity the trackpoint corresponds to

        :return: List of trackpoints where we have removed unrelevant info and added activity id
        """
        user_labels = open(self.DATASET_PATH + "Data/" + id + "/labels.txt").read().split("\n")
        for label in user_labels[1:-1]:
            attribute = label.split("\t")
            label_start_time = datetime.strptime(attribute[0], "%Y/%m/%d %H:%M:%S")
            label_end_time = datetime.strptime(attribute[1], "%Y/%m/%d %H:%M:%S")
            if start_time == label_start_time and end_time == label_end_time: return attribute[2]
        return None

    def filter_and_insert_activity(self):
        """
        This method filteres and insert activites and trackpoints to the table
        """
        self.cursor.execute(""" SELECT * FROM user""")
        users_tuple = self.cursor.fetchall()
        for id, is_labeled in users_tuple:
            activity_filenames = os.listdir(self.DATASET_PATH + "Data/" + id + "/Trajectory")
            if len(activity_filenames) == 0: raise TypeError("No activity files found for user", id)
            user_data = []
            for activity_filename in activity_filenames:
                activity_data = read_plt(self.DATASET_PATH + "Data/" + id + "/Trajectory/" + activity_filename)
                if activity_data == None: continue
                start_time_activity = datetime.strptime(activity_data[0][5] + " " + activity_data[0][6],
                                                        "%Y-%m-%d %H:%M:%S")
                end_time_activity = datetime.strptime(activity_data[-1][5] + " " + activity_data[-1][6],
                                                      "%Y-%m-%d %H:%M:%S")
                activity_type = None
                if id in self.labels:
                    activity_type = self.find_activity_type(id, start_time_activity, end_time_activity)
                activity_id = self.insert_activity_table(id, activity_type, start_time_activity, end_time_activity)
                filtered_trackpoints = self.filter_trackpoints(activity_data, activity_id)
                self.insert_trackpoints(filtered_trackpoints)


def read_plt(file_path):
    """
    Reads the .plt file and formats it as the spesification in the task

    :param file_path: the .plt file path
    """
    with open(file_path, 'r') as file:
        lines = file.readlines()
    if len(lines) > 2500 - 6: return None
    data = []
    for line in lines:
        data.append(line.strip().split(','))
    return data[6:]


program = DbMaker()
if program.connection:
    program.create_tables()
    program.insert_user_table()
    program.filter_and_insert_activity()
    program.connection.close()
    print("Done")


