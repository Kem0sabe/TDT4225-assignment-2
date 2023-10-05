from DbConnector import DbConnector
import haversine as hs
from tabulate import tabulate

class Queries:

    def __init__(self):
        self.db_connector = DbConnector()
        self.connection = self.db_connector.db_connection
        self.cursor = self.db_connector.cursor

    def task_1(self):
        print()
        print(
            "Task 1 - How many users, activities and trackpoints are there in the dataset (after it is inserted into the database):")
        self.cursor.execute("""
        SELECT 
            (SELECT count(id) FROM user) AS user_count,
            (SELECT count(id) FROM activity) AS activity_count,
            (SELECT count(id) FROM trackpoint) AS trackpoint_count;
        """)

        # Fetch all the results
        results = self.cursor.fetchall()

        print(tabulate([*results], headers=["user_count", "activity_count", "trackpoint_count"]))

    def task_2(self):
        print()
        print("Task 2 - Find the average, maximum and minimum number of trackpoints per user:")
        self.cursor.execute("""
            SELECT 
                AVG(trackpoints_count) AS average_trackpoints, 
                MIN(trackpoints_count) AS minimum_trackpoints,
                MAX(trackpoints_count) AS maximum_trackpoints 
            FROM (
                SELECT 
                    user.id, 
                    COUNT(trackpoint.id) AS trackpoints_count
                FROM user
                INNER JOIN activity ON user.id = activity.user_id
                INNER JOIN trackpoint ON activity.id = trackpoint.activity_id
                GROUP BY user.id
            ) AS trackpoints_per_user
        """)

        results = self.cursor.fetchall()

        print(tabulate([*results], headers=["average_trackpoints", "minimum_trackpoints", "maximum_trackpoints"]))

    def task_3(self):
        print()
        print("Task 3 - Find the top 15 users with the highest number of activities:")
        self.cursor.execute("""
            SELECT 
                user.id, 
                COUNT(DISTINCT activity.id) AS activity_count
            FROM user
            INNER JOIN activity ON user.id = activity.user_id
            GROUP BY user.id
            ORDER BY activity_count DESC
            LIMIT 15;
        """)

        results = self.cursor.fetchall()

        print(tabulate([*results], headers=["user_id", "activity_count"]))

    def task_4(self):
        print()
        print("Task 4 - Find all users who have taken a bus:")
        self.cursor.execute("""
            SELECT DISTINCT user.id
            FROM user
            INNER JOIN activity ON user.id = activity.user_id
            WHERE activity.transportation_mode = 'bus';
        """)

        results = self.cursor.fetchall()

        print(tabulate([*results], headers=["user_id"]))

    def task_5(self):
        print()
        print("Task 5 - List the top 10 users by their amount of different transportation modes:")
        self.cursor.execute("""
            SELECT 
                user.id, 
                COUNT(DISTINCT activity.transportation_mode) AS mode_count
            FROM user
            INNER JOIN activity ON user.id = activity.user_id
            WHERE activity.transportation_mode IS NOT NULL
            GROUP BY user.id
            ORDER BY mode_count DESC
            LIMIT 10;
        """)

        results = self.cursor.fetchall()

        print(tabulate([*results], headers=["user_id", "transportation_mode_count"]))

    def task_6(self):
        print()
        print(
            "Task 6 - Find activities that are registered multiple times.\nYou should find the query even if it gives zero result:")

        self.cursor.execute("""
            SELECT 
                user_id, 
                transportation_mode, 
                start_date_time, 
                end_date_time, 
                COUNT(*) AS occurrence_count
            FROM activity
            GROUP BY user_id, transportation_mode, start_date_time, end_date_time
            HAVING COUNT(*) > 1;
        """)

        results = self.cursor.fetchall()

        if len(results) == 0:
            print(tabulate([*results], headers=["user_id", "transportation_mode", "start_date_time", "end_date_date",
                                                "invalid_activites"]))
            print("No activities were found that were registered multiple times.")
        else:
            print(tabulate([*results], headers=["user_id", "transportation_mode", "start_date_time", "end_date_date",
                                                "invalid_activites"]))


    def task_7(self):
        self._task_7a()
        self._task_7b()

    def _task_7a(self):
        print()
        print(
            "Task 7a -  Find the number of users that have started an activity in one day and ended the activity the next day:")
        self.cursor.execute("""
            SELECT 
                COUNT(DISTINCT user_id) 
            FROM activity
            WHERE DATE(start_date_time) != DATE(end_date_time);
        """)

        results = self.cursor.fetchall()

        print(f"{results[0][0]} people started an activity and ended it on another day.")

    def _task_7b(self):
        print()
        print("Task 7b - List the transportation mode, user id and duration for these activities")
        self.cursor.execute("""
            SELECT 
                user_id, 
                transportation_mode, 
                TIMESTAMPDIFF(MINUTE, start_date_time, end_date_time) AS duration_minutes
            FROM activity
            WHERE DATE(start_date_time) != DATE(end_date_time) AND transportation_mode IS NOT NULL;
        """)

        results = self.cursor.fetchall()

        print(tabulate([*results], headers=["user_id", "transportation_mode", "actity_duration"]))

    def task_8(self):
        print()
        print("Task 8 - Find the number of users which have been close to each other in time and space.\n"
              "Close is defined as the same space (50 meters) and for the same half minute (30"
              "seconds)")
        self.cursor.execute("""
        SELECT user.id, date, lat,lon FROM trackpoint
        INNER JOIN activity
        ON activity.id = trackpoint.activity_id
        INNER JOIN user
        on user.id = activity.user_id
        ORDER BY date ASC

        """)

        sorted_trackpoints = self.cursor.fetchall()

        meetups = []
        for i in range(len(sorted_trackpoints)):
            user1 = sorted_trackpoints[i]
            user1_id = user1[0]
            user1_date = user1[1]
            user1_lat = user1[2]
            user1_lon = user1[3]
            for j in range(i + 1, len(sorted_trackpoints)):
                user2 = sorted_trackpoints[j]
                user2_id = user2[0]
                if user1_id == user2_id: continue
                user2_date = user2[1]
                seconds_difference = (user2_date - user1_date).total_seconds()
                if seconds_difference > 30: break
                user2_lat = user2[2]
                user2_lon = user2[3]
                distance = hs.haversine((user1_lat, user1_lon), (user2_lat, user2_lon))
                if distance > 0.05: continue
                meetups.append((user1_id, user2_id))

        dic = {}
        for tup in meetups:
            id1 = tup[0]
            id2 = tup[1]
            if id1 not in dic:
                dic[id1] = {id2}
            else:
                dic[id1].add(id2)
            if id2 not in dic:
                dic[id2] = {id1}
            else:
                dic[id2].add(id1)

        print(f"{len(dic)} users have been close to each other at some point.")

    def task_9(self):
        print()
        print("Task 9 - Top 15 users who have gained the most altitude:")
        self.cursor.execute("""
        SELECT
            u.id AS user_id,
            SUM(CASE WHEN tp1.altitude > tp2.altitude THEN tp1.altitude - tp2.altitude ELSE 0 END) AS total_altitude_gain
        FROM
            user AS u
        JOIN
            activity AS a ON u.id = a.user_id
        JOIN
            trackpoint AS tp1 ON a.id = tp1.activity_id
        JOIN
            trackpoint AS tp2 ON a.id = tp2.activity_id AND tp1.id = tp2.id - 1
        WHERE
            tp1.altitude IS NOT NULL AND tp2.altitude IS NOT NULL
        GROUP BY
            u.id
        ORDER BY
            total_altitude_gain DESC
        LIMIT
            15;

        """)

        results = self.cursor.fetchall()

        print(tabulate([*results], headers=["user_id", "altitude_gain"], floatfmt=("", ".0f")))

    def task_10(self):
        print()
        print(
            "Task 10 - Find the users that have traveled the longest total distance in one day for each transportation mode:")
        self.cursor.execute("""

        WITH distances as 
        (
            SELECT 
                a.user_id,
                a.transportation_mode,
                SUM(
                    6371 * ACOS(
                        COS(RADIANS(tp1.lat)) * COS(RADIANS(tp2.lat)) * COS(RADIANS(tp2.lon) - RADIANS(tp1.lon)) +
                        SIN(RADIANS(tp1.lat)) * SIN(RADIANS(tp2.lat))
                    )
                ) as total_distance,
                DATE(tp1.date) as travel_date
            FROM 
                activity a
            JOIN 
                trackpoint tp1 ON a.id = tp1.activity_id
            JOIN 
                trackpoint tp2 ON a.id = tp2.activity_id AND tp1.id = tp2.id - 1
            WHERE 
                a.transportation_mode IS NOT NULL
            GROUP BY 
                a.user_id, a.transportation_mode, travel_date
        ), ActivityMaxDistance AS (
            SELECT 
                d.transportation_mode,
                MAX(d.total_distance) AS max_distance 
            FROM distances d
            GROUP BY d.transportation_mode
        )

        SELECT user_id, d.transportation_mode, max_distance
        FROM distances d
        JOIN ActivityMaxDistance amd ON d.transportation_mode = amd.transportation_mode AND d.total_distance = amd.max_distance
        ORDER BY d.transportation_mode DESC;

        """)

        results = self.cursor.fetchall()

        print(tabulate([*results], headers=["user_id", "transportation_mode", "max_distance"]))

    def task_11(self):
        print()
        print("Task 11 -  Find all users who have invalid activities, and the number of invalid activities per user:")
        self.cursor.execute("""
        WITH InvalidActivities AS (
            SELECT
                tp1.activity_id AS a_id
            FROM
                trackpoint AS tp1
            JOIN trackpoint AS tp2 ON tp1.id + 1 = tp2.id
            WHERE
                tp1.activity_id = tp2.activity_id
                AND TIMESTAMPDIFF(MINUTE, tp1.date, tp2.date) >= 5
            GROUP BY tp1.activity_id
        )
        SELECT
            a.user_id,
            COUNT(ia.a_id) AS invalid_activity_count
        FROM
            activity AS a
        JOIN InvalidActivities AS ia ON a.id = ia.a_id
        GROUP BY a.user_id;


        """)

        results = self.cursor.fetchall()

        print(tabulate([*results], headers=["user_id", "invalid_activity_count"]))

    def task_12(self):
        print()
        print(
            "Task 12 - Find all users who have registered transportation_mode and their most used transportation_mode:")

        self.cursor.execute("""
        SELECT 
            user_id, 
            transportation_mode
        FROM 
            (SELECT 
                user_id, 
                transportation_mode, 
                RANK() OVER(PARTITION BY user_id ORDER BY COUNT(*) DESC) as rnk
             FROM activity
             WHERE transportation_mode IS NOT NULL
             GROUP BY user_id, transportation_mode) AS Ranked
        WHERE 
            rnk = 1
        ORDER BY 
            user_id ASC;

        """)

        results = self.cursor.fetchall()

        print(tabulate([*results], headers=["user_id", "most_used_transportation_mode"]))
