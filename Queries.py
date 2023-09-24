from DbConnector import DbConnector


class Queries:

    def __init__(self):
        self.db_connector = DbConnector()
        self.connection = self.db_connector.db_connection
        self.cursor = self.db_connector.cursor

    def task_1(self):
        print("Task 1 - Number of users, activities and trackpoints:")
        self.cursor.execute("""
        SELECT count(id)
        FROM user
        UNION ALL
        SELECT count(id)
        FROM activity
        UNION ALL
        SELECT count(id)
        FROM trackpoint
        """)

        # Fetch all the results
        results = self.cursor.fetchall()

        # Extract and print the counts
        user_count = results[0][0]
        activity_count = results[1][0]
        trackpoint_count = results[2][0]

        print(f"Number of users: {user_count}")
        print(f"Number of activities: {activity_count}")
        print(f"Number of trackpoints: {trackpoint_count}")
        print()

        return results

    def task_2(self):
        print("Task 2 - Average, maximum and minimum number of trackpoints per user:")
        self.cursor.execute("""
            SELECT 
                AVG(trackpoints_count) AS average_trackpoints, 
                MAX(trackpoints_count) AS maximum_trackpoints, 
                MIN(trackpoints_count) AS minimum_trackpoints
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

        average_trackpoints = results[0][0]
        maximum_trackpoints = results[0][1]
        minimum_trackpoints = results[0][2]

        print(f"Average trackpoints pr. user: {average_trackpoints}")
        print(f"Maximum trackpoints pr. user: {maximum_trackpoints}")
        print(f"Minimum trackpoints pr. user: {minimum_trackpoints}")
        print()

        return results

    def task_3(self):
        print("Task 3 - Top 15 users with most activities:")
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

        print("Top 15 users with most activities:")
        for result in results:
            print(f"User: {result[0]}, Activities: {result[1]}")
        print()

        return results

    def task_4(self):
        print("Task 4 - Users who have taken a bus:")
        self.cursor.execute("""
        SELECT DISTINCT user.id
        FROM user
        INNER JOIN activity ON user.id = activity.user_id
        WHERE activity.transportation_mode = 'bus';
        """)

        results = self.cursor.fetchall()

        print("Users who have taken a bus:")
        for result in results:
            print(f"User: {result[0]}")
        print()

        return results

    def task_5(self):
        print("Task 5 - Top 10 users with most types of transportation modes:")
        self.cursor.execute("""
        SELECT 
            user.id, 
            COUNT(DISTINCT activity.transportation_mode) AS mode_count
        FROM user
        INNER JOIN activity ON user.id = activity.user_id
        WHERE activity.transportation_mode != 'NULL'
        GROUP BY user.id
        ORDER BY mode_count DESC
        LIMIT 10;
        """)

        results = self.cursor.fetchall()

        print("Top 10 users with most types of transportation modes:")
        for result in results:
            print(f"User: {result[0]}, Modes: {result[1]}")
        print()

        return results

    def task_6(self):
        print("Task 6 - Find activities that are registered multiple times:")

        self.cursor.execute("""
        SELECT 
            user_id, start_date_time, end_date_time, COUNT(*) 
        FROM 
            activity
        GROUP BY 
            user_id, start_date_time, end_date_time
        HAVING 
            COUNT(*) > 1;
        """)

        results = self.cursor.fetchall()

        if len(results) == 0:
            print("No duplicate activities found.")
        else:
            print("Duplicate activities found:")
            for result in results:
                print(f"User: {result[0]}, Start Time: {result[1]}, End Time: {result[2]}, Count: {result[3]}")
        print()

        return results

    def task_7(self):
        self._task_7a()
        self._task_7b()

    def _task_7a(self):
        print("Task 7a -  Find the number of users that have started an activity in one day and ended the activity the next day:")
        self.cursor.execute("""
        SELECT 
            COUNT(DISTINCT user_id) 
        FROM activity
        WHERE DATE(start_date_time) != DATE(end_date_time) AND transportation_mode != 'NULL';
        """)

        results = self.cursor.fetchall()

        print(f"Amount: {results[0][0]}")
        print()

        return results

    def _task_7b(self):
        print("Task 7b - List the transportation mode, user id and duration for these activities")
        self.cursor.execute("""
        SELECT 
            user_id, 
            transportation_mode, 
            TIMESTAMPDIFF(MINUTE, start_date_time, end_date_time) AS duration_minutes
        FROM activity
        WHERE DATE(start_date_time) != DATE(end_date_time) AND transportation_mode != 'NULL';
        """)

        results = self.cursor.fetchall()

        print("List the transportation mode, user id and duration for these activities:")
        for result in results:
            print(f"User: {result[0]}, Mode: {result[1]}, Duration: {result[2]}")
        print()

        return results

    def task_8(self):
        print("Task 8 - Find the number of users which have been close to each other in time and space.\n"
              "Close is defined as the same space (50 meters) and for the same half minute (30"
              "seconds)")

    def task_9(self):
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
            tp1.altitude != 'NULL' AND tp2.altitude != 'NULL'
        GROUP BY
            u.id
        ORDER BY
            total_altitude_gain DESC
        LIMIT
            15;
        """)

        results = self.cursor.fetchall()

        print("Top 15 users with most altitude gain:")
        for result in results:
            print(f"User: {result[0]}, Altitude gain: {result[1]}")
        print()

        return results

    def task_10(self):
        print("Task 10 - Top 20 users who have traveled the longest distance:")
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
            WHERE transportation_mode != 'NULL'
            GROUP BY user_id, transportation_mode) AS Ranked
        WHERE 
            rnk = 1
        ORDER BY 
            user_id ASC;
        """)

        results = self.cursor.fetchall()

        print("Most used transportation mode per user:")
        for result in results:
            print(f"User: {result[0]}, Mode: {result[1]}")
        print()

        return results

    def task_11(self):
        print("Task 11 -  Find all users who have invalid activities, and the number of invalid activities per user:")



    def task_12(self):
        print("Task 12 - Find all users who have registered transportation_mode and their most used transportation_mode:")

