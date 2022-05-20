import sys
from .swen344_db_utils import *
from psycopg2 import sql
from datetime import datetime

"""
Project: Rideshare System

You are a tech company that provides a mobile app for booking taxi rides (e.g. Uber, Lyft). Both drivers and riders use 
this app and this is the database that stores the records of those trips. The app also has a reviewing system where both
drivers and riders review each other.

DB1-DB4 iterations.
author: Miguel Reyes
date: 02/04/22
"""
part_of_a_carpool = False
carpool = 0


def create_tables():
    """
    Function creates tables and populates them with some data for drivers and riders. Note - Drivers can also be riders.
    :return:None
    """
    drop_sql = """
           DROP TABLE IF EXISTS drivers, riders, trip_exp, receipt, trips ;
       """
    create_sql = """
           CREATE TABLE drivers(
               id SERIAL PRIMARY KEY NOT NULL,
               user_name VARCHAR(30) NOT NULL UNIQUE,
               password TEXT DEFAULT '0',
               session_key TEXT,       
               creation_date TIMESTAMP(2),  
               name VARCHAR(30) NOT NULL,                             
               avg_rating FLOAT DEFAULT 0,               
               car_make VARCHAR(30) NOT NULL,
               car_model VARCHAR(30) NOT NULL,
               years_driving SMALLINT DEFAULT 0,
               zip_code VARCHAR(7) NOT NULL,
               is_available BOOLEAN DEFAULT FALSE,
               is_driving BOOLEAN DEFAULT FALSE,
               is_active BOOLEAN DEFAULT TRUE
           );
           INSERT INTO drivers(user_name, name, avg_rating, car_make, car_model, zip_code, is_available)
                VALUES ('tomM061598', 'Tom Magliozzi', 3.2, 'Toyota', 'Prius', '06830', TRUE),
                ('rayM031198', 'Ray Magliozzi', 3.4, 'Ford', 'Focus', '06830', TRUE);
                
           CREATE TABLE riders (                
                id SERIAL PRIMARY KEY NOT NULL,
                user_name VARCHAR(30) NOT NULL UNIQUE,
                password TEXT DEFAULT '0',
                session_key TEXT,
                creation_date TIMESTAMP(2),
                name VARCHAR(30) NOT NULL, 
                avg_rating FLOAT DEFAULT 0, 
                zip_code VARCHAR(7) NOT NULL,
                is_available BOOLEAN DEFAULT FALSE,
                is_active BOOLEAN DEFAULT TRUE               
           );
           INSERT INTO riders(user_name, name, avg_rating, zip_code, is_available)
                VALUES ('mikeE06830', 'Mike Easter', 3.3, '06830', TRUE);
           INSERT INTO riders(user_name, name, avg_rating, zip_code)
                SELECT user_name, name, avg_rating, zip_code FROM drivers;  
                
            CREATE TABLE trips(
            id SERIAL PRIMARY KEY NOT NULL,
            driver_id INT REFERENCES drivers(id),
            driver_name VARCHAR(30),
            rider_id INT REFERENCES riders(id),
            rider_name VARCHAR(30),
            driver_instruction VARCHAR(75) DEFAULT 'None',
            rider_instruction VARCHAR(75) DEFAULT 'None',
            pick_up VARCHAR(30) DEFAULT 'Testing',
            drop_off VARCHAR(30) DEFAULT 'Testing',
            pick_up_date TIMESTAMP(2),
            drop_off_date TIMESTAMP(2),
            carpool_value INT,
            initial_price INT,
            receipt INT,            
            is_complete BOOLEAN  
           ); 
            INSERT INTO trips(driver_id, rider_id, is_complete)
                VALUES (1,1,TRUE), (2,1,TRUE), (1,3,TRUE);                                               
           UPDATE trips SET driver_name = d.name FROM drivers d WHERE trips.driver_id = d.id ;
           UPDATE trips SET rider_name = r.name FROM riders r WHERE trips.rider_id = r.id ; 
              
           CREATE TABLE trip_exp(
                id SERIAL PRIMARY KEY NOT NULL,
                trips_id INT REFERENCES trips (id),                               
                drivers_review VARCHAR(75) ,
                riders_review VARCHAR(75),
                drivers_rating SMALLINT CHECK (drivers_rating >= 0 AND drivers_rating <= 5),
                riders_rating SMALLINT CHECK (riders_rating >= 0 AND riders_rating <= 5)                              
           );                                                                
           INSERT INTO trip_exp(trips_id)
           SELECT id FROM trips; 
           
            CREATE TABLE receipt(
            id SERIAL PRIMARY KEY NOT NULL,
            trips_id INT REFERENCES trips(id),
            total INT                
           ); 
       """
    list_sql_cmds = [drop_sql, create_sql]
    conn = exec_commit(list_sql_cmds)
    conn.close()


def create_new_account_db2(user_name: str, password: bytes, name: str, date: datetime, zip_code: str,
                           car_make: str = None, car_model: str = None):
    """
        Create an account for either rider or driver. By default car_make and car_model are None type in order to
        recognize whose creating the account. New users have default values of 0 and None for avg_rating,years_driving
        and instructions, respectively. New account is committed to either driver or rider tables.

        :param user_name: user username
        :param password: user password stored as an int (SHA-512 digest function)
        :param date: Date created.
        :param name: The name of driver or rider.
        :param zip_code: Default value None - Driver or rider zip code
        :param car_make: Default value None - Drivers car make. If its a rider leave parameter empty.
        :param car_model: Default value None - Driver's car model. If its a rider leave parameter empty.
        :return: ID of rider or list of IDs if driver [driver_id, rider_id]
        """

    conn0 = connect()
    cur = conn0.cursor()

    if car_make is None:
        # Must be a rider account
        sql_cmd = "INSERT INTO riders(user_name, password, name, zip_code, creation_date) VALUES (%s, %s, %s, %s, %s);"
        conn = exec_commit(sql_cmd, (user_name, password.hex(), name, zip_code, date))
        conn.close()
        cur.execute("SELECT id FROM riders ORDER BY id DESC LIMIT 1")
        r_id = int(cur.fetchone()[0])
        conn0.close()
        return r_id
    else:
        sql_cmd = """INSERT INTO drivers(user_name, password, name, zip_code, creation_date, car_make, car_model) 
        VALUES (%s, %s, %s, %s, %s, %s, %s);"""
        conn = exec_commit(sql_cmd, (user_name, password.hex(), name, zip_code, date, car_make, car_model))
        conn.close()
        sql_cmd = "INSERT INTO riders(user_name, password, name,  zip_code, creation_date) VALUES (%s, %s, %s, %s, %s);"
        conn = exec_commit(sql_cmd, (user_name, password.hex(), name, zip_code, date))
        conn.close()
        cur.execute("SELECT id FROM drivers ORDER BY id DESC LIMIT 1")
        d_id = int(cur.fetchone()[0])
        cur.execute("SELECT id FROM riders ORDER BY id DESC LIMIT 1")
        r_id = int(cur.fetchone()[0])
        conn0.close()
        ids = [d_id, r_id]
        return ids


def remove_or_disable_account_db2(user_id: int, user_type: str, remove_or_disable: int) :
    """
    Remove or disable user account. Change is committed to either driver or rider tables.

    :param user_id: unique identifier given to all users
    :param user_type: (R or r) for rider and (D or d) for driver
    :param remove_or_disable: 1 if user wants to be removed. 0 if user wants to be disabled.
    :return: None
    """
    u_t = user_type.lower()
    conn = connect()
    cur = conn.cursor()
    if u_t == "r":
        cur.execute("SELECT name from riders where id = (%s)", (user_id,))
        test = cur.fetchone()
        conn.close()
        if not test:
            return 0
        if remove_or_disable == 1:
            sql_cmd = "DELETE FROM trip_exp USING trips WHERE trips_id = trips.id AND trips.rider_id= (%s); DELETE " \
                      "FROM trips WHERE rider_id = (%s); DELETE FROM riders WHERE id = (%s); "
            conn = exec_commit(sql_cmd, (user_id, user_id, user_id))
            conn.close()
        else:
            sql_cmd = "UPDATE riders SET is_active = FALSE WHERE user_id = (%s);"
            conn = exec_commit(sql_cmd, (user_id,))
            conn.close()
        return 1
    elif u_t == "d":
        cur.execute("SELECT name from drivers where id = (%s)", (user_id,))
        test = cur.fetchone()
        conn.close()
        if not test:
            return 0
        if remove_or_disable == 1:
            sql_cmd = "DELETE FROM trip_exp USING trips WHERE trips_id = trips.id AND trips.driver_id = (%s); DELETE " \
                      "FROM trips WHERE driver_id = (%s); DELETE FROM drivers WHERE id = (%s); "
            conn = exec_commit(sql_cmd, (user_id, user_id, user_id))
            conn.close()
        else:
            sql_cmd = "UPDATE drivers SET is_active = FALSE WHERE id = (%s);"
            conn = exec_commit(sql_cmd, (user_id,))
            conn.close()
        return 1
    else:
        print("Error")
        return 0


def update_account_db2(user_type: str, user_name: str, password: bytes = None,
                       name: str = None, zip_code: str = None, is_active: bool = None, car_make: str = None,
                       car_model: str = None):
    """
    Update user account. If user_id is None, creates new account using non-none parameters. Pass in None for parameters
    you wish to not update. All changes are committed to either driver or rider tables.

    :param user_name:
    :param password:
    :param user_type: (R or r) for rider and (D or d) for driver
    :param name: Default value None - The name of driver or rider.
    :param zip_code: Default value None - Driver or rider zip code.
    :param is_active: Default value None - True or False. Account is active or inactivate, respectively
    :param car_make: Default value None - Drivers car make. If its a rider leave parameter empty.
    :param car_model: Default value None - Driver's car model. If its a rider leave parameter empty.
    :return: None
    """
    # Update user values that exists
    u_t = user_type.lower()
    if u_t == "r":
        # riders
        conn = connect()
        cur = conn.cursor()
        cur.execute("SELECT name from riders where user_name = (%s)", (user_name,))
        result = cur.fetchone()
        conn.close()
        if not result:
            return None
        values = {"password": password, "name": name, "zip_code": zip_code,
                  "active": is_active}  # key = field, value = field values
        for key in values.keys():
            if values[key] is None:
                continue
            else:
                if key == "password":
                    values[key] = password.hex()
                conn = exec_commit(
                    sql.SQL("UPDATE riders SET {} = (%s) WHERE user_name = (%s);").format(sql.Identifier(key)),
                    (values[key], user_name))  # this format needs to be used if you want to dynamically insert
                # table
                # or field variables to SQL queries.
                conn.close()
        return 1
    elif u_t == "d":
        # drivers
        conn = connect()
        cur = conn.cursor()
        cur.execute("SELECT name from drivers where user_name = (%s)", (user_name,))
        result = cur.fetchone()
        conn.close()
        if not result:
            return None
        values = {"password": password, "name": name, "zip_code": zip_code,
                  "car_make": car_make, "car_model": car_model,
                  "active": is_active}  # key = field, value = field values
        for key in values.keys():
            if values[key] is None:
                continue
            else:
                if key == "password":
                    values[key] = password.hex()
                conn = exec_commit(
                    sql.SQL("UPDATE drivers SET {} = (%s) WHERE user_name = (%s);").format(sql.Identifier(key)),
                    (values[key], user_name))  # this format needs to be used if you want to dynamically insert
                # table
                # or field variables to SQL queries.
                conn.close()
        return 1
    else:
        print("Error")
        return 0


def get_account(table: str, id):
    if table == "riders":
        test = exec_get_one("SELECT id, user_name, name, avg_rating, zip_code from riders WHERE id = (%s)",
                            (id,))
    else:
        test = exec_get_one("SELECT id, user_name, name, avg_rating, car_make, car_model, zip_code "
                            "from drivers WHERE id = (%s)", (id,))

    return test


def all_accounts():
    riders = exec_get_all("SELECT id, user_name, password, name, avg_rating, zip_code from riders")
    drivers = exec_get_all("SELECT id, user_name, password, name, avg_rating, car_make, car_model, zip_code "
                           "from drivers")
    users = riders + drivers
    return users


def change_availability_db2(user_type: str, user_id: int, is_available: bool) -> None:
    """
    Changes users availability. Change is committed to either the riders or drivers table.

    :param user_type: (R or r) for rider and (D or d) for driver
    :param user_id: unique identifier given to all users.
    :param is_available: Is the user available to provide or receive services.
    :return: None
    """

    u_t = user_type.lower()
    if u_t == "r":
        sql_cmd = "UPDATE riders SET is_available = (%s) WHERE id = (%s);"
        conn = exec_commit(sql_cmd, (is_available, user_id))
        conn.close()
    elif u_t == "d":
        sql_cmd = "UPDATE drivers SET is_available = (%s) WHERE id = (%s);"
        conn = exec_commit(sql_cmd, (is_available, user_id))
        conn.close()

    else:
        print("ERROR")


def record_ride_db2(driver_id: int, rider_id: int, pick_up: str, drop_off: str, pick_up_date: datetime,
                    initial_price: int,
                    driver_instruction: str = None,
                    rider_instruction: str = None, ):
    """
    Record a new ride. Rides may only be recorded if the driver and rider have the same zipcode and are both marked as
    available. Rides committed to the trips table.

    :param driver_id: unique identifier given driver
    :param rider_id: unique identifier given rider
    :param pick_up: location where rider gets picked up
    :param drop_off: location where rider gets dropped off
    :param pick_up_date: Date the trip is recorded
    :param initial_price: price of trip from point a to b
    :param driver_instruction: Default value None - Drivers instruction for rider
    :param rider_instruction: Default value None - Riders instruction for driver
    :return: Trip id associated with ride if ride is recorded. 0 if ride is not recorded.
    """
    global part_of_a_carpool
    part_of_a_carpool = False
    global carpool

    conn = connect()
    cur = conn.cursor()
    # A trip is only valid if both rider & driver are available and they have the same zip code
    cur.execute("SELECT d.is_available, r.is_available FROM drivers d INNER JOIN riders r ON d.zip_code = r.zip_code "
                "WHERE d.id = (%s) AND r.id = (%s) ", (driver_id, rider_id))

    test = cur.fetchall()  # driver and rider availability fields
    is_trip_valid = True
    try:
        if test[0][0] is False or test[0][1] is False:
            is_trip_valid = False
    except IndexError:
        print("ERROR: NOT IN THE SAME ZIP CODE")
        sys.exit()
    conn.close()
    if is_trip_valid:
        # updating trips driver/rider name wherever the records driver/rider id match.
        update_trips = """ 
            UPDATE trips SET driver_name = d.name FROM drivers d WHERE trips.driver_id = d.id ;
            UPDATE trips SET rider_name = r.name FROM riders r WHERE trips.rider_id = r.id ;                         
        """
        conn = exec_commit([
            # this format needs to be used if you want to dynamically insert table or field variables to SQL queries
            sql.SQL("INSERT INTO trips ({fields}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)").format(
                fields=sql.SQL(',').join([
                    sql.Identifier('driver_id'),
                    sql.Identifier('rider_id'),
                    sql.Identifier('driver_instruction'),
                    sql.Identifier('rider_instruction'),
                    sql.Identifier('pick_up'),
                    sql.Identifier('drop_off'),
                    sql.Identifier('pick_up_date'),
                    sql.Identifier('initial_price'),
                ])), update_trips],
            (
                driver_id, rider_id, driver_instruction, rider_instruction, pick_up, drop_off, pick_up_date,
                initial_price))
        cur = conn.cursor()
        # if trip we're recording is valid check to see if the driver is currently driving if so this trip and any other
        # trip that is not complete with the same driver is considered a carpool
        cur.execute("SELECT is_driving from drivers WHERE id = (%s)", (driver_id,))
        test = cur.fetchone()[0]  # value of is_driving field
        if test is False:
            # driver was not driving but now he is
            cur.execute("UPDATE drivers SET is_driving = TRUE WHERE id = (%s)", (driver_id,))
            conn.commit()
        else:
            if part_of_a_carpool is False:
                # checking to see if this is the start of a carpool or another trip/rider joining the trip
                cur.execute("UPDATE trips SET carpool_value = (%s) + 1 WHERE driver_id = (%s) AND "
                            "trips.is_complete IS NULL",
                            (carpool, driver_id))
                part_of_a_carpool = True
                carpool += 1
            else:
                cur.execute(
                    "UPDATE trips SET carpool_value = (%s) WHERE driver_id = (%s) AND trips.is_complete IS "
                    "NULL",
                    (carpool, driver_id))
            conn.commit()
        conn.close()

        conn2 = connect()
        cur2 = conn2.cursor()
        # BELOW - update trip_exp table's trip_id column with new record from trips - we get the latest count after
        # creating a new record above ..  this count is the latest trip_id number
        cur2.execute("SELECT id FROM trips ORDER BY id DESC LIMIT 1")
        num = int(cur2.fetchone()[0])
        cur2.execute("INSERT INTO trip_exp(trips_id) VALUES (%s);", (num,))
        conn2.commit()
        conn2.close()
        return num
    else:
        return 0


def get_trips():
    trips = exec_get_all("SELECT driver_name, rider_name, pick_up, drop_off, receipt from trips")
    return trips


def mark_trip_complete_db2(confirm: int, trip_id: int, drop_off_date):
    """
    Updates confirm and date field in trips table. If not completed then trip is marked as incomplete
    (false for complete field in trips table). Change is committed to either the riders or drivers table.

    :param confirm: 1 for complete trip and 0 if trip is not completed.
    :param trip_id: trips table unique identifier
    :param drop_off_date: Data and time trip is completed.
    :return:
    """
    conn0 = connect()
    cur = conn0.cursor()
    cur.execute("SELECT driver_id from trips WHERE trips.id = (%s)", (trip_id,))
    driver_id = cur.fetchone()[0]

    if confirm == 1:
        sql_cmd = """
            UPDATE trips SET drop_off_date = (%s), is_complete = TRUE WHERE trips.id = (%s) ;
            UPDATE drivers SET is_driving = FALSE WHERE drivers.id = (%s);                                    
        """
        conn = exec_commit(sql_cmd, (drop_off_date, trip_id, driver_id))
        conn.close()
    else:
        sql_cmd = """
                    UPDATE trips SET is_complete = FALSE WHERE trips.id = (%s) ;
                    UPDATE drivers SET is_driving = FALSE WHERE drivers.id = (%s);            
                """
        conn = exec_commit(sql_cmd, (trip_id, driver_id))
        conn.close()


def generate_receipt_db3(trips_id: int):
    """
    Generate receipt for all trips - regular/complete, carpool/complete and incomplete.

    :param trips_id: Trip id from the trips table
    :return: Will return receipt dollar amount. If trip incomplete returns None, if trip is ongoing returns
    message "Trip is ongoing"
    """
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT is_complete, carpool_value, initial_price from trips WHERE id = (%s)", (trips_id,))
    values = cur.fetchone()
    is_complete = values[0]
    carpool_value = values[1]
    init_price = values[2]
    if is_complete:
        # check to see trip is considered complete
        if carpool_value is None:
            # carpool value of None = a regular trip therefore no need to divide amongst drivers
            # Regular trip return initial price of trip
            return init_price
        else:
            # Carpool trip return (initial price of trip/ # of riders)
            cur.execute("SELECT COUNT (carpool_value) FROM trips WHERE carpool_value = (%s)", (carpool_value,))
            number_of_riders = cur.fetchone()[0]
            return init_price / number_of_riders
    elif is_complete is None:
        # On going ride
        return "Trip is on-going"
    else:
        #  Ride could not be completed
        return None


def add_review_rating_db2(trips_id: int, user_type: str, drivers_review: str = None, riders_review: str = None,
                          drivers_rating: int = None, riders_rating: int = None) -> None:
    """
    Users can add review and rating to the trip. Updates drivers and riders avg rating
    :param trips_id: unique identifier in trips table
    :param user_type: (R or r) for rider and (D or d) for driver
    :param drivers_review: Default value None - The drivers review for the trip
    :param riders_review: Default value None - The riders review for the trip
    :param drivers_rating: Default value None - The rating the rider gives the driver for the trip
    :param riders_rating: Default value None - The rating the driver gives the rider for the trip
    :return: None
    """
    # this format needs to be used if you want to dynamically insert table or field variables to SQL queries
    u_t = user_type.lower()
    if u_t == "r":
        # rider is updating their review and giving the driver a rating
        sql_cmds = "UPDATE trip_exp SET riders_review = (%s) WHERE trips_id = (%s);" \
                   "UPDATE trip_exp SET drivers_rating = (%s) WHERE trips_id = (%s)"
        conn = exec_commit(sql_cmds, (riders_review, trips_id, drivers_rating, trips_id))
        conn.close()
        conn = connect()
        cur = conn.cursor()
        cur.execute("SELECT d.id, avg_rating FROM drivers d INNER JOIN trips t ON d.id = t.driver_id WHERE t.id = (%s)",
                    (trips_id,))
        data = cur.fetchone()
        driver_id = data[0]
        rating = data[1]
        new_driver_rating = (rating + drivers_rating) / 2
        conn.close()
        conn = exec_commit("UPDATE drivers SET avg_rating = (%s) WHERE id = (%s)", (new_driver_rating, driver_id))
        conn.close()
    elif u_t == "d":
        # driver is updating their review and giving the rider a rating
        sql_cmds = "UPDATE trip_exp SET drivers_review = (%s) WHERE trips_id = (%s);" \
                   "UPDATE trip_exp SET riders_rating = (%s) WHERE trips_id = (%s)"
        conn = exec_commit(sql_cmds, (drivers_review, trips_id, riders_rating, trips_id))
        conn.close()
        conn = connect()
        cur = conn.cursor()
        cur.execute("SELECT r.id, avg_rating FROM riders r INNER JOIN trips t ON r.id = t.rider_id WHERE t.id = (%s)",
                    (trips_id,))
        data = cur.fetchone()
        rider_id = data[0]
        rating = data[1]
        new_rider_rating = (rating + riders_rating) / 2
        conn.close()
        conn = exec_commit("UPDATE riders SET avg_rating = (%s) WHERE id = (%s)", (new_rider_rating, rider_id))
        conn.close()
    else:
        print("ERROR")


def list_available_users_db2(user_type: str, zip_code: str):
    """
    Provide your zip_code to see all available drivers or riders in your area.
    :param user_type: (R or r) for rider and (D or d) for driver
    :param zip_code: Area user is in
    :return: None if empty query. Otherwise, list of tuples with data from tables.
    """
    conn = connect()
    cur = conn.cursor()

    u_t = user_type.lower()
    if u_t == "r":
        cur.execute("SELECT * FROM drivers WHERE zip_code = (%s) AND is_available = TRUE;", (zip_code,))
        query = cur.fetchall()
    elif u_t == "d":
        cur.execute("SELECT * FROM riders WHERE zip_code = (%s) AND is_available = TRUE;", (zip_code,))
        query = cur.fetchall()
    else:
        return None

    return query


def list_all_my_rides_db3(user_type: str, user_id: int):
    """
        Provide your zip_code to see all available drivers or riders in your area.
    :param user_type: (R or r) for rider and (D or d) for driver
    :param user_id: The users id
    :return: None if empty query. Otherwise, list of tuples with data from tables.
    """
    conn = connect()
    cur = conn.cursor()

    u_t = user_type.lower()
    if u_t == "r":
        cur.execute("SELECT * FROM trips WHERE trips.rider_id = (%s) AND is_complete = TRUE;", (user_id,))
        query = cur.fetchall()
    elif u_t == "d":
        cur.execute("SELECT * FROM trips WHERE trips_driver_id = (%s) AND is_available = TRUE;", (user_id,))
        query = cur.fetchall()
    else:
        return None

    return query


def rides_in_a_day_db4(date: datetime) -> list:
    """
    Extract year, month and day from datetime object to query for all the rides in a given day

    :param date: datetime object
    :return: list
    """
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT t.driver_name, t.pick_up, t.drop_off, ARRAY_AGG (t.rider_name ORDER BY t.rider_name),"
                " CAST (AVG(drivers_rating)::numeric(10,1) AS VARCHAR(3)) FROM trips t INNER JOIN trip_exp e ON  "
                "e.trips_id = t.id WHERE EXTRACT ( YEAR FROM t.drop_off_date) = (%s) AND EXTRACT ( MONTH FROM "
                "t.drop_off_date) = (%s) AND EXTRACT (DAY FROM t.drop_off_date) = (%s) GROUP BY driver_name, pick_up,"
                " drop_off ORDER BY t.driver_name", (date.year, date.month, date.day))
    return cur.fetchall()


def fare_times_db4() -> list:
    """
    Query for the avg fare for a ride during the hour in the day.
    Note - if no rides are made in the hour ignore the hour.

    :return: list
    """
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT CAST (EXTRACT(HOUR FROM t.drop_off_date) AS INTEGER) AS hours, "
                "CAST (AVG(initial_price)::numeric(100,2) AS VARCHAR(5)) AS avg_fare FROM trips t WHERE "
                "EXTRACT(HOUR FROM t.drop_off_date) != -1 GROUP BY hours ORDER BY hours ;")
    return cur.fetchall()
