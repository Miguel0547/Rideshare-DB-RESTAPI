from .rideshare import *
from .authentication import *
import datetime


def build_tables():
    """Build the tables"""
    conn = connect()
    cur = conn.cursor()
    create_tables()
    cur.execute('SELECT * FROM riders')
    cur.execute('SELECT * FROM drivers')
    cur.execute('SELECT * FROM trips')
    conn.close()


def update_years_driving_field():
    """Update drivers 'years driving' field to something other than 0"""
    conn = connect()
    cur = conn.cursor()
    create_tables()
    # the ; after the 1 in "WHERE id = 1" allows us to perform multiple operations -  must do this for multiple
    # operations unless its the last operation.
    cur.execute('UPDATE drivers '
                'SET years_driving = 4 '
                'WHERE id = 1;'
                'UPDATE drivers '
                'SET years_driving = 2 '
                'WHERE id = 2')
    conn.close()


def update_Tom_Ray():
    """
    Call API function update_account to update fields in either riders or drivers table. Here we are updating
    Toms car and Rays name.
    """
    # resets the tables so we can run the test with the right values.
    create_tables()
    # change Tom's car to a BMW X3
    update_account_db2("d", "tomM061598", None, None, None, None, "BMW", "X3")
    # change Ray's name
    update_account_db2("d", "rayM031198", None, "Ray Gilmore")


def create_accounts():
    """Call API function create_new_account to create Hoke Colburn the driver and Ms.Daisy the rider."""
    # Create Hoke Colburn
    password = hash_password("Hoke0")
    create_new_account_db2("HokiePokie", password, "Hoke Colburn", datetime.datetime.today(), "30301", "Volvo", "X-65")
    # Create Ms.Daisy
    password = hash_password("Daisy0")
    create_new_account_db2("DaisyDukes", password, "Ms.Daisy", datetime.datetime.today(), "30301")


def change_MsDaisy_availability():
    """Call API function change_availability to update Ms.Daisy's availability to available"""
    change_availability_db2("r", 5, True)


def Hoke_MsDaisy_listing():
    """Call API function available_users to display what users are available in the given zip_code"""
    # change Hoke to available as a driver
    change_availability_db2("d", 3, True)


def record_ride():
    """
    Call API function record_ride to record the new ride between Hoke and Ms.Daisy. New ride is recorded to the
    trips table.
    """
    record_ride_db2(3, 5, "43, -70", "43.085, -77.67",
                    datetime.datetime(1989, 12, 13, 12, 0, 0, 0), 11, "No eating in the car.",
                    "Please, help drive carefully.")
    # change Hoke to available as a rider and unavailable as a driver so we can do Tom gives ride to Hoke
    change_availability_db2("r", 4, True)
    change_availability_db2("d", 3, False)

    # update Toms zip_code to 30301
    update_account_db2("d", "tomM061598", None, None, "30301")

    record_ride_db2(1, 4, "40.75, -73.99", "40.75, -73.98",
                    datetime.datetime(1989, 12, 14, 4, 0, 0, 0), 13, "No noise.", "Special")


def rating():
    mark_trip_complete_db2(1, 4, datetime.datetime.today())
    mark_trip_complete_db2(1, 5, datetime.datetime.today())
    add_review_rating_db2(5, "r", None, "Tom is so funny", 5)
    add_review_rating_db2(5, "d", "Hoke is very kind", None, None, 5)
    add_review_rating_db2(4, "r", None, "Hoke is a gentleman", 5)
    add_review_rating_db2(4, "d", "Ms.Daisy is very kind", None, None, 5)


def remove_account():
    # Remove Ms.Daisy's riders account
    remove_or_disable_account_db2(5, "r", 1)


def sketches_db3():
    #  Test data Alex, Bobby, Louie, Elaine, Tony, Godot, Vladimir
    password = hash_password("Alex0")
    alex = create_new_account_db2("AlexTheA", password, "Alex", datetime.datetime.today(), "06807", "Honda", "Accord")
    password = hash_password("Bobby0")
    bobby = create_new_account_db2("BobbyTheB", password, "Bobby", datetime.datetime.today(), "06807", "Honda", "Civic")
    password = hash_password("Louie0")
    louie = create_new_account_db2("LouieTheL", password, "Louie", datetime.datetime.today(), "06807", "Honda", "Pilot")
    password = hash_password("Elaine0")
    elaine = create_new_account_db2("ElaineTheE", password, "Elaine", datetime.datetime.today(), "06807", "Nissan",
                                    "Altima")
    password = hash_password("Tony0")
    tony = create_new_account_db2("TonyTheT", password, "Tony", datetime.datetime.today(), "06807", "Nissan", "Rogue")
    password = hash_password("Godot0")
    godot = create_new_account_db2("GodotTheG", password, "Godot", datetime.datetime.today(), "06807", "BMW", "X5")
    password = hash_password("Vlad0")
    vlad = create_new_account_db2("VladTheV", password, "Vladimir", datetime.datetime.today(), "06807")

    # update both godot and vlad as a available driver and rider and not available inversely, respectively
    change_availability_db2("r", godot[1], False)
    change_availability_db2("d", godot[0], True)
    change_availability_db2("r", vlad, True)
    # Godot and Vladimir trip
    vlad_godot_trip = record_ride_db2(godot[0], vlad, "RIT", "Gas", datetime.datetime.today(), 12)
    # Trip fell through, mark it incomplete
    mark_trip_complete_db2(0, vlad_godot_trip, datetime.datetime(2022, 2, 3, 3))

    # Alex's carpool trip
    riders = [bobby, louie, elaine, tony]  # All of our riders
    for rider in riders:
        # make bobby, louie, elaine, tony not available as drivers but available us riders
        change_availability_db2("r", rider[1], True)
        change_availability_db2("d", rider[0], False)
    # Alex is available as a driver but not a rider
    change_availability_db2("d", alex[0], True)
    change_availability_db2("r", alex[1], False)

    trips = []  # list containing all of our trips between alex and the 4 riders. Each trip is evaluated as a unique
    # Alex will pick up multiple riders without marking any ride complete which we evaluate to be a carpool.
    for rider in riders:
        x = record_ride_db2(alex[0], rider[1], "123", "456",
                            datetime.datetime.today(), 12)
        if rider == bobby:
            bobbys_trip = x
        if rider == louie:
            louies_trip = x
        trips.append(x)

    receipts = []  # list containing every riders receipt.
    for x in trips:
        mark_trip_complete_db2(1, x, datetime.datetime.today())
        receipts.append(generate_receipt_db3(x))

    # Louie's review and rating for Alex
    add_review_rating_db2(louies_trip, "r", None, "Terrible ride, Alex smells.", 2)

    # Alex response to Louie's review
    add_review_rating_db2(louies_trip, "d", "Sorry, I forgot to put on deodorant", None, None, 1)

    # Bobby's review and rating for Alex
    add_review_rating_db2(bobbys_trip, "r", None, "Alex was so kind!", 5)

    users = [alex, bobby, louie, elaine, tony]
    # Setting all users to their default availability of false
    for user in users:
        change_availability_db2("d", user[0], False)
        change_availability_db2("r", user[1], False)

    # Tony gives ride to Alex and Elaine. Then marks him unavailable.
    change_availability_db2("d", tony[0], True)
    change_availability_db2("r", alex[1], True)
    change_availability_db2("r", elaine[1], True)
    alexs_trip = record_ride_db2(tony[0], alex[1], "135", "54", datetime.datetime.today(), 15)
    elaines_trip = record_ride_db2(tony[0], elaine[1], "135", "54", datetime.datetime.today(), 15)
    add_review_rating_db2(alexs_trip, "r", None, "Tony is a little rude", 3)
    add_review_rating_db2(elaines_trip, "r", None, "Tony is a little rude", 5)
    mark_trip_complete_db2(1, alexs_trip, datetime.datetime.today())
    mark_trip_complete_db2(1, elaines_trip, datetime.datetime.today())

    change_availability_db2("d", tony[0], False)


def check_login_tester():
    hashed_password = hash_password("Hoke0")
    results = login("d", "HokiePokie", hashed_password)
    try:
        int(results, 16)
        print("SUCCESS")
        if check_session_key(results):
            print("CHECK IS SUCCESSFUL")
        else:
            print("CHECK IS UNSUCCESSFUL")
    except ValueError:
        print(results)



def check_unique_user():
    password = hash_password("Hoke0")
    try:
        create_new_account_db2("HokiePokie", password, "Hoke Colburn", datetime.datetime.today(), "30301", "Volvo", "X-65")
    except:
        print("ERROR")

def check_update_user():
    update_account_db2("r", "fosifog", None, "BobbyLow")


def load_db():
    build_tables()
    update_years_driving_field()
    update_Tom_Ray()
    create_accounts()
    change_MsDaisy_availability()
    Hoke_MsDaisy_listing()
    record_ride()
    rating()
    remove_account()
    sketches_db3()
    # check_update_user()
    # check_unique_user()
    check_login_tester()
