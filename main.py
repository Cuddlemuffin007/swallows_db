import psycopg2
import pandas.io.sql as psql


def search_by_name(cursor, connection):
    name = input("Please enter the player's last name\n>> ")
    dataframe = psql.read_sql_query('select * from batting_stats where last_name = (%s);', connection, params=(name,))
    return dataframe

def search_function(cursor, search_parameter):
    if search_parameter == 'avg':
        cursor.execute("select first_name, last_name, avg from batting_stats;")
    elif search_parameter == 'h':
        cursor.execute("select first_name, last_name, h from batting_stats;")
    elif search_parameter == 'hr':
        cursor.execute("select first_name, last_name, hr from batting_stats;")
    elif search_parameter == 'rbi':
        cursor.execute("select first_name, last_name, rbi from batting_stats;")
    elif search_parameter == 'r':
        cursor.execute("select first_name, last_name, r from batting_stats;")

    return cursor.fetchall()


def return_results(search_parameter, search_results, num_results=10, descending=True):
    if num_results > len(search_results):
        num_results = len(search_results)
    for result in sorted(search_results, key=lambda x: x[2], reverse=descending)[:num_results]:
        print("{} {}, {}: {}".format(result[0], result[1], search_parameter.upper(), result[2]))

def insert_new_player(cursor):
    insert_template = """
        INSERT INTO batting_stats
            VALUES (%s, %s, %s, '0', '0', '0', %s, %s, '0', '0', %s, %s, '0',
                    '0', '0', '0', %s, '0', '0', '0', '0', '0', '0', '0', '0', '0')
    """
    prompt = "Please enter {}.\n>> "
    first_name = input(prompt.format("first name"))
    last_name = input(prompt.format("last name"))
    age = input(prompt.format("age"))
    runs = input(prompt.format("number of runs scored"))
    hits = input(prompt.format("number of hits"))
    hr = input(prompt.format("number of home runs"))
    rbi = input(prompt.format("number of RBIs"))
    avg = input(prompt.format("batting average"))

    cursor.execute(insert_template, (first_name, last_name, age, runs,
                                     hits, hr, rbi, avg))

conn = psycopg2.connect(user="bkr", database="swallows_stats_db")
cur = conn.cursor()

print("Welcome to the Tokyo Yakult Swallows batting stats reference.")

while True:
    print("")
    choice = input("You may:\n(1) search the player database\n(2) add new player data\n(3) quit\n>> ")
    if choice == "1":
        search_param = input("You can search by name (name), "
                             "season average (avg), hits (h), homeruns (hr), "
                             "RBIs (rbi), or runs scored (r)\n>> ")

        if search_param == 'name':
            results = search_by_name(cur, conn)
            print(results.to_string(index=False, justify='left'))
            continue

        results = search_function(cur, search_param)

        print("The first ten results are displayed by default in descending order.")
        change_default = input("You can change this (y) or press ENTER to continue.\n>> ")
        print("\nFetching results...\n")
        if change_default != 'y':
            return_results(search_param, results)
        else:
            num_results = int(input("How many results would you like to display?\n>> "))
            order = input("Would you like to display the results in (a)scending or (d)escending order?\n>> ")
            new_order = False if order == 'a' else True
            return_results(search_param, results, num_results, new_order)

    elif choice == "2":
        insert_new_player(cur)
        conn.commit()

    elif choice == "3":
        exit(0)

cur.close()
conn.close()
