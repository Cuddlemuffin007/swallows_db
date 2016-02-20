# create a table
# load in data

import psycopg2
import csv

with open("player_data") as csvfile:
    file_reader = csv.reader(csvfile)
    rows = [row for row in file_reader]
    headers = rows[0]
    data = rows[1:]

conn = psycopg2.connect(database="swallows_stats_db", user="bkr")
cur = conn.cursor()

cur.execute("DROP TABLE IF EXISTS batting_stats;")

create_table_string = """
    CREATE TABLE batting_stats (
      first_name varchar(20),
      last_name varchar(30),
      age numeric(3),
      gp numeric(3),
      pa numeric(3),
      ab numeric(3),
      r numeric(3),
      h numeric(3),
      _2b numeric(2),
      _3b numeric(2),
      hr numeric(2),
      rbi numeric(3),
      sb numeric(2),
      cs numeric(2),
      bb numeric(3),
      so numeric(3),
      avg real,
      obp real,
      slg real,
      ops real,
      tb numeric(3),
      gdp numeric(2),
      hbp numeric(2),
      sh numeric(2),
      sf numeric(2),
      ibb numeric(2)
    )
"""
cur.execute(create_table_string)
conn.commit()

insert_template = """
  INSERT INTO batting_stats
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

roster = data
count = 0
for player in roster:
    cur.execute(insert_template, player)
    count += 1
    if count % 4 == 0:
        conn.commit()
conn.commit()

cur.close()
conn.close()