#!/usr/bin/python3

import sqlite3

def get_random_user():
    db = sqlite3.connect('credit.db')
    cursor = db.cursor()
    cursor.execute("SELECT uuid from credit_data ORDER BY random() LIMIT 1")
    uuid = cursor.fetchone()[0]
    print(uuid)
    db.close()


if __name__ == "__main__":
    get_random_user()
