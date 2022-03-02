#!/usr/bin/python3

import bz2
import uuid
import sqlite3

# Open the bzip file and return the lines as a generator.
def get_lines():
    with bz2.open('test.dat.bz2', mode='rt') as file:
        for i, line in enumerate(file):
            if i == 0:
                continue
            yield line


# process each line in file. Each line is fixed-width with 
# the name as the first 72 characters and the rest being 
# 9-character entries.
def process_line(line):
    name = line[0:72].strip()
    
    # Generate the UUID and convert to string.
    uid = str(uuid.uuid4())
    
    # The last character in the string is a newline. Hence, it's not included.
    line = line[72:-1]
    credit_data = [uid, name]

    credit_data.extend(list(retrieve(line)))
    return credit_data

# convert the remaining string into a list of 9 characters.
def retrieve(line):
    for i in range(0, len(line), 9):
        if i == 0:
            # First record (after the name) is the SSN. Keep it as a string.
            yield line[i:i + 9]
        else:
            # Make an integer out of it.
            yield int(line[i:i + 9])


def create_credit_table(db):
    print('Creating table...')
    cursor = db.cursor()
    cursor.execute('drop table if exists credit_data')
    cursor.execute('''create table credit_data(
        uuid varchar(36) primary key,
        name varchar(72),
        SSN varchar(9),
        X0001 int, X0002 int, X0003 int, X0004 int, X0005 int, X0006 int, X0007 int, X0008 int, X0009 int, X0010 int,
        X0011 int, X0012 int, X0013 int, X0014 int, X0015 int, X0016 int, X0017 int, X0018 int, X0019 int, X0020 int,
        X0021 int, X0022 int, X0023 int, X0024 int, X0025 int, X0026 int, X0027 int, X0028 int, X0029 int, X0030 int,
        X0031 int, X0032 int, X0033 int, X0034 int, X0035 int, X0036 int, X0037 int, X0038 int, X0039 int, X0040 int,
        X0041 int, X0042 int, X0043 int, X0044 int, X0045 int, X0046 int, X0047 int, X0048 int, X0049 int, X0050 int,
        X0051 int, X0052 int, X0053 int, X0054 int, X0055 int, X0056 int, X0057 int, X0058 int, X0059 int, X0060 int,
        X0061 int, X0062 int, X0063 int, X0064 int, X0065 int, X0066 int, X0067 int, X0068 int, X0069 int, X0070 int,
        X0071 int, X0072 int, X0073 int, X0074 int, X0075 int, X0076 int, X0077 int, X0078 int, X0079 int, X0080 int,
        X0081 int, X0082 int, X0083 int, X0084 int, X0085 int, X0086 int, X0087 int, X0088 int, X0089 int, X0090 int,
        X0091 int, X0092 int, X0093 int, X0094 int, X0095 int, X0096 int, X0097 int, X0098 int, X0099 int, X0100 int,
        X0101 int, X0102 int, X0103 int, X0104 int, X0105 int, X0106 int, X0107 int, X0108 int, X0109 int, X0110 int,
        X0111 int, X0112 int, X0113 int, X0114 int, X0115 int, X0116 int, X0117 int, X0118 int, X0119 int, X0120 int,
        X0121 int, X0122 int, X0123 int, X0124 int, X0125 int, X0126 int, X0127 int, X0128 int, X0129 int, X0130 int,
        X0131 int, X0132 int, X0133 int, X0134 int, X0135 int, X0136 int, X0137 int, X0138 int, X0139 int, X0140 int,
        X0141 int, X0142 int, X0143 int, X0144 int, X0145 int, X0146 int, X0147 int, X0148 int, X0149 int, X0150 int,
        X0151 int, X0152 int, X0153 int, X0154 int, X0155 int, X0156 int, X0157 int, X0158 int, X0159 int, X0160 int,
        X0161 int, X0162 int, X0163 int, X0164 int, X0165 int, X0166 int, X0167 int, X0168 int, X0169 int, X0170 int,
        X0171 int, X0172 int, X0173 int, X0174 int, X0175 int, X0176 int, X0177 int, X0178 int, X0179 int, X0180 int,
        X0181 int, X0182 int, X0183 int, X0184 int, X0185 int, X0186 int, X0187 int, X0188 int, X0189 int, X0190 int,
        X0191 int, X0192 int, X0193 int, X0194 int, X0195 int, X0196 int, X0197 int, X0198 int, X0199 int, X0200 int
    )''')
    db.commit()


def bulk_insert_data(db, data):
    cursor = db.cursor()
    insert_query = '''insert into credit_data values (?,?,?,
        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    '''
    cursor.executemany(insert_query, data)
    db.commit()

def get_total_count(db):
    cursor = db.cursor();
    cursor.execute("select count(*) from credit_data")
    sql_count = cursor.fetchone()[0]
    return sql_count

def main():
    db = sqlite3.connect('credit.db')
    create_credit_table(db)
    credit_data = []
    print(f"Loading data...")
    for i, line in enumerate(get_lines()):
        data = process_line(line)
        credit_data.append(data)
        # insert records 10000 at a time. This ultimately reduces
        # the total memory used, at the expense of time.
        if i and not i % 10000:
            print(f"Inserting {i} records...")
            bulk_insert_data(db, credit_data)
            credit_data = []
        
    bulk_insert_data(db, credit_data)
    records = get_total_count(db)
    print(f"{records} records inserted.")


if __name__ == "__main__":
    main()