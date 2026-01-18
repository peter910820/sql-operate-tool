"""ioriweb-backup-tool.(postgreSQL)

Usage:
  ioriweb-backup-tool.py ([-o, --overview] | [-c, --csv_backup] | [-i, --img_backup]) <url>
  ioriweb-backup-tool.py (-h | --help)

Options:
  -h --help                 Show help.
  -o, --overview            Look the database structure.
  -c, --csv_backup          Backup all the data didn't contain the bytea datatype data to csv.
  -i, --img_backup          Backup all the data for bytea datatype to .png or .jpg.
"""

from docopt import docopt
import psycopg2, os, csv, shutil
from tqdm import tqdm, trange

arguments = docopt(__doc__, options_first=True)

# nums = (int(num) for num in arguments['<num>'])

DATABASE_URL = arguments['<url>']

def overview(DATABASE_URL):
    try:
        db = psycopg2.connect(DATABASE_URL)
    except:
        return 'database connect failed.'
    try:
        cursor = db.cursor()
        cursor.execute("SELECT relname FROM pg_class WHERE relkind='r' and relname !~ '^(pg_|sql_)';")
        cur = cursor.fetchall()
        for c in cur:
            print(c[0])
            cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{c[0]}' ORDER BY ordinal_position;")
            data_field = cursor.fetchall()
            for d in data_field:
                print(f'  Filed: {d[0]}\n  Datatype: {d[1]}')
        cursor.close()
        return 0
    except:
        return 'an error has occur.'
  
def csv_backup(DATABASE_URL):
    if os.path.isdir('./backup/csv'):
        shutil.rmtree('./backup/csv')
    try:
        db = psycopg2.connect(DATABASE_URL)
    except:
        return 'database connect failed.'
    try:
        cursor = db.cursor()
        cursor.execute("SELECT relname FROM pg_class WHERE relkind='r' and relname !~ '^(pg_|sql_)';")
        cur = cursor.fetchall()

        progress = tqdm(total=len(cur),colour='blue')

        os.makedirs('./backup/csv', exist_ok=True)
        for c in cur:
            cursor.execute(f"SELECT * FROM {c[0]};")
            data = cursor.fetchall()
            with open(f'./backup/csv/{c[0]}.csv','w', encoding='utf-8-sig') as file:
                writer = csv.writer(file)
                for d in data:
                    writer.writerow(d)
            file.close()
            progress.set_description(f"Download database to csv: {c[0]}")
            progress.update(1)
        cursor.close()
        return 0
    except:
        return 'an error has occur.'
  
def img_backup(DATABASE_URL):
    if os.path.isdir('./backup/img'):
        shutil.rmtree('./backup/img')
    try:
        db = psycopg2.connect(DATABASE_URL)
    except:
        return 'database connect failed.'
    try:
        cursor = db.cursor()
        cursor.execute("SELECT relname FROM pg_class WHERE relkind='r' and relname !~ '^(pg_|sql_)';")
        cur = cursor.fetchall()

        progress = tqdm(total=len(cur),colour='blue')

        os.makedirs('./backup/img', exist_ok=True)
        for c in cur:
            cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{c[0]}' ORDER BY ordinal_position;")
            data_type = cursor.fetchall()
            title = data_type[0][0]
            for d in data_type:
                if d[1] == 'bytea':
                    cursor.execute(f"SELECT {title}, {d[0]} FROM {c[0]};")
                    image = cursor.fetchall()
                    os.makedirs(f'./backup/img/{c[0]}', exist_ok=True)
                    for i in image:
                        file = open(f"./backup/img/{c[0]}/{i[0]}","wb")
                        file.write(i[1])
                        file.close
            progress.set_description(f"Download database to img: {c[0]}")
            progress.update(1)
        cursor.close()
        return 0
    except:
        return 'an error has occur.'
  
if arguments['--overview']:
    status = overview(DATABASE_URL)
    if status == 0:
        print('The program has exited with code 0')
    else:
        print(status)
elif arguments['--csv_backup']:
    status = csv_backup(DATABASE_URL)
    if status == 0:
        print('The program has exited with code 0')
    else:
        print(status)
elif arguments['--img_backup']:
    status = img_backup(DATABASE_URL)
    if status == 0:
        print('The program has exited with code 0')
    else:
        print(status)
