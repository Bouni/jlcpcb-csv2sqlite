import sys
import codecs
import contextlib
import csv
import os
import re
import sqlite3
import zipfile
from datetime import datetime as dt

class CSV2SQlite:
    """Convert JLCPCBs CSV catalog to SQlite"""

    def __init__(self, filename):
        self.csv_file = filename
        self.dbfile = "parts.db"
        # self.convert()
        self.zipdb()

    def delete_parts_table(self):
        """Delete the parts table."""
        with contextlib.closing(sqlite3.connect(self.dbfile)) as con:
            with con as cur:
                cur.execute("DROP TABLE IF EXISTS parts")
                cur.commit()

    def create_meta_table(self):
        """Create the meta table."""
        with contextlib.closing(sqlite3.connect(self.dbfile)) as con:
            with con as cur:
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS meta ('filename', 'size', 'partcount', 'date', 'last_update')"
                )
                cur.commit()

    def create_rotation_table(self):
        """Create the rotation table."""
        with contextlib.closing(sqlite3.connect(self.dbfile)) as con:
            with con as cur:
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS rotation ('regex', 'correction')"
                )
                cur.commit()


    def create_mapping_table(self):
        """Create the mapping table."""
        with contextlib.closing(sqlite3.connect(self.dbfile)) as con:
            with con as cur:
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS mapping ('footprint', 'value', 'LCSC')"
                )
                cur.commit()

    def update_meta_data(self, filename, size, partcount, date, last_update):
        """Update the meta data table."""
        with contextlib.closing(sqlite3.connect(self.dbfile)) as con:
            with con as cur:
                cur.execute("DELETE from meta")
                cur.commit()
                cur.execute(
                    "INSERT INTO meta VALUES (?, ?, ?, ?, ?)",
                    (filename, size, partcount, date, last_update),
                )
                cur.commit()

    def create_parts_table(self, columns):
        """Create the parts table."""
        with contextlib.closing(sqlite3.connect(self.dbfile)) as con:
            with con as cur:
                cols = ",".join([f" '{c}'" for c in columns])
                cur.execute(f"CREATE TABLE IF NOT EXISTS parts ({cols})")
                cur.commit()


    def convert(self):
        """The actual worker thread that downloads and imports the CSV data."""
        print("Convert CSV to SQlite")
        size = os.stat(self.csv_file).st_size
        date = "unknown"
        _date = re.search(r"(\d{4})(\d{2})(\d{2})", self.csv_file)
        if _date:
            date = f"{_date.group(1)}-{_date.group(2)}-{_date.group(3)}"
        with codecs.open(self.csv_file, encoding="gbk") as csvfile:
            csv_reader = csv.reader(csvfile)
            headers = next(csv_reader)
            self.create_tables(headers)
            buffer = []
            part_count = 0
            with contextlib.closing(sqlite3.connect(self.dbfile)) as con:
                cols = ",".join(["?"] * len(headers))
                query = f"INSERT INTO parts VALUES ({cols})"
                for count, row in enumerate(csv_reader):
                    row.pop()
                    buffer.append(row)
                    if count % 1000 == 0:
                        con.executemany(query, buffer)
                        buffer = []
                    part_count = count
                if buffer:
                    con.executemany(query, buffer)
                con.commit()
            self.update_meta_data(self.csv_file, size, part_count, date, dt.now().isoformat())

    def zipdb(self):
        """Zip the parts.db to reduce its size from ~490MB to 57MB."""
        print("Compress SQlite to ZIP")
        with zipfile.ZipFile('parts.zip', 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
            archive.write('parts.db')

    def create_tables(self, headers):
        """Create all necessary tables."""
        self.create_meta_table()
        self.delete_parts_table()
        self.create_parts_table(headers)
        self.create_rotation_table()
        self.create_mapping_table()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Pass csv filename as argument!")
    csv2sqlite = CSV2SQlite(sys.argv[1])
