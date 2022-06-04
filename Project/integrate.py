# this program loads Census ACS data using basic, slow INSERTs 
# run it with -h to see the command line options

from cgi import print_arguments
from io import StringIO
import time
import psycopg2
import argparse
import re
import csv

DBname = "postgres"
DBuser = "postgres"
DBpwd = "jiahui"

TableName = 'Visualization'
FinalDB = 'finaldb'
Datafile = "filedoesnotexist"  # name of the data file to be loaded
CreateFinalTable = True  # indicates whether the DB table should be (re)-created

def row2vals(row):
    for key in row:
        if not row[key]:
            row[key] = 0  # ENHANCE: handle the null vals
        row['County'] = row['County'].replace('\'','')  # TIDY: eliminate quotes within literals
        row['CensusTract'] = row['CensusTract'].strip()
        # print(row['CensusTract'])
    ret = f"""{row['CensusTract']}\t'{row['State']}'\t'{row['County']}'\t{row['TotalPop']}\t{row['Men']}\t{row['Women']}\t{row['Hispanic']}\t{row['White']}\t{row['Black']}\t{row['Native']}\t{row['Asian']}\t{row['Pacific']}\t{row['Citizen']}\t{row['Income']}\t{row['IncomeErr']}\t{row['IncomePerCap']}\t{row['IncomePerCapErr']}\t{row['Poverty']}\t{row['ChildPoverty']}\t{row['Professional']}\t{row['Service']}\t{row['Office']}\t{row['Construction']}\t{row['Production']}\t{row['Drive']}\t{row['Carpool']}\t{row['Transit']}\t{row['Walk']}\t{row['OtherTransp']}\t{row['WorkAtHome']}\t{row['MeanCommute']}\t{row['Employed']}\t{row['PrivateWork']}\t{row['PublicWork']}\t{row['SelfEmployed']}\t{row['FamilyWork']}\t{row['Unemployment']}"""
    return ret

# read the input data file into a list of row strings
def readdata(fname):
    print(f"readdata: reading from File: {fname}")
    with open(fname, mode="r") as fil:
        dr = csv.DictReader(fil)
        
        rowlist = []
        for row in dr:
            rowlist.append(row)

    return rowlist

# convert list of data rows into list of SQL 'INSERT INTO ...' commands
def getSQLcmnds(rowlist):
    cmdlist = []
    for row in rowlist:
        valstr = row2vals(row)
        # cmd = f"INSERT INTO {TableName} VALUES ({valstr});"
        cmdlist.append(valstr)
    return cmdlist

# connect to the database
def dbconnect():
    connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
    )
    connection.autocommit = True
    return connection

# create the target table 
# assumes that conn is a valid, open connection to a Postgres database
def createTable(conn):

    with conn.cursor() as cursor:
        cursor.execute(f"""
            DROP TABLE IF EXISTS {TableName};
            CREATE TABLE {TableName} (
                CensusTract         NUMERIC,
                State               TEXT,
                County              TEXT,
                TotalPop            INTEGER,
                Men                 INTEGER,
                Women               INTEGER,
                Hispanic            DECIMAL,
                White               DECIMAL,
                Black               DECIMAL,
                Native              DECIMAL,
                Asian               DECIMAL,
                Pacific             DECIMAL,
                Citizen             DECIMAL,
                Income              DECIMAL,
                IncomeErr           DECIMAL,
                IncomePerCap        DECIMAL,
                IncomePerCapErr     DECIMAL,
                Poverty             DECIMAL,
                ChildPoverty        DECIMAL,
                Professional        DECIMAL,
                Service             DECIMAL,
                Office              DECIMAL,
                Construction        DECIMAL,
                Production          DECIMAL,
                Drive               DECIMAL,
                Carpool             DECIMAL,
                Transit             DECIMAL,
                Walk                DECIMAL,
                OtherTransp         DECIMAL,
                WorkAtHome          DECIMAL,
                MeanCommute         DECIMAL,
                Employed            INTEGER,
                PrivateWork         DECIMAL,
                PublicWork          DECIMAL,
                SelfEmployed        DECIMAL,
                FamilyWork          DECIMAL,
                Unemployment        DECIMAL
            );	
        """)

        print(f"Created {TableName}")

# ALTER TABLE {TableName} ADD PRIMARY KEY (CensusTract);
# CREATE INDEX idx_{TableName}_State ON {TableName}(State);

def load(conn):

    with conn.cursor() as cursor:
        # print(f"Loading {len(icmdlist)} rows")
        start = time.perf_counter()

        cursor.execute(f"""
            DROP TABLE IF EXISTS newtrip;
            CREATE TABLE newtrip 
            AS SELECT trip.trip_id, trip.vehicle_id, u.route_number, u.train, u.service_key, u.direction, u.stop_time, u.x_coordinate, u.y_coordinate
            FROM trip
            JOIN unlogged_stopevent u
            ON trip.trip_id = u.trip_number
            ORDER BY trip.trip_id;

            SELECT n.trip_id, n.vehicle_id, n.route_number, n.service_key, n.x_coordinate, n.y_coordinate, u.event_no_trip , u.event_no_stop
            FROM newtrip n
            JOIN unloggedtable u
            ON n.vehicle_id = u.vehicle_id;

            DROP TABLE IF EXISTS {FinalDB};
            CREATE TABLE {FinalDB}
            AS SELECT b.tstamp, b.latitude, b.direction, b.speed, b.trip_id, t.vehicle_id, t.route_number, t.service_key, t.x_coordinate, t.y_coordinate, t.event_no_trip , t.event_no_stop
            FROM breadcrumb b
            JOIN newtrip t
            ON t.trip_id = b.trip_id;
        """)

        elapsed = time.perf_counter() - start
        print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')

def main():
    conn = dbconnect()
    # rlis = readdata(Datafile)
    # cmdlist = getSQLcmnds(rlis)

    if CreateFinalTable:
        createTable(conn)

    load(conn)


if __name__ == "__main__":
    main()



            # DROP TABLE IF EXISTS temp;
            # CREATE TABLE temp
            # AS SELECT n.trip_number, n.vehicle_number, n.route_number, n.service_key, n.x_coordinate, n.y_coordinate, u.event_no_trip, u.event_no_stop
            # FROM unlogged_stopevent n
            # JOIN unloggedtable u
            # ON n.vehicle_number = u.vehicle_id
            # ORDER BY n.trip_number;


