# this program loads Census ACS data using basic, slow INSERTs 
# run it with -h to see the command line options

from resource import RLIMIT_STACK
import time
import psycopg2
import argparse
import re
import csv

import psycopg2.extras

DBname = "db_test"
DBuser = "postgres"
DBpwd = "7521735"
TableName = 'CensusData'
Datafile = "filedoesnotexist"  # name of the data file to be loaded
CreateDB = False  # indicates whether the DB table should be (re)-created

def row2vals(row):
    for key in row:
        if not row[key]:
            row[key] = 0  # ENHANCE: handle the null vals
        row['County'] = row['County'].replace('\'','')  # TIDY: eliminate quotes within literals

    ret = f"""
       {row['CensusTract']},            -- CensusTract
       '{row['State']}',                -- State
       '{row['County']}',               -- County
       {row['TotalPop']},               -- TotalPop
       {row['Men']},                    -- Men
       {row['Women']},                  -- Women
       {row['Hispanic']},               -- Hispanic
       {row['White']},                  -- White
       {row['Black']},                  -- Black
       {row['Native']},                 -- Native
       {row['Asian']},                  -- Asian
       {row['Pacific']},                -- Pacific
       {row['Citizen']},                -- Citizen
       {row['Income']},                 -- Income
       {row['IncomeErr']},              -- IncomeErr
       {row['IncomePerCap']},           -- IncomePerCap
       {row['IncomePerCapErr']},        -- IncomePerCapErr
       {row['Poverty']},                -- Poverty
       {row['ChildPoverty']},           -- ChildPoverty
       {row['Professional']},           -- Professional
       {row['Service']},                -- Service
       {row['Office']},                 -- Office
       {row['Construction']},           -- Construction
       {row['Production']},             -- Production
       {row['Drive']},                  -- Drive
       {row['Carpool']},                -- Carpool
       {row['Transit']},                -- Transit
       {row['Walk']},                   -- Walk
       {row['OtherTransp']},            -- OtherTransp
       {row['WorkAtHome']},             -- WorkAtHome
       {row['MeanCommute']},            -- MeanCommute
       {row['Employed']},               -- Employed
       {row['PrivateWork']},            -- PrivateWork
       {row['PublicWork']},             -- PublicWork
       {row['SelfEmployed']},           -- SelfEmployed
       {row['FamilyWork']},             -- FamilyWork
       {row['Unemployment']}            -- Unemployment
    """

    return ret


def initialize():
  parser = argparse.ArgumentParser()
  parser.add_argument("-d", "--datafile", required=True)
  parser.add_argument("-c", "--createtable", action="store_true")
  args = parser.parse_args()

  global Datafile
  Datafile = args.datafile
  global CreateDB
  CreateDB = args.createtable

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
        # cmd = valstr
        cmdlist.append(valstr)
    return cmdlist

def getcmd(rowlist):
    # cmdlist = []
    for row in rowlist:
        valstr = row2vals(row)
        # cmd = f"INSERT INTO {TableName} VALUES ({valstr});"
    return valstr

# connect to the database
def dbconnect():
    connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
    )
    # commit out for PART E. Disabling Autocommit.
    # connection.autocommit = True

    return connection

# create the target table 
# assumes that conn is a valid, open connection to a Postgres database
def createTable(conn):

    with conn.cursor() as cursor:
        cursor.execute(f"""
            SET temp_buffers='256MB';
            CREATE TEMPORARY TABLE temptable(
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

#ALTER TABLE {TableName} ADD PRIMARY KEY (CensusTract);
#CREATE INDEX idx_{TableName}_State ON {TableName}(State);

def load(conn, icmdlist,rlis):

    with conn.cursor() as cursor:
        print(f"Loading {len(icmdlist)} rows")
        start = time.perf_counter()
    
        # for cmd in icmdlist:
            # cursor.execute(cmd)
        all_rlist = ({
            **r,
        } for r in rlis)

        
        print("HERE")
        # print(icmdlist)

        psycopg2.extras.execute_batch(cursor,"""
            INSERT INTO temptable VALUES(
                %(CensusTract)s,
                %(State)s,
                %(County)s,
                %(TotalPop)s, 
                %(Men)s,        
                %(Women)s,            
                %(Hispanic)s,            
                %(White)s,           
                %(Black)s,       
                %(Native)s,           
                %(Asian)s,           
                %(Pacific)s,           
                %(Citizen)s,           
                %(Income)s,             
                %(IncomeErr)s, 
                %(IncomePerCap)s,    
                %(IncomePerCapErr)s,
                %(Poverty)s,
                %(ChildPoverty)s,
                %(Professional)s,
                %(Service)s,
                %(Office)s,
                %(Construction)s,
                %(Production)s,
                %(Drive)s,
                %(Carpool)s,
                %(Transit)s,
                %(Walk)s,
                %(OtherTransp)s,
                %(WorkAtHome)s,
                %(MeanCommute)s,
                %(Employed)s,
                %(PrivateWork)s,
                %(PublicWork)s,
                %(SelfEmployed)s,
                %(FamilyWork)s,
                %(Unemployment)s
            );
        """,all_rlist) 

        cursor.execute(f"""
            INSERT INTO {TableName} SELECT * FROM temptable;
            ALTER TABLE {TableName} ADD PRIMARY KEY (CensusTract);
            CREATE INDEX idx_{TableName}_State ON {TableName}(State);
        """)

        elapsed = time.perf_counter() - start
        print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')


def main():
    initialize()
    conn = dbconnect()
    rlis = readdata(Datafile)
    cmdlist = getSQLcmnds(rlis)
    # cmd = getcmd(rlis)

    if CreateDB:
        createTable(conn)

    load(conn, cmdlist,rlis)


if __name__ == "__main__":
    main()
