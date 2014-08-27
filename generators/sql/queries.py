class Queries:

    def createAnarchyTable(self, schema_name, table_name, store_type):
        return '\
        create ' + store_type + ' table "' + schema_name + '"."' + table_name + '"( "TEST_INT1" INTEGER not null default 0,\
            "TEST_INT2" INTEGER not null default 0,\
            "TEST_DATE" DATE not null,\
            "TEST_INT3" INTEGER null default -1,\
            "TEST_DECIMAL" DECIMAL not null,\
            "TEST_DECIMAL2" DECIMAL not null default 0.00)'

    def createHierarchyTable(self, schema_name, table_name, store_type):
        return '\
        create ' + store_type + ' table "' + schema_name + '"."' + table_name + '"(\
            "TEST_INT1" INTEGER not null default 0,\
            "TEST_INT2" INTEGER not null default 0,\
            "TEST_DATE" DATE not null,\
            "TEST_INT3" INTEGER null default -1,\
            "TEST_DECIMAL" DECIMAL not null,\
            "TEST_DECIMAL2" DECIMAL not null default 0.00,\
            PRIMARY KEY (TEST_INT1)\
            )'

    def dropSchema(self, schema_name):
        return 'DROP SCHEMA "' + schema_name + '" CASCADE'

    def createSchema(self, schema_name, user):
        return "CREATE SCHEMA \"" + schema_name + "\" OWNED BY " + user

    def getDatabaseSize(self, schema_name):
        return 'SELECT\
            ROUND(SUM(PAGE_SIZE * USED_BLOCK_COUNT)/1024/1024/1024,2) as "ROW_STORE_SIZE_GB"\
        FROM\
            "SYS"."M_DATA_VOLUME_PAGE_STATISTICS"\
        WHERE\
            PAGE_SIZECLASS = \'16k-RowStore\''

    def addForiegnKey(self, schema_name, table_name1, table_name2, keyname, column):
        return 'ALTER TABLE "' + schema_name + '"."' + table_name1 + '"\
        ADD CONSTRAINT "' + keyname + '" FOREIGN KEY ("' + column + '")\
        REFERENCES "' + schema_name + '"."' + table_name2 + '" ON DELETE CASCADE'

    def createMasterTable(self, schema_name, table_name, store_type):
        return 'create ' + store_type + ' table "' + schema_name + '"."' + table_name + '"(\
            "ID" INTEGER null,\
            "TEST1" INTEGER null,\
            TEST_DATE DATE not null,\
            TEST_DECIMAL DECIMAL not null,\
            TEST_VARCHAR VARCHAR (1000) not null,\
            PRIMARY KEY (ID)\
        )'

    def createRelationalTable(self, schema_name, table_name, store_type, master_table):
        return 'CREATE ' + store_type + ' TABLE "' + schema_name + '"."' + table_name + '" (\
            ID INT not null default 0,\
            MASTER_ID INT not null,\
            TEST_DATE DATE not null,\
            TEST_DECIMAL DECIMAL not null,\
            TEST_VARCHAR VARCHAR (1000) not null,\
            FOREIGN KEY (MASTER_ID) REFERENCES "' + schema_name + '"."' + master_table + '",\
            PRIMARY KEY (ID)\
        )'

    def insertRandomDataBulk(self, schema_name, table_name, columns, amount):
        query = 'INSERT INTO "' + schema_name + '"."' + table_name + '" ( SELECT TOP ' + str(amount) + ' '

        # Get some random data for each column
        for col in columns:
            if col == "VARCHAR":
                query += "CAST(RAND() AS VARCHAR), "

            elif col == "INT":
                query += "CAST(RAND() * 10000 AS INTEGER), "

            elif col == "DATE":
                query += "NOW(), "

            elif col == "DECIMAL":
                query += "RAND(), "
            else:
                query += str(col) + ", "

        # Remove last comma
        query = query[:-2]

        # Close insert
        query += " FROM OBJECTS CROSS JOIN OBJECTS )"

        return query