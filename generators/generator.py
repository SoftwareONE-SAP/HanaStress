import util.databaseUtils
import util.printUtil
import util.miscUtils
import random
import sys
import thread
import time

class Generator:

    def __init__(self, options, tables, store_type, rows):
        self.options = options
        self.printUtil = util.printUtil.PrintUtil(options.verbose)
        self.miscUtils = util.miscUtils.MiscUtils()
        self.user = options.user
        self.dbutil = self.miscUtils.getDBConnection(options)
        self.relationship = options.generate
        self.tables = tables
        self.store_type = store_type
        self.rows = int(rows)
        self.threadsDone = 0

    def generate(self):
        self.printUtil.debug("info", "Starting Generation...");

        self.relationship = self.relationship.upper()

        if self.relationship not in ["ANARCHY", "RELATIONAL", "HIERARCHY"]:
            self.printUtil.warn("Invalid", 'Relationships available: "ANARCHY", "RELATIONAL", "HIERARCHY". You entered: "' + self.relationship + '"')
            return

        # Randomly create a schema name
        self.schema_name = "TEST_" + self.relationship + "_" + self.miscUtils.getRandomString(16)

        # Create schema
        self.dbutil.createSchema(self.schema_name, self.user)

        if self.relationship == "ANARCHY":
            self.generateAnarchicalDatabase()
        elif self.relationship == "RELATIONAL":
            self.generateRelationalDatabase()
        elif self.relationship == "HIERARCHY":
            self.generateHierarchicalDatabase()

        self.dbutil.disconnect()
        return True

    def generateHierarchicalDatabase(self):
        # An Array of all the tables we have made
        tables = []

        # Start creating tables
        for i in range(0, self.tables):
            table_name = "HIERARCHY_" + str(i)
            tables += [table_name]

            # Create table
            self.printUtil.debug("structure", 'Creating table: "' + self.schema_name + '"."' + table_name + '"')
            self.dbutil.createHierarchyTable(self.schema_name, table_name, self.store_type)

            # Create a sequence for our unique key
            self.printUtil.debug("structure", 'Creating id sequence: "TEST_NEXTID_' + str(i) + '"')
            self.dbutil.createSequence(self.schema_name, "TEST_NEXTID_" + str(i))

        # Start adding data with our sequences
        self.threadsDone = 0
        self.activeThreads = 0

        count = 0
        while self.threadsDone < self.tables:
            if count < self.tables and self.activeThreads < self.options.threads:
                self.activeThreads += 1
                # "(SELECT "' + self.schema_name + '"."TEST_NEXTID_' + str(count) + '".NEXTVAL FROM DUMMY)"
                thread.start_new_thread(self.addAnarchyRows, ("thread-" + str(count), self.schema_name, tables[count], ['"' + self.schema_name + '"."TEST_NEXTID_' + str(count) + '".NEXTVAL', "INT", "DATE", "INT", "DECIMAL", "DECIMAL"], self.rows))
                count += 1
            else:
                time.sleep(0.1)

        for i in range(0, self.tables):
            self.printUtil.debug("structure", 'Dropping id sequence: "TEST_NEXTID_' + str(i) + '"')
            self.dbutil.dropSequence(self.schema_name, "TEST_NEXTID_" + str(i))

        for i in range(0, self.tables - 1):
            self.printUtil.debug("structure", 'Building constraint for "' + self.schema_name + '"."' + tables[i + 1] + '" REFRENCES "' + self.schema_name + '"."' + tables[i] + '"')
            self.dbutil.addForiegnKey(self.schema_name, tables[i + 1], tables[i], "TEST_HIERARCHY_CONSTRAINT_" + str(i), "TEST_INT1")


    def generateRelationalDatabase(self):
        self.threadsDone = 0
        self.activeThreads = 0

        # An Array of all the tables we have made
        tables = []

        master_table = "RELATIONSHIP_MASTER"
        self.createMasterTable(master_table)

        count = 0    
        while self.threadsDone < self.tables:
            if self.activeThreads < self.options.threads:
                count += 1
                self.activeThreads += 1
                # Create a random name for the table (and assume it is unique)
                table_name = "RELATIONAL_" + self.miscUtils.getRandomString(16)

                tables += [table_name]
                thread.start_new_thread(self.createRelationalTable, ("thread-" + str(count), self.schema_name, table_name, self.store_type, master_table))
            else:
                time.sleep(0.1)

        self.threadsDone = 0
        self.activeThreads = 0
        count = 0
        while self.threadsDone < self.rows:
            if self.activeThreads < self.options.threads:
                self.activeThreads += 1
                thread.start_new_thread(self.addRelationalRows, ("thread-" + str(count), self.schema_name, [count, count, "DATE", "DECIMAL", "VARCHAR"], master_table, tables, count))
                count += 1
            else:
                time.sleep(0.1)

    def createRelationalTable(self, threadname, schema_name, table_name, store_type, master_table):
        insertDbutil = self.miscUtils.getDBConnection(self.options)

        # Create relational table
        insertDbutil.createRelationalTable(schema_name, table_name, store_type, master_table)
        self.printUtil.debug(threadname, "Created: '" + self.schema_name + "'.'" + table_name + "'")

        insertDbutil.disconnect()

        self.activeThreads -= 1
        self.threadsDone += 1

    def createMasterTable(self, master_table):
        # Make master table for other tables to join onto
        self.printUtil.debug("structure", "Creating master table for relational mapping in schema: '" + self.schema_name + "'...")
        self.dbutil.createMasterTable(self.schema_name, master_table, self.store_type)

    # Generate tables that have nothing todo with eachother and then fill them with info
    def generateAnarchicalDatabase(self):
        for i in range(0, self.tables):
            table_name = "ANARCHY_" + self.miscUtils.getRandomString(16)

            # Create sales table
            self.dbutil.createAnarchyTable(self.schema_name, table_name, self.store_type)

        
        self.threadsDone = 0
        self.activeThreads = 0
        count = 0
        while self.threadsDone < self.tables:
            if self.activeThreads < self.options.threads:
                self.activeThreads += 1
                count += 1
                # Add dummy data to table
                thread.start_new_thread( self.addAnarchyRows, ("thread-" + str(count), self.schema_name, table_name, ["INT", "INT", "DATE", "INT", "DECIMAL", "DECIMAL"], self.rows) )
            else:
                time.sleep(0.1)

    def addAnarchyRows(self, threadname, schema_name, table_name, columns, rows):
        self.printUtil.debug(threadname, "Starting insert of " + str(self.rows) + " rows into table: " + '"' + self.schema_name + '"."' + table_name + '"' + " ...")

        insertDbutil = self.miscUtils.getDBConnection(self.options)

        insertDbutil.insertRandomData(schema_name, table_name, columns, rows)

        insertDbutil.disconnect()

        self.printUtil.debug(threadname, "Done inserting rows into: '" + schema_name + "'.'" + table_name + "'")

        self.activeThreads -= 1
        self.threadsDone += 1

    def addRelationalRows(self, threadname, schema_name, columns, master_table, tables, count):
        insertDbutil = self.miscUtils.getDBConnection(self.options)

        insertDbutil.insertRandomData(self.schema_name, master_table, [count, "INT", "INT", "INT"], 1)

        for table in tables:
            # Add dummy data to table
            self.printUtil.debug(threadname, "Adding data to '" + self.schema_name + "'.'" + table + "'")
            insertDbutil.insertRandomData(self.schema_name, table, columns, 1)

        insertDbutil.disconnect()
        self.activeThreads -= 1
        self.threadsDone += 1
