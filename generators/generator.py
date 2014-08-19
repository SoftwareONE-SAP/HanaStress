import util.databaseUtils
import util.printUtil
import util.miscUtils
import random
import sys
import thread

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
        self.threadsDone = 0;

        master_table = "HIERARCHY_MASTER"
        self.createMasterTable(master_table)

        last_table = master_table

        for i in range(0, self.tables):
            table_name = "HIERARCHY_" + str(i)

            self.printUtil.debug("creating", "Creating table: '" + table_name + "' with master of: '" + last_table + "'");
            self.dbutil.createRelationalTable(self.schema_name, table_name, self.store_type, last_table)

            # Input our first row so we can branch off it
            self.dbutil.insertRandomData(self.schema_name, table_name, [0, 0, "DATE", "DECIMAL", "VARCHAR"])

            # thread.start_new_thread( self.addRows, ("thread-" + str(i), self.schema_name, table_name, ["INT", 0, "DATE", "DECIMAL", "VARCHAR"], self.rows) )
            
            last_table = table_name

    def generateRelationalDatabase(self):
        self.threadsDone = 0

        master_table = "RELATIONSHIP_MASTER"
        self.createMasterTable(master_table)

        for i in range(0, self.tables):
            # Create a random name for the table (and assume it is unique)
            table_name = "RELATIONAL_" + self.miscUtils.getRandomString(16)

            # Create sales table
            self.dbutil.createRelationalTable(self.schema_name, table_name, self.store_type, master_table)

            # Add dummy data to table
            thread.start_new_thread( self.addRows, ("thread-" + str(i), self.schema_name, table_name, ["INT", 0, "DATE", "DECIMAL", "VARCHAR"], self.rows) )
        
        while self.threadsDone < self.tables:
            pass 

    def createMasterTable(self, master_table):
        # Make master table for other tables to join onto
        self.printUtil.debug("creating", "Creating master table for relational mapping in schema: '" + self.schema_name + "'...")
        self.dbutil.createMasterTable(self.schema_name, master_table, self.store_type)
        self.dbutil.query("INSERT INTO \"" + self.schema_name + "\".\"" + master_table + "\" VALUES (" + str(0) + ", " + str(3) + ", " + str(6) + ", " + str(4) + ")")

    def generateAnarchicalDatabase(self):
        self.threadsDone = 0

        for i in range(0, self.tables):
            table_name = "ANARCHY_" + self.miscUtils.getRandomString(16)

            # Create sales table
            self.dbutil.createAnarchyTable(self.schema_name, table_name, self.store_type)

            # Add dummy data to table
            thread.start_new_thread( self.addRows, ("thread-" + str(i), self.schema_name, table_name, ["INT", "INT", "DATE", "INT", "DECIMAL", "DECIMAL"], self.rows) )
        
        while self.threadsDone < self.tables:
            pass 
    
    def addRows(self, threadname, schema_name, table_name, columns, rows):
        self.printUtil.debug(threadname, "Starting insert of " + str(self.rows) + " rows into table: " + '"' + self.schema_name + '"."' + table_name + '"' + " ...")

        insertDbutil = self.miscUtils.getDBConnection(self.options)

        for x in xrange(0, rows / 1000):
            # self.printUtil.debug(threadname, "Inserting another 1000 rows into table: " + '"' + self.schema_name + '"."' + table_name + '"' + " ...")
            insertDbutil.insertRandomData(schema_name, table_name, columns)

        insertDbutil.disconnect()

        self.printUtil.debug(threadname, "Done inserting rows into: '" + schema_name + "'.'" + table_name + "'")
        self.threadsDone += 1
