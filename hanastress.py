#!/opt/hanastress/hanapython
import util.printUtil
import generators.generator
import destroyers.schemaDestroyer
from optparse import OptionParser
import sys

class HanaStress:

    def __init__(self):
        self.getCommand()

    def setCommands(self, parser):
        parser.add_option("-v", "--verbose", help="Give detailed messages", action="store_true")
        parser.add_option("-l", "--host", help="The hostname of the DB instance")
        parser.add_option("-i", "--instance", help="The DB instance to connect to")
        parser.add_option("-u", "--user", help="The user to use for the DB connection")
        parser.add_option("-p", "--password", help="The password to use for the DB connection")
        parser.add_option("-g", "--generate", help="Generate a schema. Usage: --create {SCHEMA_TYPE}. Can be: ")
        parser.add_option("-t", "--tables", help="Used with '--generate'. The amount of tables to create. Default: 50", default=50)
        parser.add_option("-s", "--rowstorage", help="Used with '--generate', Will set table type to Row Store instead of Column Store", action="store_true")
        parser.add_option("-r", "--rows", help="Used with '--generate', set the amount of rows for each table. Default: 100", default=100)
        parser.add_option("--destroy", help="This will DESTROY all the schemas owned by the given user (expect their default schema). BE CAREFUL!", action="store_true")

    def getCommand(self):
        # create an option parser
        parser = OptionParser()

        # Set out commands
        self.setCommands(parser)

        # Get the options passed in by the user
        options, args = parser.parse_args()

        # Should be output verbose messages
        self.printUtil = util.printUtil.PrintUtil(options.verbose)

        # Conver the username to upper case
        if options.user:
            options.user = options.user.upper()

        # Make sure we never run as system
        if options.user == "SYSTEM":
            self.printUtil.err("INVALID", "CAN NOT RUN AS 'SYSTEM' USER, THIS PROGRAM CAN BE VERY DESTRUCTIVE FOR THE USER GIVEN. CREATE A NEW USER FOR THIS TO RUN AS.")
            sys.exit(1)

        self.runCommands(options)

    def runCommands(self, options):

        if options.generate:
            store_type = "row"

            if not options.rowstorage:
                store_type = "column"

            self.generator = generators.generator.Generator(options, int(options.tables), store_type, int(options.rows))
            self.generator.generate()

        elif options.destroy:
            self.destroyer = destroyers.schemaDestroyer.SchemaDestroyer(options)
            self.destroyer.destroy()

# Run our main class
hanastress = HanaStress()