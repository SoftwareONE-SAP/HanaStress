import util.miscUtils
import util.databaseUtils
import util.printUtil

class SchemaDestroyer:

    def __init__(self, options):
        self.options = options
        self.printUtil = util.printUtil.PrintUtil(options.verbose)
        self.miscUtils = util.miscUtils.MiscUtils()
        self.dbutil = self.miscUtils.getDBConnection(self.options)

    def destroy(self):
        self.schemas = self.dbutil.select("SELECT * FROM \"SYS\".\"SCHEMAS\" WHERE \"SCHEMA_OWNER\" = '" + self.options.user + "' AND \"SCHEMA_NAME\" <> '" + self.options.user + "'")

        for schema in self.schemas:
            self.printUtil.debug("dropping", "Dropping schema: '" + schema[0] + "'")
            self.dbutil.query("DROP SCHEMA \"" + schema[0] + "\" CASCADE")
