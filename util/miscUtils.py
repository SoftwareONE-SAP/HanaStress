import random
import string
import decimal
import datetime
import util.databaseUtils

class MiscUtils:

    def getRandomString(self, len):
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(len))

    def getRandomDate(self):
        return datetime.datetime(random.randint(1901, 9999),random.randint(1, 12),random.randint(1, 28)).strftime('%Y-%m-%d %H:%M:%S')

    def getRandomDecimal(self):
        return decimal.Decimal(random.randrange(10000))/100

    def getDBConnection(self, options):
        dbutil = util.databaseUtils.DatabaseUtils(options.verbose)
        if not dbutil.connect(options.host, options.instance, options.user, options.password):
            sys.exit(1)

        return dbutil