import json

class PrintUtil:

    def __init__(self, isDebug):
        self.isDebug = isDebug

    def debug(self, type, message):
        if self.isDebug:
            print '\033[95m' + "[" + type + "] " + message + '\033[0m'

    def log(self, type, message):
        print "[" + type + "] " + message 

    def warn(self, type, message):
        print '\033[93m' + "[" + type + "] " + message + '\033[0m'

    def err(self, type, message):
        print '\033[91m' + "[" + type + "] " + message + '\033[0m'

    def tuples(self, obj):
        print json.dumps([list(row) for row in obj], sort_keys=True, indent=4, separators=(',', ': '))

    def json(self, obj):
        print json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))

    def raw(self, obj):
        print obj
