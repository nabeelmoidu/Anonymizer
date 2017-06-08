'''
Sanitize sensitive data and 
write to downstream instances for BI queries

'''
#!/usr/bin / env python

import pymysql
import hashlib
from itertools import chain
import time
import yaml


class SanitizeDataStream :
    def __init__(self,rules_file):
        try:
            rules_list = yaml.load(open(rules_file))
        except ValueError, e:
            print "Corrupted configuration file %s. \n Cannot open list of hash tables. \n Exiting" % rules_file
            sys.exit()

        self.rulelist = {}
        for lists in rules_list:
            # since rule file is listed with action first and table/column next, need
            # to reverse the k/v pair and set the table/column as key
            ruleset = dict(reversed(rules.split(" ")) for rules in fields_list[lists])
            self.rulelist.update(ruleset)

    def anonymize(self, key, value, table, database):
        params = self.rulelist.split('.')
        # there might be only table specified, but we need exactly two args,
        # add None as a table
        if len(params) == 1:
            params.append(None)
        # now there should be exactly two args.
        # TODO add proper error message
        if len(params) != 2:
            raise TypeError
        self.skip = {'db': params[0], 'table': params[1]}

        if table in self.rulelist:







class HashColumnFilter(TransformColumnFilter):
    def transform(self, value):
        if value is None:
            return value
        m = hashlib.md5(value.encode('utf-8'))
        return m.hexdigest()[0:12]

    def apply_clause(self):
        return "MID(MD5(`%s`),1,12)" % self.config['column']


class HashEmailFilter(TransformColumnFilter):
    def transform(self, value):
        if value is None:
            return value
        if '@' in value:
            name, domain = value.split('@',1)
        else:
            name, domain = value, ''

        name_hash = hashlib.md5(name.encode('utf-8')).hexdigest()[0:12]
        return "%s@%s" % (name_hash, domain)

    def apply_clause(self):
        return "CONCAT(MID(MD5(SUBSTRING_INDEX(`%s`,'@',1)),1,12), '@', SUBSTRING_INDEX(`%s`,'@',-1))" % (self.config['column'], self.config['column'])


class HashPhoneFilter(TransformColumnFilter):
    def transform(self,value):
        if value is None:
            return value
        value = zlib.crc32(value.encode('utf-8')) & 0xfffffff
        return "%010d" % value

    def apply_clause(self):
        return "LPAD(CRC32(`%s`),10,'0')" % self.config['column']

