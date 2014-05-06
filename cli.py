'''
Created on April 17, 2014
  
@author: ChenMing
'''
  
from Configuration import CredentialsParser
from AccessMode import TableOpt, DataModel, SimpleOpt
from optparse import OptionParser

import boto.dynamodb2
import config

def process_args():
    usage = "usage: %prog [option1] arg1 [option2] arg2"
    parser = OptionParser(usage=usage)
    parser.add_option("-i", "--insert", dest="number_of_records",
                      type=int, action="store", metavar="int",
                      help="Insert fake items into specified table")
    parser.add_option("-d", "--delete", dest="delete", action="store_true", default=False,
                      help="Delete all items from specified table")
    parser.add_option("--delete-table", dest="delete_table", action="store_true", default=False,
                      help="Delete table and create a new table")
    parser.add_option("-l", "--list", dest="list",
                      action="store_true", default=False,
                      help="List all records in specified table")
    parser.add_option("-c", "--create", dest="if_create_table",
                      action="store_true", default=False,
                      help="Create specified table")
    parser.add_option("--count", dest="is_count",
                      action="store_true", default=False,
                      help="Count number of items for specified table")
    parser.add_option("-s", "--size", dest="size_of_record",
                      type=int, action="store", metavar="int",
                      help="Specify size of the record")
    parser.add_option("--count-page", dest="count_page",
                      action="store_true", default=False,
                      help="Count number of items inside one page")
    parser.add_option("-q", "--query", dest="query",
                      action="store_true", default=False,
                      help="Query specific record")
    parser.add_option("-u", "--update", dest="update",
                      action="store_true", default=False,
                      help="Update specific record")
    parser.add_option("-k", "--key", dest="key",
                      type=str, action="store",
                      help="key of the queried data")
    parser.add_option("--rk", "--range_key", dest="range_key",
                      type=str, action="store",
                      help="Range key of the queried data")
    parser.add_option("--scan", dest="scan",
                      action="store_true", default=False,
                      help="Scan with filter on size of record")

    parser.add_option("--log", dest="log", action="store_true", default=False,
                      help="Operation on log file table")
    parser.add_option("-p", "--package", "--pkg", dest="pkg", action="store_true",
                      default=False,
                      help="Operation on package table")
    parser.add_option("--with-status", dest="status", metavar="str",
                      action="store", type=str,
                      help="Operation on log file table")
    parser.add_option("--with-logcount", dest="log_count", metavar="str",
                      action="store", type=str,
                      help="Operation on log file table")
    parser.add_option("-a", "--account", dest="acct", default=False,
                      action="store_true",
                      help="Operation on account information table")

    return parser.parse_args()

def main():
    (options, args) = process_args()

    conn = boto.dynamodb2.connect_to_region(
        config.region
    )

    tableOpt = TableOpt.TableOpt(conn,
            config.table_name,
            options.if_create_table)

    record_size = 0
    if options.size_of_record:
        record_size = options.size_of_record

    if options.delete_table:
        tableOpt.delete_table()

    if options.number_of_records > 0:
        tableOpt.insert_records(options.number_of_records, record_size)
    elif options.delete:
        tableOpt.batch_delete_records()
    elif options.query and options.key and options.range_key:
        tableOpt.range_get(options.key, options.range_key)
    elif options.query and options.range_key:
        tableOpt.get(options.range_key)
    elif options.update and options.range_key:
        tableOpt.update(options.range_key, record_size)
    elif options.scan and options.size_of_record:
        tableOpt.scan(record_size)

    if options.list:
        tableOpt.list_records()

    if options.is_count:
        tableOpt.count_records()

    if options.count_page:
        tableOpt.count_one_page()


if __name__ == '__main__':
    main()
