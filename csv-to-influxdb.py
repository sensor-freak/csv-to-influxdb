import requests
import gzip
import argparse
import csv
import datetime
import re
import copy
from pytz import timezone

from influxdb import InfluxDBClient

epoch_naive = datetime.datetime.utcfromtimestamp(0)
epoch = timezone('UTC').localize(epoch_naive)

# A map of well known column names
wellknownnames = {
    r'District(\w+):Facility.*': r'\1',
    r'.*Zone\s+((\w+\s+)*)Total\s+((\w+\s+)*)Energy.*\[(.*)\].*': r'\1\3',
    r'.*Zone\s+((\w+\s+)*)\[(.*)\].*': r'\1',
    r'.*:Site\s+((\w+\s+)*)\[(.*)\].*': r'\1'
}


def unix_time_millis(dt):
    return int((dt - epoch).total_seconds() * 1000)

##
## Check if data type of field is float
##
def isfloat(value):
        try:
            float(value)
            return True
        except:
            return False

def isbool(value):
    try:
        return value.lower() in ('true', 'false')
    except:
        return False

def str2bool(value):
    return value.lower() == 'true'

##
## Check if data type of field is int
##
def isinteger(value):
        try:
            if(float(value).is_integer()):
                return True
            else:
                return False
        except:
            return False


def loadCsv(inputfilename, servername, user, password, dbname, metric, 
    timecolumn, timeformat, tagcolumns, fieldcolumns, usegzip, 
    delimiter, batchsize, create, datatimezone, usessl, useautofields):

    host = servername[0:servername.rfind(':')]
    port = int(servername[servername.rfind(':')+1:])
    client = InfluxDBClient(host, port, user, password, dbname, ssl=usessl)

    if(create == True):
        print('Deleting database %s'%dbname)
        client.drop_database(dbname)
        print('Creating database %s'%dbname)
        client.create_database(dbname)

    client.switch_user(user, password)

    # format tags and fields
    if tagcolumns:
        tagcolumns = tagcolumns.split(',')
    if fieldcolumns:
        fieldcolumns = fieldcolumns.split(',')

    # open csv
    datapoints = []
    inputfile = open(inputfilename, 'r')
    count = 0
    with inputfile as csvfile:
        reader = csv.DictReader(csvfile, delimiter=delimiter)

        #reader.fieldnames[1] = 'Test'
        readernames = [value for value in reader.fieldnames]

        # Extract some tags from the headers of the data (KIT specific)
        tmpnames = {}
        for idx, name in enumerate(reader.fieldnames):
            match = re.match(r"\s*\((.*?)\)", name)
            if match:
                tmpnames['building'] = match.group(1)
                reader.fieldnames[idx] = re.sub(match.re, '', reader.fieldnames[idx])
            match = re.search(r"\((.*?)\)\s*$", name)
            if match:
                tmpnames['resolution'] = match.group(1)
                reader.fieldnames[idx] = re.sub(match.re, '', reader.fieldnames[idx])

            # Map this name to a different (shorter) name
            for regex, repl in wellknownnames.items():
                match = re.match(regex, name)
                if match:
                    reader.fieldnames[idx] = re.sub(match.re, repl, name)

            # Trim the string
            reader.fieldnames[idx] = reader.fieldnames[idx].strip(' :')

        # If no fields are specified on command line, use all
        # columns as fields (except time column)
        if not fieldcolumns:
            fieldcolumns = [x for x in reader.fieldnames]
            fieldcolumns.remove(timecolumn)

        for row in reader:
            datetime_naive = datetime.datetime.strptime(row[timecolumn],timeformat)

            if datetime_naive.tzinfo is None:
                datetime_local = timezone(datatimezone).localize(datetime_naive)
            else:
                datetime_local = datetime_naive

            timestamp = unix_time_millis(datetime_local) * 1000000 # in nanoseconds

            tags = copy.deepcopy(tmpnames)
            for t in tagcolumns:
                v = 0
                if t in row:
                    v = row[t]
                tags[t] = v

            fields = {}
            for f in fieldcolumns:
                v = 0
                if f in row:
                    if (isfloat(row[f])):
                        v = float(row[f])
                    elif (isbool(row[f])):
                        v = str2bool(row[f])
                    else:
                        v = row[f]
                fields[f] = v


            point = {"measurement": metric, "time": timestamp, "fields": fields, "tags": tags}

            datapoints.append(point)
            count+=1
            
            if len(datapoints) % batchsize == 0:
                print('Read %d lines'%count)
                print('Inserting %d datapoints...'%(len(datapoints)))
                response = client.write_points(datapoints)

                if not response:
                    print('Problem inserting points, exiting...')
                    exit(1)

                print("Wrote %d points, up to %s, response: %s" % (len(datapoints), datetime_local, response))

                datapoints = []
            

    # write rest
    if len(datapoints) > 0:
        print('Read %d lines'%count)
        print('Inserting %d datapoints...'%(len(datapoints)))
        response = client.write_points(datapoints)

        if response == False:
            print('Problem inserting points, exiting...')
            exit(1)

        print("Wrote %d, response: %s" % (len(datapoints), response))

    print('Done')
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Csv to influxdb.')

    parser.add_argument('-i', '--input', nargs='?', required=True,
                        help='Input csv file.')

    parser.add_argument('-d', '--delimiter', nargs='?', required=False, default=',',
                        help='Csv delimiter. Default: \',\'.')

    parser.add_argument('-s', '--server', nargs='?', default='localhost:8086',
                        help='Server address. Default: localhost:8086')

    parser.add_argument('--ssl', action='store_true', default=False,
                        help='Use HTTPS instead of HTTP.')

    parser.add_argument('-u', '--user', nargs='?', default='root',
                        help='User name.')

    parser.add_argument('-p', '--password', nargs='?', default='root',
                        help='Password.')

    parser.add_argument('--dbname', nargs='?', required=True,
                        help='Database name.')

    parser.add_argument('--create', action='store_true', default=False,
                        help='Drop database and create a new one.')

    parser.add_argument('-m', '--metricname', nargs='?', default='value',
                        help='Measurement name. Default: value')

    parser.add_argument('-tc', '--timecolumn', nargs='?', default='timestamp',
                        help='Timestamp column name. Default: timestamp.')

    parser.add_argument('-tf', '--timeformat', nargs='?', default='%Y-%m-%d %H:%M:%S',
                        help='Timestamp column format. Default: \'%%Y-%%m-%%d %%H:%%M:%%S\' e.g.: 1970-01-01 00:00:00')

    parser.add_argument('-tz', '--timezone', default='UTC',
                        help='Timezone of supplied data. Default: UTC')

    parser.add_argument('--fieldcolumns', nargs='?', default='',
                        help='List of csv columns to use as fields, separated by comma, e.g.: value1,value2. Default: value')

    parser.add_argument('--tagcolumns', nargs='?', default='',
                        help='List of csv columns to use as tags, separated by comma, e.g.: host,data_center. Default: host')

    parser.add_argument('--tags', nargs='?', default='',
                        help='List of csv key=value pairs to use as tags, separated by comma, e.g.: host=foo,station=bar.')

    parser.add_argument('-g', '--gzip', action='store_true', default=False,
                        help='Compress before sending to influxdb.')

    parser.add_argument('-b', '--batchsize', type=int, default=5000,
                        help='Batch size. Default: 5000.')

    parser.add_argument('--autofields', action='store_true', default=False,
                        help='Fill tag and field columns from CSV header.')

    args = parser.parse_args()

    loadCsv(args.input, args.server, args.user, args.password, args.dbname,
        args.metricname, args.timecolumn, args.timeformat, args.tagcolumns, 
        args.fieldcolumns, args.gzip, args.delimiter, args.batchsize, args.create, 
        args.timezone, args.ssl, args.autofields)
