__author__ = 'G'

import sys
import urllib
import pandas as pd
import argparse
import json
import datetime


# url = "http://documents.hants.gov.uk/population/2014SAPFforthewebLSOAbyageandgender.xlsx"
# output_path = "tempHccPopForecasts.csv"


def download(url, outPath):
    dName = outPath

    col = ['District', 'LSOA', 'Year', 'Gender', 'Age', 'Value', 'Production Date']

    listurl = url.split('/')
    pDate = listurl[len(listurl) - 1][:4]

    try:
        socket = urllib.request.urlopen(url)
    except urllib.error.HTTPError as e:
        errfile.write(str(now()) + ' excel download HTTPError is ' + str(e.code) + ' . End progress\n')
        logfile.write(str(now()) + ' error and end progress\n')
        sys.exit('excel download HTTPError = ' + str(e.code))
    except urllib.error.URLError as e:
        errfile.write(str(now()) + ' excel download URLError is ' + str(e.args) + ' . End progress\n')
        logfile.write(str(now()) + ' error and end progress\n')
        sys.exit('excel download URLError = ' + str(e.args))
    except Exception:
        print('excel file download error')
        import traceback
        errfile.write(str(now()) + ' generic exception: ' + str(traceback.format_exc()) + ' . End progress\n')
        logfile.write(str(now()) + ' error and end progress\n')
        sys.exit('generic exception: ' + traceback.format_exc())

    # operate this excel file
    logfile.write(str(now()) + ' excel file loading\n')
    print('excel file loading------')
    xd = pd.ExcelFile(socket)
    sheets = xd.sheet_names

    raw_data = {}
    for j in col:
        raw_data[j] = []

    for sheet in sheets:
        df = xd.parse(sheet)

        logfile.write(str(now()) + ' for sheet ' + str(sheet) + '------\n')
        logfile.write(str(now()) + ' indicator checking\n')
        print('for sheet ' + str(sheet) + ' ------')
        print('indicator checking------')

        fflag = 0
        for i in range(df.shape[0]):
            for j in range(df.shape[1]):
                if ('Aged' in str(df.iloc[i][j]).split()) and (len(str(df.iloc[i][j]).split()) == 2):
                    fflag = 1
                    break

            if fflag == 1:
                ageReq = df.iloc[i][j:-1].tolist()
                restartIndex = i + 1
                break

        if fflag == 0:
            errfile.write(str(now()) + " The sheet " + str(sheet) + " has not required fields, such as 'Aged 10-14'. Please check the file at: " + str(url) + " . End progress\n")
            logfile.write(str(now()) + ' error and end progress\n')
            sys.exit("The sheet " + str(sheet) + " has not not required fields, such as 'Aged 10-14'. Please check the file at: " + url)

        logfile.write(str(now()) + ' data reading\n')
        print('data reading------')
        for i in range(restartIndex, df.shape[0]):
            if str(df.iloc[i][0]):
                for k in ageReq:
                    raw_data[col[4]].append(k.split()[1])

                raw_data[col[0]] = raw_data[col[0]] + [(df.iloc[i][0])] * len(ageReq)
                raw_data[col[1]] = raw_data[col[1]] + [(df.iloc[i][1])] * len(ageReq)
                raw_data[col[3]] = raw_data[col[3]] + [(df.iloc[i][2])] * len(ageReq)
                raw_data[col[5]] = raw_data[col[5]] + df.iloc[i][j:-1].tolist()

        raw_data[col[2]] = raw_data[col[2]] + [sheet] * len(ageReq) * (df.shape[0] - restartIndex)

    raw_data[col[6]] = [pDate] * len(raw_data[col[0]])

    # save csv file
    logfile.write(str(now()) + ' writing to file\n')
    print('writing to file ' + dName)
    dfw = pd.DataFrame(raw_data, columns=col)
    dfw.to_csv(dName, index=False)
    logfile.write(str(now()) + ' has been extracted and saved as ' + str(dName) + '\n')
    print('Requested data has been extracted and saved as ' + dName)
    logfile.write(str(now()) + ' finished\n')
    print("finished")

def now():
    return datetime.datetime.now()


parser = argparse.ArgumentParser(
    description='Extract online Youth agelessness Data Excel file Section 1 to .csv file.')
parser.add_argument("--generateConfig", "-g", help="generate a config file called config_Yageless.json",
                    action="store_true")
parser.add_argument("--configFile", "-c", help="path for config file")
args = parser.parse_args()

if args.generateConfig:
    obj = {
        "url": "http://documents.hants.gov.uk/population/2014SAPFforthewebLSOAbyageandgender.xlsx",
        "outPath": "tempHccPopForecasts.csv"
    }

    logfile = open("log_tempHccPopForecasts.log", "w")
    logfile.write(str(now()) + ' start\n')

    errfile = open("err_tempHccPopForecasts.err", "w")

    with open("config_tempHccPopForecasts.json", "w") as outfile:
        json.dump(obj, outfile, indent=4)
        logfile.write(str(now()) + ' config file generated and end\n')
        sys.exit("config file generated")

if args.configFile == None:
    args.configFile = "config_tempHccPopForecasts.json"

with open(args.configFile) as json_file:
    oConfig = json.load(json_file)

    logfile = open('log_' + oConfig["outPath"].split('.')[0] + '.log', "w")
    logfile.write(str(now()) + ' start\n')

    errfile = open('err_' + oConfig["outPath"].split('.')[0] + '.err', "w")

    logfile.write(str(now()) + ' read config file\n')
    print("read config file")

download(oConfig["url"], oConfig["outPath"])
