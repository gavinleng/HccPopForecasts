__author__ = 'G'

import sys
import urllib
import pandas as pd
import argparse
import json
import datetime


# url = "http://documents.hants.gov.uk/population/2014SAPFforthewebLSOAbyageandgender.xlsx"
# output_path = "tempHccPopForecasts.csv"
# sheets = ["2014", "2016", "2021"]
# required_indicators = ["Aged 0", "Aged 1", "Aged 20-24", "Aged 80-84", "Aged 90+"]


def download(url, sheets, reqFields, outPath):
    ageReq = reqFields
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
    xd = pd.ExcelFile(socket)
    sName = xd.sheet_names

    raw_data = {}
    for j in col:
        raw_data[j] = []

    for sheet in sheets:
        if sheet not in sName:
            errfile.write(str(now()) + " Requested sheet " + str(sheet) + " does not match the excel file. Please check the file at: " + str(url) + " . End progress\n")
            logfile.write(str(now()) + ' error and end progress\n')
            sys.exit("Requested sheet " + str(sheet) + " does not match the excel file. Please check the file at: " + url)

        df = xd.parse(sheet)

        logfile.write(str(now()) + ' for sheet ' + str(sheet) + '------\n')
        logfile.write(str(now()) + ' indicator checking\n')
        print('for sheet ' + str(sheet) + ' ------')
        print('indicator checking------')
        for i in range(df.shape[0]):
            numCol = []
            for k in ageReq:
                for j in range(df.shape[1]):
                    if df.iloc[i][j] == k:
                        numCol.append(j)
                        restartIndex = i + 1

            if len(numCol) == len(ageReq):
                break

        if len(numCol) != len(ageReq):
            errfile.write(str(now()) + " Requested data " + str(ageReq).strip(
                '[]') + " don't match the excel file. Please check the file at: " + str(url) + " . End progress\n")
            logfile.write(str(now()) + ' error and end progress\n')
            sys.exit("Requested data " + str(ageReq).strip(
                '[]') + " don't match the excel file. Please check the file at: " + url)

        logfile.write(str(now()) + ' data reading\n')
        print('data reading------')
        for i in range(restartIndex, df.shape[0]):
            if str(df.iloc[i][0]):
                for k in range(len(numCol)):
                    raw_data[col[0]].append(df.iloc[i][0])
                    raw_data[col[1]].append(df.iloc[i][1])
                    raw_data[col[2]].append(sheet)
                    raw_data[col[3]].append(df.iloc[i][2])
                    raw_data[col[4]].append(ageReq[k].split(' ')[1])
                    raw_data[col[5]].append(df.iloc[i][numCol[k]])
                    raw_data[col[6]].append(pDate)

    # save csv file
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
        "outPath": "tempHccPopForecasts.csv",
        "sheet": ["2014", "2016", "2021"],
        "reqFields": ["Aged 0", "Aged 1", "Aged 20-24", "Aged 80-84", "Aged 90+"]
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

download(oConfig["url"], oConfig["sheet"], oConfig["reqFields"], oConfig["outPath"])
