__author__ = 'G'

import sys
sys.path.append('../harvesterlib')

import pandas as pd
import argparse
import json

import now
import openurl
import datasave as dsave


# url = "http://documents.hants.gov.uk/population/2014SAPFforthewebLSOAbyageandgender.xlsx"
# output_path = "tempHccPopForecasts.csv"


def download(url, outPath, col, keyCol, digitCheckCol, noDigitRemoveFields):
    dName = outPath

    listurl = url.split('/')
    pDate = listurl[len(listurl) - 1][:4]

    # operate this excel file
    logfile.write(str(now.now()) + ' excel file loading\n')
    print('excel file loading------')
    xd = pd.ExcelFile(url)
    sheets = xd.sheet_names

    raw_data = {}
    for j in col:
        raw_data[j] = []

    for sheet in sheets:
        df = xd.parse(sheet)

        logfile.write(str(now.now()) + ' for sheet ' + str(sheet) + '------\n')
        logfile.write(str(now.now()) + ' indicator checking\n')
        print('for sheet ' + str(sheet) + ' ------')
        print('indicator checking------')

        # indicator checking
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
            errfile.write(str(now.now()) + " The sheet " + str(sheet) + " has not required fields, such as 'Aged 10-14'. Please check the file at: " + str(url) + " . End progress\n")
            logfile.write(str(now.now()) + ' error and end progress\n')
            sys.exit("The sheet " + str(sheet) + " has not not required fields, such as 'Aged 10-14'. Please check the file at: " + url)

        # data reading
        logfile.write(str(now.now()) + ' data reading\n')
        print('data reading------')
        for i in range(restartIndex, df.shape[0]):
            if str(df.iloc[i][0]):
                for k in ageReq:
                    raw_data[col[5]].append(k.split()[1])

                raw_data[col[0]] = raw_data[col[0]] + [(df.iloc[i][0])] * len(ageReq)
                raw_data[col[1]] = raw_data[col[1]] + [(df.iloc[i][1])] * len(ageReq)
                raw_data[col[2]] = raw_data[col[2]] + [(df.iloc[i][2])] * len(ageReq)
                raw_data[col[4]] = raw_data[col[4]] + [(df.iloc[i][3])] * len(ageReq)
                raw_data[col[6]] = raw_data[col[6]] + df.iloc[i][j:-1].tolist()

        raw_data[col[3]] = raw_data[col[3]] + [sheet] * len(ageReq) * (df.shape[0] - restartIndex)


    raw_data[col[7]] = [pDate] * len(raw_data[col[0]])
    raw_data[col[8]] = ["HCC_SAPF_2015"] * len(raw_data[col[0]])
    logfile.write(str(now.now()) + ' data reading end\n')
    print('data reading end------')

    # save csv file
    dsave.save(raw_data, col, keyCol, digitCheckCol, noDigitRemoveFields, dName, logfile)


parser = argparse.ArgumentParser(
    description='Extract online HCC Pop Forecasts Data Excel file to .csv file.')
parser.add_argument("--generateConfig", "-g", help="generate a config file called config_tempHccPopForecasts.json",
                    action="store_true")
parser.add_argument("--configFile", "-c", help="path for config file")
args = parser.parse_args()

if args.generateConfig:
    obj = {
        "url": "./data/2014HccPopForecasts.xlsx",
        "outPath": "tempHccPopForecasts.csv",
        "colFields": ['District', 'ONSCode', 'LSOA', 'Year', 'Gender', 'Ageband', 'Value', 'Production Date', 'ScenarioID'],
        "primaryKeyCol": [],
        "digitCheckCol": ['Value'],
        "noDigitRemoveFields": []
    }

    logfile = open("log_tempHccPopForecasts.log", "w")
    logfile.write(str(now.now()) + ' start\n')

    errfile = open("err_tempHccPopForecasts.err", "w")

    with open("config_tempHccPopForecasts.json", "w") as outfile:
        json.dump(obj, outfile, indent=4)
        logfile.write(str(now.now()) + ' config file generated and end\n')
        sys.exit("config file generated")

if args.configFile == None:
    args.configFile = "config_tempHccPopForecasts.json"

with open(args.configFile) as json_file:
    oConfig = json.load(json_file)

    logfile = open('log_' + oConfig["outPath"].split('.')[0] + '.log', "w")
    logfile.write(str(now.now()) + ' start\n')

    errfile = open('err_' + oConfig["outPath"].split('.')[0] + '.err', "w")

    logfile.write(str(now.now()) + ' read config file\n')
    print("read config file")

download(oConfig["url"], oConfig["outPath"], oConfig["colFields"], oConfig["primaryKeyCol"], oConfig["digitCheckCol"], oConfig["noDigitRemoveFields"])
