#!/usr/bin/env python
# -*- coding: windows-1256 -*-
import csv
import sys
import traceback
import cx_Oracle
import os
import argparse
import dbHelper
import logging
from Logger import Logger
from OraDesc import description
from configparser import ConfigParser

prs = argparse.ArgumentParser(description='usage')
prs.add_argument('objName', action='store', help='log directory path')
prs.add_argument('numRows', action='store', help='log directory path')

prs.add_argument('cfg_file', action='store', help='configFile.ini path')
prs.add_argument('log_dir', action='store', help='log directory path')
args = prs.parse_args()

__LOGDIR__ = os.path.abspath(args.log_dir)
__confiFileName__ = os.path.abspath(args.cfg_file)
__objName__ = args.objName
__numRows__ = int(args.numRows)

__level__ = logging.INFO

logMain = Logger(filename="main_init", level=__level__,
                 dirname="File-" + os.path.basename(__file__), rootdir=__LOGDIR__)

headerCSV = []
column = ''
latest_row = []


def loadConfigFile():
    logLoadConfigFile = Logger(filename="__init__", level=__level__,
                               dirname="File-" + os.path.basename(
                                   __file__) + "-Func-" + sys._getframe().f_code.co_name, rootdir=__LOGDIR__)
    try:
        configINI = ConfigParser()
        configINI.read(__confiFileName__)

        global V_DB_USERNAME
        global V_DB_PASSWORD
        global V_DB_DSN
        global V_OBJ_PATH

        V_DB_USERNAME = configINI.get('ORACLE_CONNECTION', 'dbUsername')
        V_DB_PASSWORD = configINI.get('ORACLE_CONNECTION', 'dbPassword')
        V_DB_DSN = configINI.get('ORACLE_CONNECTION', 'dbDSN')
        V_OBJ_PATH = configINI.get('DESC_OPTION', 'descPATH')

    except:
        logLoadConfigFile.error("Unexpected error:" + str(sys.exc_info()[0]))


try:

    loadConfigFile()
    os.environ["PYTHONIOENCODING"] = "windows-1256"
    connection = None
    columns = description(V_DB_USERNAME, V_DB_PASSWORD, V_DB_DSN, __LOGDIR__, __objName__)

    if __name__ == "__main__":

        logMain = Logger(filename="__init__", level=__level__,
                         dirname="File-" + os.path.basename(
                             __file__) + "-Func-" + sys._getframe().f_code.co_name, rootdir=__LOGDIR__)

        try:
            # create instances of the dbHelper connection and cursor
            connection = dbHelper.Connection(V_DB_USERNAME, V_DB_PASSWORD, V_DB_DSN, __LOGDIR__ + '/ORA')
            cursor = connection.cursor()

            os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AR8MSWIN1256"

            # demonstrate that the dbHelper connection and cursor are being used
            try:
                # No commit as you don-t need to commit DDL.
                V_NLS_LANGUAGE, = cursor.execFetchOne("SELECT VALUE AS NLS_LANGUAGE "
                                                      "FROM V$NLS_PARAMETERS "
                                                      "WHERE PARAMETER = ('NLS_LANGUAGE')"
                                                      ,
                                                      __LOGDIR__ + '/ORA')

                V_NLS_TERRITORY, = cursor.execFetchOne("SELECT VALUE AS NLS_TERRITORY "
                                                       "FROM V$NLS_PARAMETERS "
                                                       "WHERE PARAMETER = ('NLS_TERRITORY')"
                                                       ,
                                                       __LOGDIR__ + '/ORA')

                V_NLS_CHARACTERSET, = cursor.execFetchOne("SELECT VALUE AS NLS_CHARACTERSET "
                                                          "FROM V$NLS_PARAMETERS WHERE "
                                                          "PARAMETER = ('NLS_CHARACTERSET')"
                                                          ,
                                                          __LOGDIR__ + '/ORA')

                if V_NLS_LANGUAGE and V_NLS_TERRITORY and V_NLS_CHARACTERSET is not None:
                    # export NLS_LANG=<language>_<territory>.<character set>
                    os.environ["NLS_LANG"] = V_NLS_LANGUAGE + "." + V_NLS_TERRITORY + "." + V_NLS_CHARACTERSET

                    for value in columns:
                        headerCSV.append(value)
                        column += value + ","

                    sql = "SELECT " + column[:-1] + " FROM " + __objName__

                    cursor.execute(sql)
                    records = cursor.fetchmany(numRows=__numRows__)

                    final_result = [list(i) for i in records]

                    print(final_result)

                    with open(V_OBJ_PATH, "w", encoding='windows-1256') as output:
                        writer = csv.writer(output, lineterminator='\n')
                        writer.writerow(headerCSV)
                        writer.writerows(final_result)

            except Exception as e:
                logMain.error(traceback.format_exc())
            except:
                logMain.error("Unexpected error:" + sys.exc_info()[0])

            # Ensure that we always disconnect from the database to avoid
            # ORA-00018: Maximum number of sessions exceeded.

        except cx_Oracle.DatabaseError as ex:
            logMain.warning("General Error Database in -> NewMaxID()")
            logMain.error("Error Massage: " + str(ex))
        except:
            logMain.error("Unexpected error:" + sys.exc_info()[0])

except RuntimeError as e:
    logMain.error(sys.stderr.write("ERROR: %s\n" % e))

except:
    logMain.error("Unexpected error:" + sys.exc_info()[0])
