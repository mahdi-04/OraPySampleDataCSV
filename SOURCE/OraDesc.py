#!/usr/bin/env python
# -*- coding: windows-1256 -*-
from __future__ import print_function
import argparse
import cx_Oracle
import logging
import os
import sys
import dbHelper
from Logger import Logger
from configparser import ConfigParser

sql = """

SELECT
    t.column_name   "Name",
    DECODE(t.nullable, 'Y', 'NULL', 'NOT NULL') "Null?",
    DECODE(t.data_type_mod, NULL, '', t.data_type_mod || ' OF ')
    || ( CASE
        WHEN ( t.data_type_owner = upper(t.owner)
               OR t.data_type_owner IS NULL ) THEN ''
        ELSE t.data_type_owner || '.'
    END )
    || DECODE(t.data_type, 'BFILE', 'BINARY FILE LOB', upper(t.data_type))
    || CASE
        WHEN ( t.data_type = 'VARCHAR'
               OR t.data_type = 'VARCHAR2'
               OR t.data_type = 'RAW'
               OR t.data_type = 'CHAR' )
             AND ( t.data_length <> 0
                   AND nvl(t.data_length, - 1) <> - 1 ) THEN CASE
            WHEN ( t.char_used = 'C'
                   AND 'BYTE' = (
                SELECT
                    value
                FROM
                    nls_session_parameters
                WHERE
                    parameter = 'NLS_LENGTH_SEMANTICS'
            ) ) THEN '('
                     || t.char_length
                     || ' CHAR)'
            WHEN ( t.char_used = 'B'
                   AND 'CHAR' = (
                SELECT
                    value
                FROM
                    nls_session_parameters
                WHERE
                    parameter = 'NLS_LENGTH_SEMANTICS'
            ) ) THEN '('
                     || t.data_length
                     || ' BYTE)'
            WHEN ( t.char_used = 'C'
                   AND 'CHAR' = (
                SELECT
                    value
                FROM
                    nls_session_parameters
                WHERE
                    parameter = 'NLS_LENGTH_SEMANTICS'
            ) ) THEN '('
                     || t.char_length
                     || ')'
            WHEN ( t.char_used = 'B'
                   AND 'BYTE' = (
                SELECT
                    value
                FROM
                    nls_session_parameters
                WHERE
                    parameter = 'NLS_LENGTH_SEMANTICS'
            ) ) THEN '('
                     || t.data_length
                     || ')'
            ELSE '('
                 || t.data_length
                 || ' BYTE)'
        END
        WHEN ( t.data_type = 'NVARCHAR2'
               OR t.data_type = 'NCHAR' ) THEN '('
                                               || t.data_length / 2
                                               || ')'
        WHEN ( t.data_type LIKE 'TIMESTAMP%'
               OR t.data_type LIKE 'INTERVAL DAY%'
               OR t.data_type LIKE 'INTERVAL YEAR%'
               OR t.data_type = 'DATE'
               OR ( t.data_type = 'NUMBER'
                    AND ( ( t.data_precision = 0 )
                          OR nvl(t.data_precision, - 1) = - 1 )
                    AND nvl(t.data_scale, - 1) = - 1 ) ) THEN ''
        WHEN ( ( t.data_type = 'NUMBER'
                 AND nvl(t.data_precision, - 1) = - 1 )
               AND ( t.data_scale = 0 ) ) THEN '(38)'
        WHEN ( ( t.data_type = 'NUMBER'
                 AND nvl(t.data_precision, - 1) = - 1 )
               AND ( nvl(t.data_scale, - 1) != - 1 ) ) THEN '(38,'
                                                            || t.data_scale
                                                            || ')'
        WHEN ( t.data_type = 'BINARY_FLOAT'
               OR t.data_type = 'BINARY_DOUBLE' ) THEN ''
        WHEN ( t.data_precision IS NULL
               AND t.data_scale IS NULL ) THEN ''
        WHEN ( t.data_scale = 0
               OR nvl(t.data_scale, - 1) = - 1 ) THEN '('
                                                      || t.data_precision
                                                      || ')'
        WHEN ( t.data_precision != 0
               AND t.data_scale != 0 ) THEN '('
                                            || t.data_precision
                                            || ','
                                            || t.data_scale
                                            || ')'
    END "Type"
FROM
    sys.all_tab_columns t
WHERE
    upper(t.owner) = :OWNER
    AND upper(t.table_name) = :NAME
ORDER BY
    t.column_id

"""


def description(arg_user, arg_pass, arg_dsn, arg_log, arg_objName):
    __level__ = logging.INFO

    logMain = Logger(filename="main_init", level=__level__,
                     dirname="File-" + os.path.basename(__file__), rootdir=arg_log)

    global i
    try:
        connection = dbHelper.Connection(arg_user, arg_pass, arg_dsn, arg_log + '/ORA')
        cursor = connection.cursor()

        ownerInput = arg_objName.split('.')[0]
        objNameInput = arg_objName.split('.')[1]

        try:
            cursor.execute(sql, {'OWNER': ownerInput, "NAME": objNameInput})

            all_records = cursor.fetchall()

            list_result = []
            for result in all_records:
                list_result.append(result[0])

            return list_result

        except cx_Oracle.DatabaseError as ex:
            logMain.warning("General Error Database in -> NewMaxID()")
            logMain.error("Error Massage: " + str(ex))
        except:
            logMain.error("Unexpected error:" + str(sys.exc_info()[0]))

    except RuntimeError as e:

        logMain.error(sys.stderr.write("ERROR: %s\n" % e))
