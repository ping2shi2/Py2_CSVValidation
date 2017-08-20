# -*- coding: utf-8 -*-
'''
Created on 2017/07/30
'''

from __future__ import print_function
import os

import pandas as pd
import ConfigParser
import logging
from validate_email import validate_email
import datetime


APPLICATION_NAME = 'CSV Validator'


class CsvValidationError (Exception):
    """
    csvファイルのValidation時に発生するエラー
    """
    def __init__ (self, message):

        if isinstance(message, unicode):
            super(CsvValidationError, self).__init__(message.encode('utf-8'))
            self.message = message
        elif isinstance(message, str):
            super(CsvValidationError, self).__init__(message)
            self.message = message.decode('utf-8')
        else:
            raise TypeError

    def __unicode__ (self):
        # エラーメッセージ
        return (u'CSVチェックエラー "{0}"'.format (self.message))


def setup_logger():
    """
    Setup Logger.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    log_file_path = os.path.join(os.path.abspath('..'), 'log')
    d = datetime.datetime.now()
    log_file_path = os.path.join(log_file_path, 'CSVValidator' + d.strftime("%Y%m%d %H%M%S") + '.log')
    fh = logging.FileHandler(log_file_path, encoding = "UTF-8")
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    logger.addHandler(sh)

    formatter = logging.Formatter(
        '%(asctime)s:%(lineno)d:%(levelname)s:%(message)s')
    fh.setFormatter(formatter)
    sh.setFormatter(formatter)

    return logger


logger = setup_logger()


def setup_config():
    """
    Setup Config.
    """
    # 設定ファイル読み込み
    conf_dir_path = os.path.join(os.path.abspath('..'), 'conf')
    conf_file_path = os.path.join(conf_dir_path, 'settings.ini')
    conf_file = ConfigParser.SafeConfigParser()
    conf_file.read(conf_file_path)

    # configの値はglobal変数に
    global VALIDATE_HEADER
    global VALIDATE_REQUIRE
    global VALIDATE_LENGTH
    global VALIDATE_ILLEGAL_CHAR
    global VALIDATE_FORMAT
    global VALIDATE_SELECT
    global VALIDATE_UNIQUE
    global VALIDATE_INCLUDE

    global HEADER_FILE_NAME
    global REQUIRE_FILE_NAME
    global LENGTH_FILE_NAME
    global ILLEGAL_CHAR_FILE_NAME
    global FORMAT_FILE_NAME
    global SELECT_FILE_NAME
    global UNIQUE_FILE_NAME
    global INCLUDE_FILE_NAME
    global OBJECT_FILE_NAME

    # 設定ファイルからチェック種類を取得
    VALIDATE_HEADER = conf_file.getboolean(
        'settings', 'VALIDATE_HEADER')
    VALIDATE_REQUIRE = conf_file.getboolean(
        'settings', 'VALIDATE_REQUIRE')
    VALIDATE_LENGTH = conf_file.getboolean(
        'settings', 'VALIDATE_LENGTH')
    VALIDATE_ILLEGAL_CHAR = conf_file.getboolean(
        'settings', 'VALIDATE_ILLEGAL_CHAR')
    VALIDATE_FORMAT = conf_file.getboolean(
        'settings', 'VALIDATE_FORMAT')
    VALIDATE_SELECT = conf_file.getboolean(
        'settings', 'VALIDATE_SELECT')
    VALIDATE_UNIQUE = conf_file.getboolean(
        'settings', 'VALIDATE_UNIQUE')
    VALIDATE_INCLUDE = conf_file.getboolean(
        'settings', 'VALIDATE_INCLUDE')

    # 設定ファイルから各ファイル名を取得
    if VALIDATE_HEADER:
        HEADER_FILE_NAME = unicode(conf_file.get(
            'settings', 'HEADER_FILE_NAME'), 'UTF-8')
    if VALIDATE_REQUIRE:
        REQUIRE_FILE_NAME = unicode(conf_file.get(
            'settings', 'REQUIRE_FILE_NAME'), 'UTF-8')
    if VALIDATE_LENGTH:
        LENGTH_FILE_NAME = unicode(conf_file.get(
            'settings', 'LENGTH_FILE_NAME'), 'UTF-8')
    if VALIDATE_ILLEGAL_CHAR:
        ILLEGAL_CHAR_FILE_NAME = unicode(conf_file.get(
            'settings', 'ILLEGAL_CHAR_FILE_NAME'), 'UTF-8')
    if VALIDATE_FORMAT:
        FORMAT_FILE_NAME = unicode(conf_file.get(
            'settings', 'FORMAT_FILE_NAME'), 'UTF-8')
    if VALIDATE_SELECT:
        SELECT_FILE_NAME = unicode(conf_file.get(
            'settings', 'SELECT_FILE_NAME'), 'UTF-8')
    if VALIDATE_UNIQUE:
        UNIQUE_FILE_NAME = unicode(conf_file.get(
            'settings', 'UNIQUE_FILE_NAME'), 'UTF-8')
    if VALIDATE_INCLUDE:
        INCLUDE_FILE_NAME = unicode(conf_file.get(
            'settings', 'INCLUDE_FILE_NAME'), 'UTF-8')
    OBJECT_FILE_NAME = unicode(conf_file.get(
        'settings', 'OBJECT_FILE_NAME'), 'UTF-8')


setup_config()


def validate_header(header_df, object_df):
    """
    validate header
    """
    correct_header_list = header_df.columns.values.tolist()

    object_header_list = object_df.columns.values.tolist()

    shortage_header_set = set(correct_header_list) - set(object_header_list)

    error_list = list(shortage_header_set)

    if error_list:
        raise CsvValidationError(u"CSVヘッダエラー : "  + ','.join(error_list))


def validate_unique(unique_df, object_df):
    """
    validate unique
    """

    error_list = []

    for column, value in unique_df.iteritems():

        if pd.isnull(value[0]):
            continue

        # 該当列を重複判定
        dup_series = object_df[column].duplicated()

        # 重複結果Trueのindexを取得
        dup_ix_list = (dup_series[dup_series == True].index)

        # +2 してcsvの行番号に変換
        dup_ix_list = map(lambda x: x+2, dup_ix_list)

        error_list.append(u"列=" + column + " : Line No. = " + ','.join(map(str,dup_ix_list)))

    if dup_ix_list:
        raise CsvValidationError(u"重複エラー : "  + ','.join(error_list))


def validate_require(require_df, series_row):
    """
    validate require
    """
    error_list = []

    for column, value in require_df.iteritems():

        if value.values[0:1][0] == 1:
            # 必須項目の場合
            if pd.isnull(series_row[column]):
                error_list.append(unicode(column))

    if error_list:
        raise CsvValidationError(u"必須項目エラー : "  + ','.join(error_list))


def validate_length(length_df, series_row):
    """
    validate require
    """
    error_list = []

    for column, value in length_df.iteritems():

        min_length = value.values[0:1][0]

        if min_length > 0:
            # minlengthcheck対象の場合
            if not len(str(series_row[column])) >= min_length:
                error_list.append(column)

        max_length = value.values[0:2][0]

        if max_length > 0:
            # maxlengthcheck対象の場合
            if not len(str(series_row[column])) <= max_length:
                error_list.append(column)

    if error_list:
        raise CsvValidationError(u"項目長エラー : "  + ','.join(error_list))


def validate_illegal_char(illegal_char_df, series_row):
    """
    validate_illegal_char
    """
    error_list = []

    for column, value in illegal_char_df.iteritems():

        illegal_chars = value.values[0:1][0]

        if pd.isnull(illegal_chars):
            # NaNの場合次の列へ
            continue

        # ";"区切りの複数禁止文字を分割
        illegal_char_list = illegal_chars.split(";")

        for illegal_char in illegal_char_list:
            # illegal_char check
            if illegal_char in str(series_row[column]):
                error_list.append(column)

    if error_list:
        raise CsvValidationError(u"禁止文字エラー : "  + ','.join(error_list))


def validate_format(format_df, series_row):
    """
    validate_illegal_char
    """
    error_list = []

    for column, value in format_df.iteritems():

        formats = value.values[0:1][0]

        if pd.isnull(formats):
            # NaNの場合次の列へ
            continue

        # ";"区切りの複数formatを分割
        format_list = formats.split(";")
        if len(format_list) > 0:
            if pd.isnull(series_row[column]):
                continue

        object_strs = unicode(series_row[column])
        object_str_list = object_strs.split(";")

        for format in format_list:
            # format check
            if format == "email":
                for object_str in object_str_list:
                    if not validate_email(object_str):
                        error_list.append(column)

    if error_list:
        raise CsvValidationError(u"データフォーマットエラー : "  + ','.join(error_list))


def validate_select(select_df, series_row):
    """
    validate_select
    """
    error_list = []

    for column, value in select_df.iteritems():

        if pd.isnull(value.values[0:1]):
            # 候補値なしの場合
            continue

        select_list = map(str, list(value))

        object_value = series_row[column]

        if not object_value in select_list:
            error_list.append(column)

    if error_list:
        raise CsvValidationError(u"選択値エラー : "  + ','.join(error_list))


def validate_include(include_df, series_row):
    """
    validate_include_str
    """
    error_list = []

    for column, value in include_df.iteritems():

        include_strs = value.values[0:1][0]

        if pd.isnull(include_strs):
            # NaNの場合次の列へ
            continue

        # ";"区切りの複数formatを分割
        include_str_list = include_strs.split(";")
        if len(include_str_list) > 0:
            if pd.isnull(series_row[column]):
                continue

        object_strs = unicode(series_row[column])
        object_str_list = object_strs.split(";")

        for include_str in include_str_list:
            # format check
            for object_str in object_str_list:
                if not include_str in object_str:
                    error_list.append(column)

    if error_list:
        raise CsvValidationError(u"キーワード文字列なしエラー : "  + ','.join(error_list))


def main():
    """
    Creates a Google Calendar API service object and
    create events on the user's calendar.
    """
    logger.info('Start CSV Validator')

    # CSVファイルディレクトリ
    csv_files_path = os.path.join(os.path.abspath('..'), 'csvfiles')

    # ヘッダの定義
    if VALIDATE_HEADER:
        header_csv_file = os.path.join(csv_files_path, HEADER_FILE_NAME)
        header_df = pd.read_csv(header_csv_file, encoding = 'utf8')

    # 必須項目の定義
    if VALIDATE_REQUIRE:
        require_csv_file = os.path.join(csv_files_path, REQUIRE_FILE_NAME)
        require_df = pd.read_csv(require_csv_file, encoding = 'utf8')

    # 項目長の定義
    if VALIDATE_LENGTH:
        length_csv_file = os.path.join(csv_files_path, LENGTH_FILE_NAME)
        length_df = pd.read_csv(length_csv_file, encoding = 'utf8')

    # 禁止文字の定義
    if VALIDATE_ILLEGAL_CHAR:
        illegal_char_csv_file = os.path.join(csv_files_path, ILLEGAL_CHAR_FILE_NAME)
        illegal_char_df = pd.read_csv(illegal_char_csv_file, encoding = 'utf8')

    # 形式
    if VALIDATE_FORMAT:
        format_csv_file = os.path.join(csv_files_path, FORMAT_FILE_NAME)
        format_df = pd.read_csv(format_csv_file, encoding = 'utf8')

    # 選択
    if VALIDATE_SELECT:
        select_csv_file = os.path.join(csv_files_path, SELECT_FILE_NAME)
        select_df = pd.read_csv(select_csv_file, encoding = 'utf8')

    # ユニーク
    if VALIDATE_UNIQUE:
        unique_csv_file = os.path.join(csv_files_path, UNIQUE_FILE_NAME)
        unique_df = pd.read_csv(unique_csv_file, encoding = 'utf8')

    # 文字列含む
    if VALIDATE_INCLUDE:
        include_csv_file = os.path.join(csv_files_path, INCLUDE_FILE_NAME)
        include_df = pd.read_csv(include_csv_file, encoding = 'utf8')

    # Vlidation対象CSV読み込み
    object_csv_file = os.path.join(csv_files_path, OBJECT_FILE_NAME)
    object_df = pd.read_csv(object_csv_file, encoding = 'utf8')

    if VALIDATE_HEADER:
        try:
            validate_header(header_df, object_df)
        except CsvValidationError as e:
            logger.error(e.message)
        except Exception as e:
            logger.exception(e)

    if VALIDATE_UNIQUE:
        try:
            validate_unique(unique_df, object_df)
        except CsvValidationError as e:
            logger.error(e.message)
        except Exception as e:
            logger.exception(e)


    for i, series_row in object_df.iterrows():
        # チェック対象CSVを行方向にループ
        # iをCSVの行数に変換
        line_no = i + 2

        if VALIDATE_REQUIRE:
            try:
                validate_require(require_df, series_row)
            except CsvValidationError as e:
                logger.error(u"Line : " + unicode(line_no) + " : " + e.message)
            except Exception as e:
                logger.error(u"Line :"  + unicode(line_no))
                logger.exception(e)

        if VALIDATE_LENGTH:
            try:
                validate_length(length_df, series_row)
            except CsvValidationError as e:
                logger.error(u"Line : " + unicode(line_no) + " : " + e.message)
            except Exception as e:
                logger.error(u"Line :"  + unicode(line_no))
                logger.exception(e)

        if VALIDATE_ILLEGAL_CHAR:
            try:
                validate_illegal_char(illegal_char_df, series_row)
            except CsvValidationError as e:
                logger.error(u"Line : " + unicode(line_no) + " : " + e.message)
            except Exception as e:
                logger.error(u"Line :"  + unicode(line_no))
                logger.exception(e)

        if VALIDATE_FORMAT:
            try:
                validate_format(format_df, series_row)
            except CsvValidationError as e:
                logger.error(u"Line : " + unicode(line_no) + " : " + e.message)
            except Exception as e:
                logger.error(u"Line :"  + unicode(line_no))
                logger.exception(e)

        if VALIDATE_SELECT:
            try:
                validate_select(select_df, series_row)
            except CsvValidationError as e:
                logger.error(u"Line : " + unicode(line_no) + " : " + e.message)
            except Exception as e:
                logger.error(u"Line :"  + unicode(line_no))
                logger.exception(e)

        if VALIDATE_INCLUDE:
            try:
                validate_include(include_df, series_row)
            except CsvValidationError as e:
                logger.error(u"Line : " + unicode(line_no) + " : " + e.message)
            except Exception as e:
                logger.error(u"Line :"  + unicode(line_no))
                logger.exception(e)


    logger.info('End CSV Validator')

if __name__ == '__main__':
    main()
