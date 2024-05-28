from datetime import datetime


def get_date_num():
    return int(datetime.now().strftime("%Y%m%d"))


def convert(datetime_string, from_format, to_format):
    datetime_object = datetime.strptime(datetime_string, from_format)
    return datetime.strftime(datetime_object, to_format)


def get_datetime_from_str(datetime_string, pattern='%Y-%m-%dT%H:%M:%S'):
    return datetime.strptime(datetime_string, pattern)


def get_now_datetime():
    return datetime.strftime(datetime.now(), '%Y-%m-%dT%H:%M:%S')
