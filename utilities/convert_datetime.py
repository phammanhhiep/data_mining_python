__author__ = 'Raphael Amoedo & Pham Manh Hiep' #a.k.a. Ralph Avalon
__license__ = 'MIT'

import datetime, calendar
import time

date_default_format = '%Y-%m-%d'


def add_days_to_datetime(original_datetime, days):
    try:
        d = original_datetime + datetime.timedelta(days)
    except Exception as ex:
        print(ex)  
        print(original_datetime)
        exit()
    return d



def compare_string_of_date(d_str1, d_str2, to_format=date_default_format):
    d_str1_obj = string_to_date(d_str1, to_format)
    d_str2_obj = string_to_date(d_str2, to_format)

    if d_str1_obj >= d_str2_obj:
        return True
    else:
        return False

def divide_dates(start_date, end_date, divide_by='m'):
    date_list = []

    if divide_by == 'm':
        stat_obj = string_to_date(start_date)
        end_obj = string_to_date(end_date)

        month_num = (end_obj.year - stat_obj.year)*12 + end_obj.month - stat_obj.month + 1
        print(month_num)
        for i in range(month_num):
            selected_start = stat_obj if not i else add_days_to_datetime(selected_start, get_num_days_in_month(selected_start))
            selected_end = add_days_to_datetime(selected_start, get_num_days_in_month(selected_start) - 1)

            date_list.append((date_to_string(selected_start), date_to_string(selected_end)))

    elif divide_by == 'w':
        pass
    elif divide_by == 'd':   
        pass
    elif divide_by == 'y':
        pass

    return date_list


def date_to_datetime(from_date):
    return datetime.datetime(from_date.year, from_date.month, from_date.day)

# find better way to handle exception
def date_to_string(from_date, to_format=date_default_format):
    try:
        s = datetime.datetime(from_date.year, from_date.month, from_date.day).strftime(to_format)
    except Exception as ex:     
        print(ex)
        print(from_date)
        exit()
    return s

def date_to_timestamp(from_date):
    return time.mktime(from_date.timetuple())

def datetime_to_date(from_datetime):
    return from_datetime.date()

def datetime_to_datetime(from_datetime, to_format):
    return datetime.datetime.strptime(from_datetime.strftime(to_format), to_format)

def datetime_to_string(from_datetime, to_format=date_default_format):
    return from_datetime.strftime(to_format)

def datetime_to_timestamp(from_datetime):
    return time.mktime(from_datetime.timetuple())

def get_num_days_in_month(date):
    return calendar.monthrange(date.year, date.month)[1]

def string_to_date(string, current_format=date_default_format):
    try:
        d = datetime.datetime.strptime(string, current_format).date()
    except Exception as ex:
        print(ex)
        print(string)
        exit()
    return d 

def string_to_datetime(string, current_format, to_format=None):
    if to_format:
        return datetime.datetime.strptime(datetime.datetime.strptime(string, current_format).strftime(to_format), to_format)
    return datetime.datetime.strptime(string, current_format)

def string_to_string(string, current_format, to_format):
    return datetime.datetime.strptime(string, current_format).strftime(to_format)

def string_to_timestamp(string, current_format):
    return time.mktime(datetime.strptime(string, current_format).timetuple())

def timestamp_to_date(timestamp):
    return datetime.datetime.fromtimestamp(timestamp).date()

def timestamp_to_datetime(timestamp, to_format=None):
    if to_format:
        return datetime.datetime.strptime(datetime.datetime.fromtimestamp(timestamp).strftime(to_format), to_format)
    return datetime.datetime.fromtimestamp(timestamp)

def timestamp_to_string(timestamp, to_format):
    return datetime.datetime.fromtimestamp(timestamp).strftime(to_format)

