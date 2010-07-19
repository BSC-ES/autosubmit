#!/usr/bin/env python

import datetime, time
from dateutil.relativedelta import *

string_date='19600101'

def add_months(string_date,number_of_months):
 date = time.strptime(string_date,'%Y%m%d')
 delta = relativedelta(months=+number_of_months)
 processed_date = datetime.date(date.tm_year,date.tm_mon,date.tm_mday)
 result = processed_date + delta
 return result

def subs_dates(start_date,end_date):
 start = time.strptime(start_date,'%Y%m%d')
 end = time.strptime(end_date,'%Y%m%d')
 start_datetime = datetime.date(start.tm_year,start.tm_mon,start.tm_mday)
 end_datetime= datetime.date(end.tm_year,end.tm_mon,end.tm_mday)
 result = end_datetime - start_datetime
 return result.days
 
def chunk_start_date(string_date,chunk,chunk_length):
 chunk_1 = chunk-1
 total_months = chunk_1 * chunk_length
 result = add_months(string_date,total_months)
 start_date = "%s%02d%02d" % (result.year,result.month,result.day)
 return start_date

def chunk_end_date(start_date,chunk_length):
 result = add_months(start_date,chunk_length)
 end_date = "%s%02d%02d" % (result.year,result.month,result.day)
 return end_date

def running_days(start_date,end_date):
 return subs_dates(start_date,end_date)
 
def previous_days(string_date,start_date):
 print string_date
 print start_date
 return subs_dates(string_date,start_date)

def chunk_start_month(string_date):
 date = time.strptime(string_date,'%Y%m%d')
 result=date.tm_mon
 return result

def chunk_start_year(string_date):
 date = time.strptime(string_date,'%Y%m%d')
 result=date.tm_year
 return result

if __name__ == "__main__":
 start_date = chunk_start_date(string_date,5,12)
 print start_date
 end_date = chunk_end_date(start_date,12)
 print end_date
 print running_days(start_date,end_date)
 print running_days(string_date,end_date)
 print previous_days(string_date,start_date)
 print "year: ", chunk_start_year(string_date)
 print "month: ", chunk_start_month(string_date)