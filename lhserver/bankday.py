import datetime


def the_first(d):
    """
    >>> the_first(datetime.date(2015, 5, 6))
    datetime.date(2015, 5, 1)
    """
    return d - datetime.timedelta(d.day - 1)


def month_end(year_or_date, month=None):
    if isinstance(year_or_date, int):
        year = year_or_date
        next1 = the_first(datetime.date(year, month, 1) + datetime.timedelta(35))
        return next1 - datetime.timedelta(1)
    else:
        assert month == None
        d = year_or_date
        next = d + datetime.timedelta(35 - d.day)
        return next - datetime.timedelta(next.day)


def n_months_earlier(d, n):
    """
    >>> n_months_earlier(datetime.date(2015, 5, 1), 2)
    datetime.date(2015, 3, 1)
    >>> n_months_earlier(datetime.date(2015, 5, 1), 6)
    datetime.date(2014, 11, 1)
    >>> n_months_earlier(datetime.date(2015, 5, 1), 8)
    datetime.date(2014, 9, 1)
    >>> n_months_earlier(datetime.date(2015, 5, 1), 12)
    datetime.date(2014, 5, 1)
    >>> n_months_earlier(datetime.date(2015, 5, 1), 24)
    datetime.date(2013, 5, 1)
    >>> n_months_earlier(datetime.date(2015, 5, 1), 5*12)
    datetime.date(2010, 5, 1)
    """
    assert d.day == 1
    if n < 4:
        offset = n * 28
    elif n < 8:
        offset = n * 29
    elif n < 16:
        offset = n * 30
    else:
        years, months = n // 12, n % 12
        years, months = years - 1, months + 12
        offset = 365 * years + months * 30
    return the_first(d - datetime.timedelta(offset))


def add_business_days(base, n):
    """
    This emulates the Fido SQL add_business_days function for Python datetime.date objects.

    >>> d = datetime.date(2017, 9, 7) # thursday
    >>> add_business_days(d, 1)
    datetime.date(2017, 9, 8)
    >>> add_business_days(d, 2)
    datetime.date(2017, 9, 11)
    >>> add_business_days(d, -3)
    datetime.date(2017, 9, 4)
    >>> add_business_days(d, -4)
    datetime.date(2017, 9, 1)

    >>> d = datetime.date(2017, 9, 2) # saturday
    >>> add_business_days(d, 2)
    datetime.date(2017, 9, 5)
    >>> d = datetime.date(2017, 9, 1) # friday
    >>> add_business_days(d, 2)
    datetime.date(2017, 9, 5)
    """
    weeks, days = n // 5, n % 5
    base = base + datetime.timedelta(weeks * 7)
    if days != 0 and base.weekday() >= 5:
        days += 6 - base.weekday()
    elif days != 0 and base.weekday() + days >= 5:
        days += 2
    return base + datetime.timedelta(days)
