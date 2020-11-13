import datetime


def epoch_from_datetime(dt):
    return (dt - datetime.datetime(1970, 1, 1)) / datetime.timedelta(seconds=1)


def utcnow():
    return epoch_from_datetime(datetime.datetime.utcnow())
