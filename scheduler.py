import schedule
import time


def job(show):
    print(show)
    return schedule.CancelJob


def job1(show):
    print(show)
    return schedule.CancelJob


scheduler = None


def getSchedule():
    global scheduler
    scheduler = "hello"


# schedule.every(5).seconds.do(lambda: job("I'm working....."))
# schedule.every(3).seconds.do(job1, show="I'm working1.....")
schedule.every(5).seconds.do(getSchedule)

count = 0
while True:
    schedule.run_pending()
