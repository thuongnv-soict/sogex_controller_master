import random
import schedule
import constant
from datetime import timedelta

from infrastructure.database import Database
from infrastructure.mqtt import MQTT
from model.message import JobMessage, UpdateJobMessage
from task.module.util import *
from task.module.validation import *

broker = MQTT()
db = Database()


# ----------------------------------------
# Daily job
# ----------------------------------------
def daily_job(flag):
    # Get all active clusters from database
    clusters = db.get_clusters()

    for cluster in clusters:

        # Get tomorrow date
        today = dt.today()
        tomorrow = today + timedelta(days=1)
        if flag == 0:
            selected_date = today
        else:
            selected_date = tomorrow
        selected_date_str = str(selected_date.date().__format__(constant.FORMAT_DATE))

        # Get Scheduler data for tomorrow
        scheduler_record = db.get_scheduler(cluster.id, selected_date_str)
        if scheduler_record is None:
            print("Scheduler of cluster " + str(cluster.id) + " at " + selected_date_str +
                  " not found. We will temporarily skip this cluster")
            continue

        # Decode Scheduler and validate scheduler data
        scheduler = decode_schedule_content(scheduler_record.content)

        # Validate scheduler
        validate_scheduler_result = validate_scheduler(scheduler, cluster)
        if not validate_scheduler_result:
            print("Scheduler of cluster " + str(cluster.id) + " is not satisfy. We will temporarily skip this cluster")
            continue

        # Setup start time = 00:00:00 in tomorrow
        start = dt.strptime(selected_date_str + ' 00:00:00', constant.FORMAT_DATETIME)
        space = timedelta(minutes=cluster.space)

        # Setup start time
        # Default = 0
        next_space_order = 0

        # If start scraping right now, calculate space order
        if flag == 0:
            next_space_order = get_next_space_order(cluster.space)
            start += next_space_order * space
        print(start)

        # TEST
        # start = dt.strptime('2020-02-04 15:52:00', '%Y-%m-%d %H:%M:%S')
        # space = timedelta(seconds=20)

        # Send request to scrapyd
        # from START_TIME to end of tomorrow
        number_of_spaces = int(1440 / cluster.space)
        for space_order in range(next_space_order, number_of_spaces):
            start += space
            schedule.every().day.at(str(start.time())).do(space_job, cluster=cluster,
                                                          scheduler=scheduler, space_order=space_order,
                                                          execute_at=start)


# ----------------------------------------
# Space Job `space` minutes
# ----------------------------------------
def space_job(cluster, scheduler, space_order, execute_at):
    # Get data from infrastructure
    # db = Database()
    servers = db.get_servers(cluster.id)
    accounts = db.get_accounts(cluster.id)
    followings = db.get_followings(cluster.id)

    # Validation cluster data
    validation_cluster_result = validate_cluster(cluster, servers, accounts, followings)
    if not validation_cluster_result:
        print("Check data of cluster " + str(cluster.id) + " again. We will temporarily skip this cluster")

    # shuffle followings
    random.shuffle(followings)

    #    Split followings for concurrent accounts
    pages_per_account = int(len(followings) / len(scheduler))

    for j in range(len(servers)):
        # Get account
        selected_account = get_account_by_order(accounts, scheduler[j][space_order])

        # shuffle followings and divide for each account
        # selected_followings = get_selected_followings(
        #     followings[j * pages_per_account:(j + 1) * pages_per_account])

        selected_followings = followings[j * pages_per_account:(j + 1) * pages_per_account]
        # Schedule task for publishing messages
        print(str(execute_at.time()))

        for following in selected_followings:
            # Create task message
            job_message = JobMessage()
            job_message.server_ip = servers[j].ip
            job_message.project = servers[j].project
            job_message.spider = servers[j].fanpage_spider
            job_message.username = selected_account.username
            job_message.password = selected_account.password
            job_message.followings = "https://mbasic.facebook.com/" + following.url
            job_message.execute_at = execute_at.strftime(constant.FORMAT_DATETIME)

            # Save job into database
            db.save_job_message(job_message)

            # Ignore death account
            if selected_account.status != constant.STATUS_ACCOUNT_ACTIVATED:
                update_job_message = UpdateJobMessage()
                update_job_message.id = job_message.id
                update_job_message.status = constant.JOB_STATUS_STARTED_FAILED
                update_job_message.error_code = constant.JOB_ERROR_CODE_ACCOUNT_ERROR
                update_job_message.error_detail = 'Account has been disable before starting job'
                # rabbitmq.send_message(constant.RABBITMQ_MONITOR_QUEUE, update_monitor_message)
                broker.send_message(constant.MQTT_TOPIC_JOB_UPDATE, update_job_message)

                print("Skip task having account %s " % selected_account.username)
                continue

            # Send task message to slave
            broker.send_message(constant.MQTT_TOPIC_TASK, job_message)

    # db.curr.close()
    return schedule.CancelJob


# Run job in current day
daily_job(0)

# Run job in the next days
schedule.every().day.at(constant.START_TIME).do(daily_job, flag=1)

# Running
while True:
    schedule.run_pending()
