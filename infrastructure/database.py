import logging
import time
import os
import mysql.connector
from dotenv import load_dotenv
import constant

from model.account import Account
from model.cluster import Cluster
from model.server import Server
from model.following import Following
from model.scheduler import Scheduler


class Database:
    def __init__(self):
        self.connection = None
        self.curr = None
        self.connect()

    # ----------------------------------------
    #   Connect to MYSQL Server
    # ----------------------------------------
    def connect(self):
        load_dotenv()
        try:
            self.connection = mysql.connector.Connect(
                host=os.getenv('MYSQL_HOST'),
                user=os.getenv('MYSQL_USERNAME'),
                passwd=os.getenv('MYSQL_PASSWORD'),
                database=os.getenv('MYSQL_DATABASE')
            )
            self.curr = self.connection.cursor()
        except mysql.connector.Error as exc:
            logging.error("Connect to MYSQL server failed")
            while True:
                logging.info("Reconnect to MYSQL server...")
                self.connect()
                if self.connection.is_connected():
                    break
                time.sleep(5)

    # ----------------------------------------
    #   Get all active clusters
    # ----------------------------------------
    def get_clusters(self):
        clusters = []
        try:
            query = """SELECT Id, Name, Space, NumberOfAccounts, NumberOfServers, NumberOfFollowings 
                        FROM clusters 
                        WHERE Status = %s ORDER BY Id ASC"""
            data_tuple = (constant.STATUS_CLUSTER_ACTIVATED,)
            self.curr.execute(query, data_tuple)
            result = self.curr.fetchall()
            for r in result:
                cluster = Cluster(r[0], r[1], r[2], r[3], r[4], r[5])
                clusters.append(cluster)
            result.clear()
        except mysql.connector.Error as error:
            logging.error('MySQL server is disconnected: ', str(error))
            logging.info('Reconnect to Mysql server..')
            self.connect()
            self.get_clusters()
        return clusters

    # ----------------------------------------
    #   Get all accounts in specific cluster
    # ----------------------------------------
    def get_accounts(self, cluster_id):
        accounts = []
        try:
            query = """SELECT Id, Username, Password, Cluster, OrderInCluster, Status
                        FROM accounts 
                        WHERE (Status = %s OR Status = %s) AND Cluster = %s ORDER BY OrderInCluster ASC;"""
            data_tuple = (constant.STATUS_ACCOUNT_ACTIVATED, constant.STATUS_ACCOUNT_ERROR_NOT_REPLACED, cluster_id)
            self.curr.execute(query, data_tuple)
            result = self.curr.fetchall()
            for r in result:
                account = Account(r[0], r[1], r[2], r[3], r[4], r[5])
                accounts.append(account)
            result.clear()

        except mysql.connector.Error as error:
            logging.error('MySQL server is disconnected: ', str(error))
            logging.info('Reconnect to Mysql server..')
            self.connect()
            self.get_accounts(cluster_id)
        return accounts

    # ----------------------------------------
    #   Get all followings in specific cluster
    # ----------------------------------------
    def get_followings(self, cluster_id):
        followings = []
        try:
            query = """SELECT Id, SocialMedia, Url, Name, Type, Cluster 
                        FROM followings 
                        WHERE Status = %s AND Cluster = %s;"""
            data_tuple = (constant.STATUS_FOLLOWING_ACTIVATED, cluster_id)
            self.curr.execute(query, (1, cluster_id))
            result = self.curr.fetchall()
            for r in result:
                following = Following(r[0], r[1], r[2], r[3], r[4], r[5])
                followings.append(following)
            result.clear()
        except mysql.connector.Error as error:
            logging.error('MySQL server is disconnected: ', str(error))
            logging.info('Reconnect to Mysql server..')
            self.connect()
            self.get_followings(cluster_id)
        return followings

    # ----------------------------------------
    #   Get all servers in specific cluster
    # ----------------------------------------
    def get_servers(self, cluster_id):
        servers = []
        try:
            query = """SELECT Id, IP, Project, FanPageSpider, GroupSpider, Username, Password 
                        FROM servers 
                        WHERE Status = %s AND Cluster =  %s;"""
            data_tuple = (constant.STATUS_SERVER_ACTIVATED, cluster_id)
            self.curr.execute(query, data_tuple)
            result = self.curr.fetchall()
            for r in result:
                server = Server(r[0], r[1], r[2], r[3], r[4], r[5], r[6])
                servers.append(server)
            result.clear()
        except mysql.connector.Error as error:
            logging.error('MySQL server is disconnected: ', str(error))
            logging.info('Reconnect to Mysql server..')
            self.connect()
            self.get_servers(cluster_id)
        return servers

    # ----------------------------------------
    #   Get scheduler for specific cluster in specific date
    # ----------------------------------------
    def get_scheduler(self, cluster_id, date):
        scheduler = None
        try:
            query = """SELECT Id, Date, Cluster, Content
                        FROM schedulers 
                        WHERE Status = %s AND Cluster =  %s AND Date = DATE_FORMAT(%s, "%Y-%m-%d");"""
            data_tuple = (1, cluster_id, date)
            self.curr.execute(query, data_tuple)
            result = self.curr.fetchone()
            if result is not None:
                scheduler = Scheduler(result[0], result[1], result[2], result[3])
        except mysql.connector.Error as error:
            logging.error('MySQL server is disconnected: ', str(error))
            logging.info('Reconnect to Mysql server..')
            self.connect()
            self.get_scheduler(cluster_id, date)
        return scheduler

    #   1. Disable following
    #       Update status for this following
    #   2. Update job info
    def disable_following(self, following):
        # Update in table followings
        update = """UPDATE following SET status = %s,  WHERE Url = %s;"""
        data_tuple = (constant.STATUS_FOLLOWING_ERROR, following)
        self.curr.execute(update, data_tuple)

        # Update in table job
        # To do
        self.connection.commit()

    # ----------------------------------------
    #    1. Switch account
    #        If (exist backup account in current cluster)
    #            switch error account to backup account
    #            change status for both accounts
    #        Else:
    #           change status for error account
    #    2. Update job info
    # ----------------------------------------
    def switch_account(self, username):

        # Switch account
        try:
            # Get information of error account through username
            query = """SELECT Id, Username, Password, Cluster, OrderInCluster, Status
                        FROM accounts 
                        WHERE Username = %s;"""
            data_tuple = (username,)
            self.curr.execute(query, data_tuple)
            result = self.curr.fetchone()
            if result is not None:
                error_account = Account(result[0], result[1], result[2], result[3], result[4], result[5])
            else:
                logging.error('Cannot find any account has username = ' + username)
                logging.info('Failed to change error account ' + username)
                return

            # Check if account has been replaced
            if error_account.status == constant.STATUS_ACCOUNT_ERROR_REPLACED:
                logging.info('Error account has been replaced: ' + username)
                return

            # Get information of backup account
            backup_account = None
            query = """SELECT Id, Username, Password, Cluster, OrderInCluster, Status
                                FROM accounts 
                                WHERE Status = %s AND Cluster =  %s;"""
            data_tuple = (constant.STATUS_ACCOUNT_BACKUP, error_account.cluster)
            self.curr.execute(query, data_tuple)
            result = self.curr.fetchone()
            if result is not None:
                backup_account = Account(result[0], result[1], result[2], result[3], result[4], result[5])
                # Update error account
                update = """UPDATE accounts SET status = %s, OrderInCluster = -1 WHERE Username = %s;"""
                data_tuple = (constant.STATUS_ACCOUNT_ERROR_REPLACED, username)
                self.curr.execute(update, data_tuple)
                self.connection.commit()

                # Update backup account
                update = """UPDATE accounts SET status = %s, OrderInCluster = %s WHERE Username = %s;"""
                data_tuple = (constant.STATUS_ACCOUNT_ERROR_REPLACED, error_account.order_in_cluster,
                              backup_account.username)
                self.curr.execute(update, data_tuple)
                self.connection.commit()

                logging.info("Replace account from " + username + " to " + backup_account.username)
                # result.clear()
            else:
                # Update error account
                update = """UPDATE accounts SET status = %s WHERE Username = %s;"""
                data_tuple = (constant.STATUS_ACCOUNT_ERROR_NOT_REPLACED, username)
                self.curr.execute(update, data_tuple)
                self.connection.commit()

                print("Disable account " + username)
                logging.error('Cannot find any backup account')
                logging.info('Failed to change error account ' + username)
                return

            # Update job info

        except mysql.connector.Error as error:
            logging.error('MySQL server is disconnected: ', error)
            logging.info('Reconnect to Mysql server..')
            self.connect()

    # ----------------------------------------
    #   Insert new job into database
    # ----------------------------------------
    def save_job_message(self, message):
        # Insert  query
        insert_query = """INSERT INTO scraping_jobs(`Id`, `ServerIP`, `Project`, `Spider`, `Account`, `Following`, 
                `ExpectedExecuteAt`, `Status`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

        # Data tuple for query
        data_tuple = (message.id, message.server_ip, message.project, message.spider,
                      message.username, message.followings, message.execute_at, constant.JOB_STATUS_SEND_SUCCESSFULLY)

        # Execute query
        self.curr.execute(insert_query, data_tuple)
        self.connection.commit()

        # Verify query result
        if self.curr.rowcount >= 1:
            print('Insert %s successfully.' % message.id)
        else:
            print("Insert %s failed." % message.id)

    # ----------------------------------------
    #   Update job info
    # ----------------------------------------
    def update_job_info(self, message):
        # Update query
        update_query = """UPDATE scraping_jobs SET 
                            RealExecuteAt = CASE WHEN %s IS NOT NULL THEN %s END,
                            Status =        CASE WHEN %s IS NOT NULL THEN %s END,
                            FinishedAt =    CASE WHEN %s IS NOT NULL THEN %s END,
                            ErrorCode =     CASE WHEN %s IS NOT NULL THEN %s END,
                            ErrorDetail =   CASE WHEN %s IS NOT NULL THEN %s END
                        WHERE Id = %s"""

        # Data tuple for query
        data_tuple = (message['real_execute_at'], message['real_execute_at'], message['status'], message['status'],
                      message['finished_at'], message['finished_at'], message['error_code'], message['error_code'],
                      message['error_detail'], message['error_detail'], message['id'])

        # Execute query
        self.curr.execute(update_query, data_tuple)
        self.connection.commit()

        # Verify query result
        if self.curr.rowcount >= 1:
            print('Update %s successfully.' % message['id'])
        else:
            print("Update %s failed." % message['id'])
