# -----------------------------------
#   Validate Cluster Data
#   1. check length of accounts, followings, servers
#   2. check the sequencing of accounts
# --------------------------------------------------
def validate_cluster(cluster, servers, accounts, followings):
    if cluster.number_of_followings != len(followings):
        return False
    if cluster.number_of_accounts != len(accounts):
        return False
    if cluster.number_of_servers != len(servers):
        return False

    # Check the sequencing of accounts
    flag = 0
    rowCount = 0
    for account in accounts:
        if account.order_in_cluster == rowCount:
            rowCount += 1
        else:
            flag = 1
            break
    if flag == 1:
        return False
    return True


# -----------------------------------
#   Validate Scheduler Data
#   1. check number_of_server, number_of_accounts
#   2. check 0 <= value of cell < number_of_accounts
# --------------------------------------------------
def validate_scheduler(scheduler, cluster):
    number_of_spaces = int(1440 / cluster.space)
    if len(scheduler) != cluster.number_of_servers:
        return False
    for i in range(len(scheduler)):
        if len(scheduler[i]) != number_of_spaces:
            return False
        for sch in scheduler[i]:
            if sch < 0 or sch >= cluster.number_of_accounts:
                return False
    return True