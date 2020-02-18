from datetime import datetime as dt


# ----------------------------------------
#   Decode schedule content from string to 2D array
# ----------------------------------------
def decode_schedule_content(scheduler_string):
    result = []
    schedule_string_arr = scheduler_string.splitlines()
    for con in schedule_string_arr:
        sub_arr = []
        elements = con.split("|")
        for e in elements:
            sub_arr.append(int(e))
        result.append(sub_arr)

    return result


# ----------------------------------------
#   Get account by order in cluster
# ----------------------------------------
def get_account_by_order(accounts, order_in_cluster):
    for account in accounts:
        if account.order_in_cluster == order_in_cluster:
            return account
    return None


# ----------------------------------------
#   Get selected followings
# ----------------------------------------
def get_selected_followings(sub_followings):
    selected_followings = ""
    for following in sub_followings:
        selected_followings += "https://mbasic.facebook.com/" + following.url + " "
    return selected_followings[0:len(selected_followings) - 1]


# ----------------------------------------
#   Get next space order
# ----------------------------------------
def get_next_space_order(space):
    now = dt.now()
    next_space_order = int((now.hour * 60 + now.minute) / space)
    return next_space_order
