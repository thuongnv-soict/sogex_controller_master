from infrastructure.database import Database

db = Database()


# ----------------------------------------
#   Switch account when account could not access facebook
# ----------------------------------------
def switch_account(message):
    print(message['user_email'])
    db.switch_account(message['user_email'])


# ----------------------------------------
#   Disable following when it has not been existed
# ----------------------------------------
def disable_following(message):
    print(message['following_url'])
    db.disable_following(message['following_url'])
