from infrastructure.database import Database

db = Database()


# ----------------------------------------
#   Update job info
# ----------------------------------------
def update_job_info(message):
    db.update_job_info(message)
