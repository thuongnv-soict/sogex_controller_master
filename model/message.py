import json
import uuid


class JobMessage:
    def __init__(self):
        self.id = str(uuid.uuid4())
        self.server_ip = None
        self.project = None
        self.spider = None
        self.username = None
        self.password = None
        self.followings = None
        self.execute_at = None

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True, indent=4)


class UpdateJobMessage:
    def __init__(self):
        self.id = None
        self.real_execute_at = None
        self.status = None
        self.error_code = None
        self.error_detail = None
        self.finished_at = None

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True, indent=4)