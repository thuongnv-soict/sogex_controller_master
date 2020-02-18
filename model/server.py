class Server:
    def __init__(self, id, ip, project, fanpage_spider, group_spider, username, password):
        self.id = id
        self.ip = ip
        self.project = project
        self.fanpage_spider = fanpage_spider
        self.group_spider = group_spider
        self.username = username
        self.password = password