class Account:
    def __init__(self, id, username, password, cluster, order_in_cluster, status):
        self.id = id
        self.username = username
        self.password = password
        self.cluster = cluster
        self.order_in_cluster = order_in_cluster
        self.status = status