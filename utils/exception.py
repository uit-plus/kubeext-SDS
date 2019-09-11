class LessArgException(Exception):
    def __init__(self, reason, message):
        self.reason = reason
        self.message = message

class PoolExistException(Exception):
    def __init__(self, msg):
        self.msg = msg

class DiskExistException(Exception):
    def __init__(self, reason, message):
        self.reason = reason
        self.message = message

class PoolNotExistException(Exception):
    def __init__(self, msg):
        self.msg = msg

class DiskNotExistException(Exception):
    def __init__(self, reason, message):
        self.reason = reason
        self.message = message

class NotSupportValueException(Exception):
    def __init__(self, reason, message):
        self.reason = reason
        self.message = message


class ExecuteException(Exception):
    def __init__(self, reason, message):
        self.reason = reason
        self.message = message
