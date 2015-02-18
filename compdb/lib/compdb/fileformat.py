class Fileformat(object):
    
    def __init__(self):
        self._extensions = []

    def parse(self, data):
        raise NotImplementedError()

    def write(self):
        pass
