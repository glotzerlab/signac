from compdb.db.conversion import BasicFormat

class TrajectoryFile(BasicFormat):

    def __init__(self, data):
        self._data = data

    @property
    def data(self):
        return self._data

class HoomdXMLTrajectoryFile(TrajectoryFile):
    pass

class PosTrajectoryFile(TrajectoryFile):
    pass
