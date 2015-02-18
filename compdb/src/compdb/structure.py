class Structure(object):

    def __init__(self, name):
        self._name = None
        self._file = None
        self._fileformat = None

        self.name = name

    def _set_name(self, value):

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def name(self):
        return self._name

    def export(self):
        return dict(self)

class Molecule(Structure):
    
    def __init__(self, name):
        super().__init(name)
