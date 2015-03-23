from compdb.db import formats, BasicFormat

class XMLFile(formats.FileFormat):
    pass

class TrajectoryFile(formats.FileFormat):
    pass

class HoomdXMLTrajectoryFile(TrajectoryFile, XMLFile):
    pass

class PosTrajectoryFile(TrajectoryFile):
    pass

class DCDTrajectoryFile(TrajectoryFile):
    pass

class SourceCodeFile(formats.FileFormat):
    pass

class SourceCodeHeaderFile(formats.FileFormat):
    pass

class ScriptFile(formats.FileFormat):
    pass

class SimulationInputFile(formats.FileFormat):
    pass

class HoomdInputFile(SimulationInputFile):
    pass
