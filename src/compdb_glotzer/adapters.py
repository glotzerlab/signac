from compdb.db import Adapter
import freud

from . import formats

class PosFileToFreudTrajectory(Adapter):
    expects = formats.PosTrajectoryFile
    returns = freud.trajectory.Trajectory

    def convert(self, x):
        import tempfile
        with tempfile.NamedTemporaryFile() as file:
            file.write(x.data)
            file.flush()
            return freud.trajectory.TrajectoryPOS(file.name)

class XmlFileToFreudTrajectory(Adapter):
    expects = formats.HoomdXMLTrajectoryFile
    returns = freud.trajectory.Trajectory

    def convert(self, ):
        import tempfile
        with tempfile.NamedTemporaryFile() as file:
            file.write(x.data)
            file.flush()
            return freud.trajectory.TrajectoryXML([file.name])
