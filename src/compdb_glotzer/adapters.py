from compdb.db import conversion
import freud

from . import formats

class PosFileToFreudTrajectory(conversion.Adapter):
    expects = formats.PosTrajectoryFile
    returns = freud.trajectory.Trajectory

    def convert(self, x):
        import tempfile
        with tempfile.NamedTemporaryFile() as file:
            file.write(x.data)
            file.flush()
            return freud.trajectory.TrajectoryPOS(file.name)

