from compdb.db import conversion
import freud

class CalcNumFrames(conversion.DBMethod):
    expects = freud.trajectory.Trajectory
    def apply(self, arg):
        return len(arg)

class GetNumberOfParticles(conversion.DBMethod):
    expects = freud.trajectory.Trajectory
    def apply(self, arg):
        return arg.numParticles()
