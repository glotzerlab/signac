from compdb.db import DBMethod
import freud

class CalcNumFrames(DBMethod):
    expects = freud.trajectory.Trajectory
    def apply(self, arg):
        return len(arg)

class GetNumberOfParticles(DBMethod):
    expects = freud.trajectory.Trajectory
    def apply(self, arg):
        return arg.numParticles()
