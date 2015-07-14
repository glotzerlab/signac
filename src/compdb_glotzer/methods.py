import freud

from compdb.db import DBMethod

class CalcNumFrames(DBMethod):
    expects = freud.trajectory.Trajectory
    def apply(self, arg):
        return len(arg)

class GetNumberOfParticles(DBMethod):
    expects = freud.trajectory.Trajectory
    def apply(self, arg):
        return arg.numParticles()
