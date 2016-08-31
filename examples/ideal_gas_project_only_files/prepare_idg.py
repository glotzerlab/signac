# prepare_idg.py
from fractions import Fraction
import fractions
import signac

project = signac.get_project()
cmd = 'bash idg.sh {N} {kT} {p_n} {p_d} > {out}'
for job in project.find_jobs():
    sp = job.statepoint()
    p = Fraction(sp['p'])
    print(cmd.format(
        N=int(sp['N']),
        kT=int(sp['kT']),
        p_n=p.numerator,
        p_d=p.denominator,
        out=job.fn('V.txt')))
