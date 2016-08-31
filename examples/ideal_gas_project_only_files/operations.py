def calc_volume(N, kT, p):
    "Compute the volume of an ideal gas."
    return N * kT / p

def compute_volume(job):
    "Compute the volume of this statepoint."
    sp = job.statepoint()
    with job:
        V = calc_volume(sp['N'], sp['kT'], sp['p'])
        with open('V.txt', 'w') as file:
            file.write(str(V)+'\n')
        print(job, 'computed volume')
