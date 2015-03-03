def dump_db(host, database, dst):
    import subprocess
    cmd = "mongodump --host {host} --db {database} --out {dst}"
    c = cmd.format(host = host, database = database, dst = dst)
    subprocess.check_output(c.split(), stderr = subprocess.STDOUT)

def restore_db(host, database, src):
    import subprocess
    cmd = "mongorestore --host {host} --db {database} {src}"
    c = cmd.format(host = host, database = database, src = src)
    subprocess.check_output(c.split(), stderr = subprocess.STDOUT)
