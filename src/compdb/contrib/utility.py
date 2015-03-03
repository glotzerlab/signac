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

def query_yes_no(question, default="yes"):
    """Ask a yes/no question via input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
    It must be "yes" (the default), "no" or None (meaning
    an answer is required of the user).

    The "answer" return value is one of "yes" or "no".
    """
    import sys
    valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = raw_input().lower() if sys.hexversion < 0x03000000 else input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")

# Usage example
#
#>>> query_yes_no("Is cabbage yummier than cauliflower?")
#Is cabbage yummier than cauliflower? [Y/n] oops
#Please respond with 'yes' or 'no' (or 'y' or 'n').
#Is cabbage yummier than cauliflower? [Y/n] y
#>>> True
