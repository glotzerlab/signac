import os.path

CONFIG_FILENAMES = ['compmatdb.rc',]
HOME = os.path.expanduser('~')
CWD = os.getcwd()

CONFIG_PATH = [HOME, CWD]

def read_config():
    args = dict()
    import json
    for filename in CONFIG_FILENAMES:
        for path in CONFIG_PATH:
            try:
                with open(os.path.join(path, filename)) as file:
                    args.update(json.loads(file.read()))
                except (IOError, ) as error:
                    continue
    return args

def read_environment():
    import os
    for var in ENVIRONMENT_VARIABLES:
        var = 
