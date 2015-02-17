import json

def write_config():
    config = {
        'author_id': 'csadorf',
        'project_dir': '/nobackup/csadorf/example_project',
        'filestorage': '/nfs/glotzer/compmatdb/storage/',
        }
    print(json.dumps(config))

def read_config():
    with open('compmatdb.rc', 'r') as config_file:
        config = json.loads(config_file.read())

    print(config)

if __name__ == '__main__':
    #write_config()
    read_config()
