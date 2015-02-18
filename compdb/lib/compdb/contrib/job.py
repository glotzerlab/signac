from .. config import CONFIG

JOB_STATUS_KEY = 'status'
JOB_ERROR_KEY = 'error'
JOB_NAME_KEY = 'name'
JOB_PARAMETERS_KEY = 'parameters'

def valid_name(name):
    return not name.startswith('_compdb')

def _get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient(CONFIG['database']['host'])
    return client[db_name]

def get_db(db_name):
    assert valid_name(db_name)
    return _get_db(db_name)

def get_meta_db():
    return _get_db(CONFIG['database_meta'])

def get_jobs_collection():
    return get_meta_db()['jobs']

def get_project_db():
    return get_db(CONFIG['project'])

def filestorage_dir():
    return CONFIG['filestorage_dir']

def job_spec(name, parameters):
    spec = {JOB_NAME_KEY: name}
    if parameters is not None:
        spec.update({JOB_PARAMETERS_KEY: parameters})
    return spec

class Job(object):
    
    def __init__(self, spec):
        self._spec = spec
        self._jobs_collection = get_meta_db()['jobs']

    @property
    def spec(self):
        return self._spec

    def get_working_directory(self):
        import os.path
        return os.path.join(CONFIG['working_dir'], str(self.get_id()))

    def __enter__(self):
        import os
        _id = get_jobs_collection().save(self._spec)
        assert self._spec['_id'] == _id
        if not os.path.isdir(self.get_working_directory()):
            os.makedirs(self.get_working_directory())
        os.chdir(self.get_working_directory())
        get_jobs_collection().update(
            self.spec, {'$set': {JOB_STATUS_KEY: 'open'}})
        return self

    def __exit__(self, err_type, err_value, traceback):
        get_jobs_collection().update(
            self.spec, {'$set': {JOB_STATUS_KEY: 'closed'}})
        if err_type is not None:
            err_doc = '{}:{}'.format(err_type, err_value)
            get_jobs_collection().update(
                self.spec, {'$push': {JOB_ERROR_KEY: err_doc}})

    def get_id(self):
        return self.spec.get('_id', None)

    @property
    def collection(self):
        return get_project_db()['job_{}'.format(self.get_id())]

    def open_file(self, file, * args, ** kwargs):
        from os.path import join
        fn = join(self.get_working_directory(), file)
        return open(fn, * args, ** kwargs)
