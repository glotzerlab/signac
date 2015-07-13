import logging
logger = logging.getLogger(__name__)

MSG_NO_DB_ACCESS = "Unable to connect to database host '{}'."
MSG_ENV_INCOMPLETE = "The following configuration variables are not set: '{}'.\nYou can use these commands to set them:"

def check_database_connection():
    from compdb.contrib import get_project
    project = get_project()
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure
    try:
        db = project._get_meta_db()
    except ConnectionFailure:
        print(MSG_NO_DB_ACCESS.format(db_host))
        return False
    else:
        return True

def check_global_config():
    from compdb.contrib import get_project
    project = get_project()

    keys = ['author_name', 'author_email', 'workspace_dir', 'filestorage_dir']
    missing = []
    for key in keys:
        if project.config.get(key) is None:
            missing.append(key)
    if len(missing):
        print()
        print(MSG_ENV_INCOMPLETE.format(missing))
        for key in missing:
            print("compdb config add {key} [your_value]".format(key = key))
        return False
    else:
        return True

def check_project_config_online():
    from compdb.contrib import get_project
    from tempfile import TemporaryDirectory
    import uuid, os

    project = get_project()
    project.get_id()
    checktoken = {'checktoken': str(uuid.uuid4())}
    checkvalue = str(uuid.uuid4())
    job = project.open_job(checktoken)
    try:
        with job:
            job.document['check'] = checkvalue
            assert job.document['check'] == checkvalue
            with job.storage.open_file('check.txt', 'wb') as file:
                file.write(checkvalue.encode())
            with open('check_wd.txt', 'wb') as file:
                file.write(checkvalue.encode())
            job.storage.store_file('check_wd.txt')

        with job:
            assert job.document.get('check') == checkvalue
            assert job.document['check'] == checkvalue
            with job.storage.open_file('check.txt', 'rb') as file:
                assert file.read().decode() == checkvalue
            job.storage.restore_file('check_wd.txt')
            assert os.path.exists('check_wd.txt')
            with open('check_wd.txt', 'rb') as file:
                assert file.read().decode() == checkvalue
    except:
        raise
    else:
        return True
    finally:
        job.remove()

def check_project_config_online_readonly():
    from . import get_project
    project = get_project()
    project.get_id()
    list(project.find(limit = 1))
    return True

def check_project_config_offline():
    from compdb.contrib import get_project
    from compdb.contrib.errors import ConnectionFailure
    from tempfile import TemporaryDirectory
    import uuid, os

    original_host = os.environ.get('COMPDB_DATABASE_HOST')
    original_timeout = os.environ.get('COMPDB_CONNECT_TIMEOUT')
    os.environ['COMPDB_DATABASE_HOST'] = 'example.com'
    os.environ['COMPDB_CONNECT_TIMEOUT'] = '100'
    try:
        project = get_project()
        checktoken = {'checktoken': str(uuid.uuid4())}
        checkvalue = str(uuid.uuid4())
        with TemporaryDirectory() as tmp_dir:
            job = project.open_job(checktoken)
    except:
        raise
    else:
        return True
    finally:
        if original_host is None:
            del os.environ['COMPDB_DATABASE_HOST']
        else:
            os.environ['COMPDB_DATABASE_HOST'] = original_host
        if original_timeout is None:
            del os.environ['COMPDB_CONNECT_TIMEOUT']
        else:
            os.environ['COMPDB_CONNECT_TIMEOUT'] = original_timeout

def check_project_version():
    import warnings
    from compdb.contrib import get_project
    project = get_project()
    return project._check_version()
