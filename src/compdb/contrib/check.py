import logging
logger = logging.getLogger('compdb.check')

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
        logger.warning(MSG_NO_DB_ACCESS.format(db_host))
        return False

def check_global_config():
    from compdb.contrib import get_project
    project = get_project()

    keys = ['author_name', 'author_email', 'workspace_dir', 'filestorage_dir']
    missing = []
    for key in keys:
        if project.config.get(key) is None:
            missing.append(key)
    if len(missing):
        print(MSG_ENV_INCOMPLETE.format(missing))
        for key in missing:
            print("compdb config add {key} [your_value]".format(key = key))
        return False
    else:
        return True

def check_project_config():
    from compdb.contrib import get_project
    from tempfile import TemporaryDirectory
    import uuid, os

    project = get_project()
    checktoken = {'checktoken': str(uuid.uuid4())}
    checkvalue = str(uuid.uuid4())
    with TemporaryDirectory() as tmp_dir:
        job = project.open_job(checktoken)
        try:
            with job:
                job.document['check'] = checkvalue
                with job.storage.open_file('check.txt', 'wb') as file:
                    file.write(checkvalue.encode())
                with open('check_wd.txt', 'wb') as file:
                    file.write(checkvalue.encode())
                job.storage.store_file('check_wd.txt')

            with job:
                assert job.document['check'] == checkvalue
                with job.storage.open_file('check.txt', 'rb') as file:
                    assert file.read().decode() == checkvalue
                job.storage.restore_file('check_wd.txt')
                assert os.path.exists('check_wd.txt')
                with open('check_wd.txt', 'rb') as file:
                    assert file.read().decode() == checkvalue
        except:
            raise
        finally:
            job.remove()
