import logging
logger = logging.getLogger(__name__)

AUTH_NONE = 'none'
AUTH_SCRAM_SHA_1 = 'SCRAM-SHA-1'
AUTH_SSL = 'SSL'
AUTH_SSL_x509 = 'SSL-x509'

SUPPORTED_AUTH_MECHANISMS = [AUTH_NONE]
NOT_SUPPORTED_AUTH_MECHANISMS = [AUTH_SCRAM_SHA_1, AUTH_SSL, AUTH_SSL_x509]

def raise_unsupported_auth_mechanism(mechanism):
    msg = "Auth mechanism '{}' for snapshot creation and restoration currently not supported. Supported mechanisms: {}."
    raise ValueError(msg.format(mechanism, SUPPORTED_AUTH_MECHANISMS))

def dump_db(host, database, dst):
    import subprocess
    cmd = "mongodump --host {host} --db {database} --out {dst}"
    c = cmd.format(host = host, database = database, dst = dst)
    logger.debug("Trying to dump database with command: '{}'.".format(c))
    subprocess.check_output(c.split(), stderr = subprocess.STDOUT)

def restore_db(host, database, src):
    import subprocess
    cmd = "mongorestore --host {host} --db {database} {src}"
    c = cmd.format(host = host, database = database, src = src)
    logger.debug("Trying to restore database with command: '{}'.".format(c))
    subprocess.check_output(c.split(), stderr = subprocess.STDOUT)

#def dump_db_ssl(host, database, dst, cachain, cakey):
#    import subprocess
#    cmd = "mongodump --host {host} --db {database} --out {dst} --ssl --sslCAFile={cachain} --sslPEMKeyFile={cakey}"
#    c = cmd.format(
#        host = host, database = database, dst = dst, 
#        cachain = cachain, cakey = cakey)
#    logger.debug("Trying to dump database with command: '{}'.".format(c))
#    subprocess.check_output(c.split(), stderr = subprocess.STDOUT)
#
#def restore_db_ssl(host, database, src, cachain, cakey):
#    import subprocess
#    cmd = "mongorestore --host {host} --db {database} --ssl --sslCAFile={cachain} -sslPEMKeyFile={cakey} {src}"
#    c = cmd.format(
#        host = host, database = database, src = src,
#        cachain = cachain, cakey = cakey)
#    logger.debug("Trying to restore database with command: '{}'.".format(c))
#    subprocess.check_output(c.split(), stderr = subprocess.STDOUT)

def dump_db_from_config(config, dst):
    auth_mechanism = config['database_auth_mechanism']
    if auth_mechanism in (AUTH_NONE, AUTH_SCRAM_SHA_1):
        dump_db(
            host = config['database_host'],
            database = config['project'],
            dst = dst)
    elif auth_mechanism in (AUTH_SSL, AUTH_SSL_x509):
        raise_unsupported_auth_mechanism(auth_mechanism)
        #dump_db_ssl(
        #    host = config['database_host'],
        #    database = config['project'],
        #    dst = dst,
        #    cachain = config['database_ssl_ca_certs'],
        #    cakey = config['database_ssl_cakeypemfile'])
    else:
        raise_unsupported_auth_mechanism(auth_mechanism)

def restore_db_from_config(config, src):
    auth_mechanism = config['database_auth_mechanism']
    if auth_mechanism in (AUTH_NONE, AUTH_SCRAM_SHA_1):
        restore_db(
            host = config['database_host'],
            database = config['project'],
            src = src)
    elif auth_mechanism in (AUTH_SSL, AUTH_SSL_x509):
        raise_unsupported_auth_mechanism(auth_mechanism)
        #restore_db_ssl(
        #    host = config['database_host'],
        #    database = config['project'],
        #    src = src,
        #    cachain = config['database_ssl_ca_certs'],
        #    cakey = config['database_ssl_cakeypemfile'])
    else:
        raise_unsupported_auth_mechanism(auth_mechanism)

