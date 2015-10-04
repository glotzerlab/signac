import logging
import argparse
import sys
import code
import getpass

from ..core.config import load_config
from ..core.dbclient_connector import DBClientConnector
from ..core.utility import get_subject_from_certificate
from ..core.dbclient_connector import SUPPORTED_AUTH_MECHANISMS
from ..contrib import get_all_project_ids, get_basic_project_from_id
from ..contrib.utility import add_verbosity_argument, set_verbosity_level

logger = logging.getLogger(__name__)

def connect_and_authenticate(args):
    config = load_config()
    prefix = 'database_'
    if args.certificate is not None:
        config[prefix+'ssl_certfile'] = args.certificate
    if args.cacertificate is not None:
        config[prefix+'ssl_ca_certs'] = args.cacertificate
    if args.auth is not None:
        config[prefix+'auth_mechanism'] = args.auth
    connector = DBClientConnector(config, prefix = prefix)
    connector.connect()
    connector.authenticate()
    return connector.client

def add_x509_user(client, subject, databases, roles):
    db_auth = client['$external']
    kwargs = {'roles' : [{'role': role, 'db': db}] for role in roles for db in databases}
    return db_auth.command('createUser', username, ** kwargs)

def add_scram_sha1_user(client, username, password, databases, roles):
    db_auth = client['admin']
    kwargs = {
        'roles' : [{'role': role, 'db': db}] for role in roles for db in databases}
    kwargs['pwd'] = password
    return db_auth.command('createUser', username, ** kwargs)

def grant_roles_to_user(db_auth, user, databases, roles):
    values = {'roles': [{'role': role, 'db': db} for role in roles for db in databases]}
    return db_auth.command('grantRolesToUser', user, ** values)

def revoke_roles_from_user(db_auth, user, databases, roles):
    values = {'roles': [{'role': role, 'db': db} for role in roles for db in databases]}
    return db_auth.command('revokeRolesFromUser', user, ** values)

def get_username(args, default = None):
    if args.username and args.usercertificate:
        raise ValueError("Either supply user name or user certificate, not both.")
    if args.username is not None:
        return args.username
    elif args.usercertificate is not None:
        return get_subject_from_certificate(args.usercertificate)
    else:
        return default

def manage_shell(args):
    client = connect_and_authenticate(args)
    banner = "Use the 'client' variable to interact with the pymongo client."
    code.interact(banner = banner, local = {'client': client})

def display_status(args):
    client = connect_and_authenticate(args)
    status = client.admin.command("serverStatus")
    for key, value in status.items():
        print(key)
        print(value)
        print()

def display_info(args):
    client = connect_and_authenticate(args)
    for project_id in get_all_project_ids(client):
        project = get_basic_project_from_id(project_id, client=client)
        n_jobs = project.num_active_jobs()
        print(project_id)
        print("Active jobs: {num}".format(num=n_jobs))
        print()

def manage_user(args):
    client = connect_and_authenticate(args)
    msg = "Managing user '{}'."
    username = get_username(args)
    if args.command == 'show':
        for db_auth in client['$external'], client['admin']:
            print("Authorizing DB: {}".format(db_auth))
            if username is None:
                info = db_auth.command('usersInfo')
            else:
                info = db_auth.command('usersInfo', username)
            for user in info['users']:
                print('user: {}'.format(user['user']))
                print('roles: {}'.format(user['roles']))
                print()

    elif args.command == 'add':
        if args.database is None:
            raise ValueError("Specify database to manage.")
        if args.username is not None:
            password = getpass.getpass("Password for new user: ")
            password2 = getpass.getpass("Confirm password: ")
            if password != password2:
                raise ValueError("Passwords do not match.")
            db_auth = client['admin']
            print(add_scram_sha1_user(client, username, password, [args.database], args.roles))
            print(grant_roles_to_user(db_auth, username, [args.database], args.roles))
        elif args.usercertificate is not None:
            result = add_x509_user(client, username, [args.database], args.roles)
            print(result)
        else:
            raise ValueError("Specify username or user certificate.")
    elif args.command == 'remove':
        if args.external:
            db_auth = client['$external']
        else:
            db_auth = client['admin']
        db_auth.remove_user(username)
        print("OK")
    elif args.command in ('grant', 'revoke'):
        if args.database is None:
            raise ValueError("Specify database to manage.")
        if args.username is not None:
            db_auth = client['admin']
        elif args.usercertificate is not None:
            db_auth = client['$external']
        if args.command == 'grant':
            result = grant_roles_to_user(db_auth, username, [args.database], args.roles)
        elif args.command == 'revoke':
            result = revoke_roles_from_user(db_auth, username, [args.database], args.roles)
        else:
            assert 0
        if result['ok']:
            print('OK')
        else:
            raise RuntimeError("Command failed.")
    else:
        raise ValueError("Invalid command '{}'.".format(args.command))

def main():
    parser = argparse.ArgumentParser(
        description = "Administrative management of signac.")
    add_verbosity_argument(parser)
    parser.add_argument(
        '-c', '--certificate',
        type = str,
        help = "The certificate to be used for authentication.",
        )
    parser.add_argument(
        '--cacertificate',
        type = str,
        help = "The ca certificate used for authentication.")
    parser.add_argument(
        '-a', '--auth',
        type = str,
        choices = SUPPORTED_AUTH_MECHANISMS,
        help = "The auth mode to use.")
    parser.add_argument(
        '-d', '--database',
        type = str,
        help = "The database to manage.")

    subparsers = parser.add_subparsers()

    parser_shell = subparsers.add_parser(
        'shell',
        description = "Enter an interactive mongo shell.")
    parser_shell.set_defaults(func = manage_shell)

    parser_info = subparsers.add_parser(
        'info',
        description = "Print administrative information.")
    parser_info.set_defaults(func = display_info)

    parser_user = subparsers.add_parser(
        'user',
        description = 'Show and manage user roles.')
    parser_user.add_argument(
        'command',
        type = str,
        choices = ['show', 'add', 'remove', 'grant', 'revoke'],
        )
    parser_user.add_argument(
        '-u', '--username',
        type = str,
        help = "The name of the user to manage.")
    parser_user.add_argument(
        '-c', '--certificate',
        type = str,
        dest = 'usercertificate',
        help = "The certificate of the user to manage.")
    parser_user.add_argument(
        '-r', '--roles',
        type = str,
        nargs = '+',
        default = ['read'],
        help = "The roles to be granted/ revoked.")
    parser_user.add_argument(
        '--external',
        action = 'store_true',
        help = "Use the external database for user management. Automatically used for certificates.")
    parser_user.set_defaults(func = manage_user)

    parser_status = subparsers.add_parser(
        'status',
        description = "Display status information.")
    parser_status.set_defaults(func = display_status)

    args = parser.parse_args()
    set_verbosity_level(args.verbosity)
    try:
        if 'func' in args:
            args.func(args)
        else:
            parser.print_usage()
    except Exception as error:
        if args.verbosity > 0:
            raise
        else:
            print("Error: {}".format(error))
            sys.exit(1)
    else:
        sys.exit(0)

if __name__ == '__main__':
    main()
