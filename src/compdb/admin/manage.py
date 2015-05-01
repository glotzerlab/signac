import logging
logger = logging.getLogger('compdb.admin.manage')

def connect_and_authenticate(args):
    from ..core.config import load_config
    from ..core.dbclient_connector import DBClientConnector
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
    db_external = client['$external']
    values = {'roles' : [{'role': role, 'db': db}] for role in roles for db in databases}
    return db_external.command('createUser', subject, ** values)

def grant_roles_to_user(db_auth, user, databases, roles):
    values = {'roles': [{'role': role, 'db': db} for role in roles for db in databases]}
    return db_auth.command('grantRolesToUser', user, ** values)

def revoke_roles_from_user(db_auth, user, databases, roles):
    values = {'roles': [{'role': role, 'db': db} for role in roles for db in databases]}
    return db_auth.command('revokeRolesFromUser', user, ** values)

def get_username(args, default = None):
    from ..core.utility import get_subject_from_certificate
    if args.username and args.usercertificate:
        raise ValueError("Either supply user name or user certificate, not both.")
    if args.username is not None:
        return args.username
    elif args.usercertificate is not None:
        return get_subject_from_certificate(args.usercertificate)
    else:
        return default

def manage_shell(args):
    import code
    client = connect_and_authenticate(args)
    banner = "Use the 'client' variable to interact with the pymongo client."
    code.interact(banner = banner, local = {'client': client})

def manage_user(args):
    client = connect_and_authenticate(args)
    db = client['$external']
    msg = "Managing user '{}'."
    username = get_username(args)
    if args.command == 'show':
        if username is None:
            info = db.command('usersInfo')
        else:
            info = db.command('usersInfo', username)
        for user in info['users']:
            print('user: {}'.format(user['user']))
            print('roles: {}'.format(user['roles']))
            print()

    elif args.command == 'add':
        if args.database is None:
            raise ValueError("Specify database to manage.")
        if args.username is not None:
            raise NotImplementedError("Adding user by name not supported.")
        elif args.usercertificate is not None:
            result = add_x509_user(client, username, [args.database], args.roles)
            print(result)
        else:
            raise ValueError("Specify username or user certificate.")
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
    import argparse, sys
    from ..contrib.utility import add_verbosity_argument, set_verbosity_level
    from ..core.dbclient_connector import SUPPORTED_AUTH_MECHANISMS
    parser = argparse.ArgumentParser(
        description = "Administrative management of compdb.")
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
    parser_user.set_defaults(func = manage_user)

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
