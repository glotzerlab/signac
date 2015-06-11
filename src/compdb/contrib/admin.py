#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
logger = logging.getLogger(__name__)

def welcome_msg(project):
    msg = "Administrating project '{}':"
    print(msg.format(project))

def get_project(args):
    from . import get_project
    return get_project()

def get_client(project):
    return project._get_client()

def get_username(args):
    from os.path import isfile
    if args.user is None:
        return None
    if isfile(args.user):
        from ..core.utility import get_subject_from_certificate
        username = get_subject_from_certificate
        return get_subject_from_certificate(args.user)
    else:
        return args.user

def get_roles(args):
    LEGAL_ROLES = ['read', 'readWrite']
    try:
        if args.readonly:
            args.role = 'read'
        else:
            args.role = 'readWrite'
    except AttributeError:
        pass
    if not args.role in LEGAL_ROLES:
        msg = "Legal roles are {}."
        raise ValueError(msg.format(LEGAL_ROLES))
    return [args.role]

def get_db_auth(client, args):
    if args.ssl:
        return client['$external']
    else:
        return client['admin']

def user_exists(client, args):
    username = get_username(args)
    db_auth = get_db_auth(client, args)
    info = db_auth.command('usersInfo', username)
    return bool(info['users'])

def add_user(args):
    project = get_project(args)
    welcome_msg(project)
    client = get_client(project)
    #dbs = [project.get_id()]
    username = get_username(args)
    if user_exists(client, args):
        _grant_revoke_roles(args, False, project)
    else:
        add_user_to_db(project, client, username, args)

def add_user_to_db(project, client, username, args):
    dbs = [project.get_id()]
    roles = get_roles(args)
    if args.ssl:
        from ..admin.manage import add_x509_user 
        add_x509_user(client, username, dbs, roles)
    else:
        from ..admin.manage import add_scram_sha1_user
        import getpass
        msg = "Enter password for new user '{}': "
        password = getpass.getpass(msg.format(username))
        password2 = getpass.getpass("Confirm password: ")
        if password != password2:
            raise ValueError("Passwords do not match.")
        print("Adding user '{}' to database.".format(username))
        result = add_scram_sha1_user(client, username, password, dbs, roles)
        if result['ok']:
            print('OK.')
        else:
            raise RuntimeError(result)

def remove_user(args):
    from . utility import query_yes_no
    project = get_project(args)
    welcome_msg(project)
    client = get_client(project)
    db_auth = get_db_auth(client, args)
    username = get_username(args)
    if not user_exists(client, args):
        msg = "User with username '{}', not found."
        raise ValueError(msg.format(username))
    q = "Are you sure that you want to remove user '{}' from the database?"
    if args.yes or query_yes_no(q.format(username), 'no'):
        db_auth.remove_user(username)
        print("OK.")

def grant_roles(args):
    return grant_revoke_roles(args, revoke = False)

def revoke_roles(args):
    return grant_revoke_roles(args, revoke = True)

def grant_revoke_roles(args, revoke):
    project = get_project(args)
    welcome_msg(project)
    return _grant_revoke_roles(args, revoke, project)

def _grant_revoke_roles(args, revoke, project):
    from ..admin.manage import grant_roles_to_user
    from ..admin.manage import revoke_roles_from_user
    client = get_client(project)
    db_auth = get_db_auth(client, args)
    username = get_username(args)
    if not user_exists(client, args):
        msg = "User '{}' not registered in database."
        raise RuntimeError(msg.format(username))
    dbs = [project.get_id()]
    roles = get_roles(args)
    if revoke:
        msg = "Revoking roles '{}' from user '{}'."
        print(msg.format(roles, username))
        result = revoke_roles_from_user(db_auth, username, dbs, roles)
    else:
        msg = "Granting roles '{}' to user '{}'."
        print(msg.format(roles, username))
        result = grant_roles_to_user(db_auth, username, dbs, roles)
    if result['ok']:
        print("OK")
    else:
        raise RuntimeError(result)

def collect_users(info, dbs):
    for entry in info['users']:
        for role in entry['roles']:
            if role['db'] in dbs:
                yield entry['user']
                break

def collect_roles(info, dbs, username):
    roles = []
    for entry in info['users']:
        if entry['user'] == username:
            for role in entry['roles']:
                if role['db'] in dbs:
                    yield role['role']
                    break

def show_users(args):
    project = get_project(args)
    welcome_msg(project)
    client = get_client(project)
    username = get_username(args)
    dbs = ['admin', project.get_id()]
    dbs_auth = client['admin'], client['$external']
    infos = []
    for db_auth in dbs_auth:
        if username is None:
            infos.append(db_auth.command('usersInfo'))
        else:
            infos.append(db_auth.command('usersInfo', username))
    if username is None:
        users = []
        for info in infos:
            users.extend(collect_users(info, dbs))
        print("Registered users:")
    else:
        users = [username]
        print("Roles of user '{}':".format(username))
    for user in users:
        roles = []
        for info in infos:
            roles.extend(collect_roles(info, dbs, user))
        if len(users) > 1:
            print("user: ", user)
            print("roles:", roles)
        else:
            print(roles)

HELP_OPERATION = """\
    R|Administrate the project database.
    You can perform the following operations:

        add:            Add a user to this project.

        remove:         Remove a user from this project.

        grant:          Grant permissions to user.

        revoke:         Revoke permissions from user.

        show:           Show all registered users for this project.
    """

def setup_subparser(subparser):
    subparser.add_argument(
        'user',
        type = str,
        help = "A username or the path to a certificate file.")
    subparser.add_argument(
        '--ssl',
        action = 'store_true',
        help = "Use SSL certificates for authentication.")

def setup_parser(parser):
    import textwrap
    subparsers = parser.add_subparsers()

    parser_add = subparsers.add_parser('add')
    setup_subparser(parser_add)
    parser_add.add_argument(
        '-r', '--readonly',
        action = 'store_true',
        help = "Grant only read permissions to new user.")
    parser_add.set_defaults(func = add_user)

    parser_remove = subparsers.add_parser('remove')
    setup_subparser(parser_remove)
    parser_remove.set_defaults(func = remove_user)

    parser_grant = subparsers.add_parser('grant')
    setup_subparser(parser_grant)
    parser_grant.add_argument(
        'role',
        type = str,
        help = "The role to grant to this user. Choices: {read,readWrite}")
    parser_grant.set_defaults(func = grant_roles)

    parser_revoke = subparsers.add_parser('revoke')
    setup_subparser(parser_revoke)
    parser_revoke.add_argument(
        'role',
        type = str,
        help = "The role to revoke from this user. Choices: {read,readWrite}")
    parser_revoke.set_defaults(func = revoke_roles)

    parser_show = subparsers.add_parser('show')
    parser_show.set_defaults(func = show_users)
    parser_show.add_argument(
        '-u', '--user',
        type = str,
        help = "A username or a path to a certificate file.")

def main(arguments = None):
        from argparse import ArgumentParser
        from ..contrib.utility import add_verbosity_argument, set_verbosity_level
        parser = ArgumentParser(
            description = "Administrate compdb.",
            )
        setup_parser(parser)
        parser.add_argument(
            '-y', '--yes',
            action = 'store_true',
            help = "Assume yes to all questions.",)
        add_verbosity_argument(parser)
        args = parser.parse_args(arguments)
        set_verbosity_level(args.verbosity)
        try:
            if 'func' in args:
                args.func(args, get_project(args))
            else:
                parser.print_usage()
        except Exception as error:
            if args.verbosity > 0:
                raise
            else:
                print("Error: {}".format(error))
                return 1
        else:
            return 0

if __name__ == '__main__':
    import sys
    logging.basicConfig(level = logging.INFO)
    sys.exit(main())
