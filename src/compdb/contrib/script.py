LEGAL_COMMANDS = ['init', 'config']

def main():
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument(
        'command',
        choices = LEGAL_COMMANDS,
        )

    args, more = parser.parse_known_args()

    if args.command == 'init':
        from compdb.contrib.init_project import main
        return main(more)
    elif args.command == 'config':
        from compdb.contrib.configure import main
        return main(more)
    else:
        print("Unknown command '{}'.".format(args.command))
