class Parser(object):
    
    def parse(data):
        raise NotImplementedError()
    
    def parse_file(filename):
        with open(filename, 'rb') as file:
            parse(file.read())
