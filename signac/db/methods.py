from . import DBMethod


def make_converter(expected_format):
    class Converter(DBMethod):
        expects = expected_format

        def apply(self, arg):
            return arg
    return Converter


def converter(expected_format):
    return make_converter(expected_format)()


def make_is_convertable(expected_format):
    class IsConvertable(DBMethod):
        expects = expected_format

        def apply(self, arg):
            return True
    return IsConvertable


def is_convertable(expected_format):
    return make_is_convertable(expected_format)()
