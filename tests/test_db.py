import unittest

import signac.db

try:
    signac.db.get_database('testing', hostname='testing')
except AttributeError:
    DB_AVAILABLE=False
else:
    DB_AVAILABLE=True
    import signac.common.host

@unittest.skipIf(not DB_AVAILABLE, "pymongo not available")
class DBTest(unittest.TestCase):

    def get_test_db(self):
        signac.db.get_database('testing', hostname='testing')

    def test_get_connector(self):
        signac.common.host.get_connector(hostname='testing')

    def test_get_connector_no_client(self):
        c = signac.common.host.get_connector(hostname='testing')
        with self.assertRaises(RuntimeError):
            c.client

    def test_get_client(self):
        signac.common.host.get_client(hostname='testing')

    def test_connector_get_host(self):
        host_config = signac.common.host.get_host_config(hostname='testing')
        c = signac.common.host.get_connector(hostname='testing')
        self.assertEqual(host_config['url'], c.host)
        self.assertEqual(host_config, c.config)

    def test_logout(self):
        c = signac.common.host.get_connector(hostname='testing')
        with self.assertRaises(RuntimeError):
            c.client
        c.connect()
        c.client
        c.authenticate()
        c.logout()

if __name__ == '__main__':
    unittest.main()
