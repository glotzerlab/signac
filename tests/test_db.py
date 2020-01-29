# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import pytest

import signac.db


try:
    signac.db.get_database('testing', hostname='testing')
except signac.common.errors.ConfigError:
    SKIP_REASON = "No 'testing' host configured."
except ImportError:
    SKIP_REASON = "pymongo not available"
else:
    SKIP_REASON = None
    import signac.common.host


@pytest.mark.skipif(SKIP_REASON is not None, reason=SKIP_REASON)
class TestDB():

    def get_test_db(self):
        signac.db.get_database('testing', hostname='testing')

    def test_get_connector(self):
        host_config = signac.common.host.get_host_config(hostname='testing')
        signac.common.host.get_connector(host_config)

    def test_get_connector_no_client(self):
        host_config = signac.common.host.get_host_config(hostname='testing')
        c = signac.common.host.get_connector(host_config)
        with pytest.raises(RuntimeError):
            c.client

    def test_get_client(self):
        host_config = signac.common.host.get_host_config(hostname='testing')
        signac.common.host.get_client(host_config)

    def test_connector_get_host(self):
        host_config = signac.common.host.get_host_config(hostname='testing')
        c = signac.common.host.get_connector(host_config)
        assert host_config['url'] == c.host
        assert host_config == c.config

    def test_logout(self):
        host_config = signac.common.host.get_host_config(hostname='testing')
        c = signac.common.host.get_connector(host_config)
        with pytest.raises(RuntimeError):
            c.client
        c.connect()
        c.client
        c.authenticate()
        c.logout()
