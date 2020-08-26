# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.


class TestBufferedJSONDict(TestJSONDict):

    def test_buffered_read_write(self, synced_dict, testdata):
        key = 'buffered_read_write'
        assert len(synced_dict) == 0
        with synced_dict.buffered() as b:
            b[key] = testdata
            assert b[key] == testdata
            assert len(b) == 1
            assert len(synced_dict) == 0
        assert len(synced_dict) == 1
        assert synced_dict[key] == testdata
        with synced_dict.buffered() as b:
            del b[key]
            assert key not in b
            assert len(synced_dict) == 1
        assert key not in synced_dict


class TestBufferedJSONList(TestJSONList):

    def test_buffered_read_write(self, synced_list, testdata):
        assert len(synced_list) == 0
        with synced_list.buffered() as b:
            b.append(testdata)
            assert b[0] == testdata
            assert len(b) == 1
            assert len(synced_list) == 0
        assert len(synced_list) == 1
        with synced_list.buffered() as b:
            del b[0]
            assert len(b) == 0
            assert len(synced_list) == 1
        assert len(synced_list) == 0
