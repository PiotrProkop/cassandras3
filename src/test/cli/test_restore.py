from mock import MagicMock, patch
import unittest

from cassandras3.cli.restore import do_restore
from cassandras3.util.nodetool import NodeTool


class TestRestoreClient(unittest.TestCase):
    @patch('cassandras3.cli.restore.ClientCache')
    @patch('cassandras3.cli.restore.NodeTool')
    def test_restore(self, nodetool_constructor, _):
        self._setup_mocks(nodetool_constructor)

        do_restore('us-east-1', 'localhost', 7199, 'backup-id', 'system',
                   'some-host', 'test', '/var/lib/cassandra/data')

        self.mock_nodetool.restore.assert_called_with('system', 'test',
                                                      'backup-id')

    @patch('cassandras3.cli.restore.ClientCache')
    @patch('cassandras3.cli.restore.NodeTool')
    def test_restore_no_hostname(self, nodetool_constructor, _):
        self._setup_mocks(nodetool_constructor)

        do_restore('us-east-1', 'localhost', 7199, 'backup-id', 'system', '',
                   'test', '/var/lib/cassandra/data')

        self.mock_nodetool.restore.assert_called_with('system', 'test',
                                                      'backup-id')

    def _setup_mocks(self, nodetool_constructor):
        self.mock_nodetool = MagicMock(spec=NodeTool)
        nodetool_constructor.return_value = self.mock_nodetool

    @patch('cassandras3.util.nodetool.sh')
    @patch.object(NodeTool, '_refresh')
    @patch.object(NodeTool, '_download_file')
    @patch.object(NodeTool, '_folders')
    @patch('cassandras3.cli.backup.ClientCache.s3')
    def test_restore_different_dir_name(self, s3, folders, download, refresh,
                                        sh):
        folders.return_value = ['dir/filename']

        do_restore('us-east-1', 'localhost', 7199, 'backup-id', 'system',
                   'ScyllaNode', 'test', '/var/lib/scylla/data')

        sh.mkdir.assert_called_once_with('-p',
                                         '/var/lib/scylla/data/system/dir')
