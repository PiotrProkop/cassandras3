from mock import MagicMock, patch, ANY
import unittest

from cassandras3.cli.backup import do_backup
from cassandras3.util.nodetool import NodeTool


class TestBackupClient(unittest.TestCase):
    @patch('cassandras3.cli.backup.ClientCache')
    @patch('cassandras3.cli.backup.NodeTool')
    def test_backup(self, nodetool_constructor, _):
        self._setup_mocks(nodetool_constructor)

        do_backup('us-east-1', 'localhost', 7119, 'system', 'test',
                  '/var/lib/cassandra/data')

        self.mock_nodetool.backup.assert_called_with('system', 'test', ANY)

    def _setup_mocks(self, nodetool_constructor):
        self.mock_nodetool = MagicMock(spec=NodeTool)
        nodetool_constructor.return_value = self.mock_nodetool

    @patch('socket.gethostname')
    @patch('time.time')
    @patch('cassandras3.util.nodetool.sh')
    @patch('cassandras3.util.nodetool.os.walk')
    @patch.object(NodeTool, '_clearsnapshot')
    @patch.object(NodeTool, '_snapshot')
    @patch('cassandras3.cli.backup.ClientCache.s3')
    def test_backup_different_dir_name(self, s3, snapshot, clear, os_walk, sh,
                                       time, hostname):
        os_walk.return_value = []
        time.return_value = '100'
        hostname.return_value = 'ScyllaNode'

        do_backup('us-east-1', 'localhost', 7119, 'system', 'test',
                  '/var/lib/scylla/data')
        tag = '%s-%s-%s' % ('ScyllaNode', 'system', 100)
        sh.find.assert_called_once_with('/var/lib/scylla/data', '-name', tag)
