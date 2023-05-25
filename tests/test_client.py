import collections
from unittest import TestCase
from unittest.mock import call, patch

from pymemcache.exceptions import MemcacheUnknownCommandError, MemcacheUnknownError

from django_elastipymemcache.client import ConfigurationEndpointClient

EXAMPLE_RESPONSE = [
    b"CONFIG cluster 0 147\r\n",
    b"12\n"
    b"myCluster.pc4ldq.0001.use1.cache.amazonaws.com|10.82.235.120|11211 "
    b"myCluster.pc4ldq.0002.use1.cache.amazonaws.com|10.80.249.27|11211\n\r\n",
    b"END\r\n",
]


class DjangoElastiPymemcacheClientTestCases(TestCase):
    @patch("socket.getaddrinfo", return_value=[range(5)])
    @patch("socket.socket")
    def test_get_cluster_info(self, socket, _):
        recv_bufs = collections.deque(
            [
                b"VERSION 1.4.14\r\n",
            ]
            + EXAMPLE_RESPONSE
        )

        client = socket.return_value
        client.recv.side_effect = lambda *args, **kwargs: recv_bufs.popleft()
        cluster_info = ConfigurationEndpointClient(("h", 0)).get_cluster_info()
        self.assertEqual(
            cluster_info["nodes"],
            [
                ("10.82.235.120", 11211),
                ("10.80.249.27", 11211),
            ],
        )
        client.sendall.assert_has_calls(
            [
                call(b"version\r\n"),
                call(b"config get cluster\r\n"),
            ]
        )

    @patch("socket.getaddrinfo", return_value=[range(5)])
    @patch("socket.socket")
    def test_get_cluster_info_before_1_4_13(self, socket, _):
        recv_bufs = collections.deque(
            [
                b"VERSION 1.4.13\r\n",
            ]
            + EXAMPLE_RESPONSE
        )

        client = socket.return_value
        client.recv.side_effect = lambda *args, **kwargs: recv_bufs.popleft()
        cluster_info = ConfigurationEndpointClient(("h", 0)).get_cluster_info()
        self.assertEqual(
            cluster_info["nodes"],
            [
                ("10.82.235.120", 11211),
                ("10.80.249.27", 11211),
            ],
        )
        client.sendall.assert_has_calls(
            [
                call(b"version\r\n"),
                call(b"get AmazonElastiCache:cluster\r\n"),
            ]
        )

    @patch("socket.getaddrinfo", return_value=[range(5)])
    @patch("socket.socket")
    def test_no_configuration_protocol_support_with_errors(self, socket, _):
        with self.assertRaises(MemcacheUnknownCommandError):
            recv_bufs = collections.deque(
                [
                    b"VERSION 1.4.13\r\n",
                    b"ERROR\r\n",
                ]
            )
            client = socket.return_value
            client.recv.side_effect = lambda *args, **kwargs: recv_bufs.popleft()
            ConfigurationEndpointClient(("h", 0)).get_cluster_info()

    @patch("socket.getaddrinfo", return_value=[range(5)])
    @patch("socket.socket")
    def test_cannot_parse_version(self, socket, _):
        with self.assertRaises(MemcacheUnknownError):
            recv_bufs = collections.deque(
                [
                    b"VERSION 1.4.34\r\n",
                    b"CONFIG cluster 0 147\r\n",
                    b"fail\nhost|ip|11211 host|ip|11211\n\r\n",
                    b"END\r\n",
                ]
            )

            client = socket.return_value
            client.recv.side_effect = lambda *args, **kwargs: recv_bufs.popleft()
            ConfigurationEndpointClient(("h", 0)).get_cluster_info()

    @patch("socket.getaddrinfo", return_value=[range(5)])
    @patch("socket.socket")
    def test_cannot_parse_nodes(self, socket, _):
        with self.assertRaises(MemcacheUnknownError):
            recv_bufs = collections.deque(
                [
                    b"VERSION 1.4.34\r\n",
                    b"CONFIG cluster 0 147\r\n",
                    b"1\nfail\n\r\n",
                    b"END\r\n",
                ]
            )

            client = socket.return_value
            client.recv.side_effect = lambda *args, **kwargs: recv_bufs.popleft()
            ConfigurationEndpointClient(("h", 0)).get_cluster_info()

    @patch("socket.getaddrinfo", return_value=[range(5)])
    @patch("socket.socket")
    def test_ignore_erros(self, socket, _):
        recv_bufs = collections.deque(
            [
                b"VERSION 1.4.34\r\n",
                b"fail\nfail\n\r\n",
                b"END\r\n",
            ]
        )

        client = socket.return_value
        client.recv.side_effect = lambda *args, **kwargs: recv_bufs.popleft()
        cluster_info = ConfigurationEndpointClient(
            ("h", 0),
            ignore_cluster_errors=True,
        ).get_cluster_info()
        self.assertEqual(cluster_info["nodes"], [("h", 0)])
