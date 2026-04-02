import unittest
from unittest.mock import MagicMock, patch, call
import requests
import pendulum
from tap_frontapp.streams import (
    write_metrics_state,
    sync_metrics,
    sync_selected_streams,
    create_report,
    get_report_metrics
)


class TestWriteMetricsState(unittest.TestCase):
    """Test write_metrics_state function"""

    def test_write_metrics_state_success(self):
        """Test that write_metrics_state correctly writes bookmark"""
        mock_atx = MagicMock()
        mock_atx.state = {}

        date_to_resume = pendulum.parse('2024-01-15T00:00:00Z')
        write_metrics_state(mock_atx, 'test_metric', date_to_resume)

        # Verify bookmark was written
        self.assertIn('bookmarks', mock_atx.state)
        self.assertIn('test_metric', mock_atx.state['bookmarks'])
        self.assertEqual(
            mock_atx.state['bookmarks']['test_metric']['date_to_resume'],
            '2024-01-15 00:00:00'
        )

        # Verify write_state was called
        mock_atx.write_state.assert_called_once()


class TestSyncMetrics(unittest.TestCase):
    """Test sync_metrics function"""

    @patch('tap_frontapp.streams.sync_metric')
    @patch('tap_frontapp.streams.write_metrics_state')
    def test_sync_metrics_with_bookmark(self, mock_write_state, mock_sync_metric):
        """Test sync_metrics when bookmark exists"""
        mock_atx = MagicMock()
        mock_atx.config = {
            'start_date': '2024-01-01T00:00:00Z',
            'end_date': '2024-01-03T00:00:00Z'
        }
        mock_atx.state = {
            'bookmarks': {
                'test_metric': {
                    'date_to_resume': '2024-01-02 00:00:00'
                }
            }
        }

        sync_metrics(mock_atx, 'test_metric')
        # Should sync from last bookmark (2024-01-02) to end date (2024-01-03)
        self.assertEqual(mock_sync_metric.call_count, 2)

    @patch('tap_frontapp.streams.sync_metric')
    @patch('tap_frontapp.streams.write_metrics_state')
    def test_sync_metrics_without_bookmark(self, mock_write_state, mock_sync_metric):
        """Test sync_metrics when no bookmark exists"""
        mock_atx = MagicMock()
        mock_atx.config = {
            'start_date': '2024-01-01T00:00:00Z',
            'end_date': '2024-01-02T00:00:00Z'
        }
        mock_atx.state = {'bookmarks': {}}
        sync_metrics(mock_atx, 'test_metric')

        # Should sync from start_date
        self.assertGreater(mock_sync_metric.call_count, 0)

    @patch('tap_frontapp.streams.sync_metric')
    @patch('tap_frontapp.streams.write_metrics_state')
    def test_sync_metrics_writes_bookmark_after_each_day(self, mock_write_state, mock_sync_metric):
        """Test that bookmark is written after each day sync"""
        mock_atx = MagicMock()
        mock_atx.config = {
            'start_date': '2024-01-01T00:00:00Z',
            'end_date': '2024-01-03T00:00:00Z'
        }
        mock_atx.state = {'bookmarks': {}}

        sync_metrics(mock_atx, 'test_metric')
        self.assertEqual(mock_write_state.call_count, mock_sync_metric.call_count)

    @patch('tap_frontapp.streams.singer.metadata.to_map')
    @patch('tap_frontapp.streams.sync_metric')
    @patch('tap_frontapp.streams.write_metrics_state')
    def test_sync_metrics_derives_metadata_from_catalog(self, mock_write_state, mock_sync_metric, mock_to_map):
        """sync_metrics should derive mdata from catalog metadata when not provided."""
        from tap_frontapp.streams import sync_metrics

        mock_atx = MagicMock()
        mock_atx.config = {
            'start_date': '2024-01-01T00:00:00Z',
            'end_date': '2024-01-01T00:00:00Z',
        }
        mock_atx.state = {'bookmarks': {}}

        # Minimal catalog/stream entry with metadata for the metric
        stream = MagicMock()
        stream.tap_stream_id = 'test_metric'
        stream.metadata = 'raw_metadata'
        mock_catalog = MagicMock()
        mock_catalog.streams = [stream]
        mock_atx.catalog = mock_catalog

        derived_mdata = {'from': 'catalog'}
        mock_to_map.return_value = derived_mdata

        sync_metrics(mock_atx, 'test_metric')

        # Only a single day should be processed for this range
        mock_sync_metric.assert_called_once()
        args, _ = mock_sync_metric.call_args
        # Last positional arg is mdata
        self.assertIs(args[-1], derived_mdata)
        mock_to_map.assert_called_once_with(stream.metadata)


class TestSyncSelectedStreams(unittest.TestCase):
    """Test sync_selected_streams function"""

    @patch('tap_frontapp.sync.update_currently_syncing')
    @patch('tap_frontapp.streams.sync_metrics')
    def test_sync_selected_streams_updates_currently_syncing(self, mock_sync_metrics, mock_update):
        """Test that currently_syncing is updated for each stream"""
        mock_atx = MagicMock()
        mock_atx.selected_stream_ids = ['stream1', 'stream2', 'stream3']
        mock_atx.state = {}

        sync_selected_streams(mock_atx)
        calls = [
            call(mock_atx.state, 'stream1'),
            call(mock_atx.state, 'stream2'),
            call(mock_atx.state, 'stream3'),
            call(mock_atx.state, None)
        ]
        mock_update.assert_has_calls(calls)

    @patch('tap_frontapp.sync.update_currently_syncing')
    @patch('tap_frontapp.streams.sync_metrics')
    def test_sync_selected_streams_syncs_all_streams(self, mock_sync_metrics, mock_update):
        """Test that all selected streams are synced"""
        mock_atx = MagicMock()
        mock_atx.selected_stream_ids = ['accounts_table', 'teams_table']
        mock_atx.state = {}
        sync_selected_streams(mock_atx)

        calls = [
            call(mock_atx, 'accounts_table'),
            call(mock_atx, 'teams_table')
        ]
        mock_sync_metrics.assert_has_calls(calls)
        self.assertEqual(mock_sync_metrics.call_count, 2)


class TestCreateReport(unittest.TestCase):
    """Test create_report function"""

    def test_create_report_success(self):
        """Test successful report creation"""
        mock_atx = MagicMock()
        mock_atx.client.create_report.return_value = 'https://api2.frontapp.com/analytics/reports/xyz'
        result = create_report(mock_atx, 1704067200, 1704153600, {'team_ids': ['team1']})
        self.assertEqual(result, 'https://api2.frontapp.com/analytics/reports/xyz')
        mock_atx.client.create_report.assert_called_once()

    def test_create_report_bad_request_returns_none(self):
        """Test that bad request returns None"""
        mock_atx = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_error = requests.exceptions.HTTPError()
        mock_error.response = mock_response
        mock_atx.client.create_report.side_effect = mock_error

        result = create_report(mock_atx, 1704067200, 1704153600, {'team_ids': ['team1']})
        self.assertIsNone(result)


class TestHelperFunctions(unittest.TestCase):
    """Test helper functions"""

    @patch('singer.write_records')
    @patch('singer.metrics.record_counter')
    def test_write_records(self, mock_counter, mock_singer_write):
        """Test write_records function"""
        from tap_frontapp.streams import write_records
        mock_counter_instance = MagicMock()
        mock_counter.return_value.__enter__.return_value = mock_counter_instance
        records = [{'id': 1}, {'id': 2}, {'id': 3}]
        write_records('test_stream', records)
        mock_singer_write.assert_called_once_with('test_stream', records)
        mock_counter_instance.increment.assert_called_once_with(3)

    def test_get_date_and_integer_fields(self):
        """Test get_date_and_integer_fields function"""
        from tap_frontapp.streams import get_date_and_integer_fields
        mock_stream = MagicMock()
        mock_stream.schema.properties = {
            'id': MagicMock(type='integer', format=None),
            'count': MagicMock(type=['null', 'integer'], format=None),
            'created_at': MagicMock(type='string', format='date-time'),
            'name': MagicMock(type='string', format=None),
            'updated_at': MagicMock(type='string', format='date-time')
        }
        date_fields, integer_fields = get_date_and_integer_fields(mock_stream)
        self.assertIn('created_at', date_fields)
        self.assertIn('updated_at', date_fields)
        self.assertIn('id', integer_fields)
        self.assertIn('count', integer_fields)
        self.assertNotIn('name', date_fields)
        self.assertNotIn('name', integer_fields)

    def test_base_transform_empty_string_to_none(self):
        """Test base_transform converts empty strings to None"""
        from tap_frontapp.streams import base_transform
        obj = {
            'id': '123',
            'name': '',
            'description': 'test'
        }
        result = base_transform([], [], obj)
        self.assertIsNone(result['name'])
        self.assertEqual(result['description'], 'test')

    def test_base_transform_integer_conversion(self):
        """Test base_transform converts integer fields"""
        from tap_frontapp.streams import base_transform
        obj = {
            'id': '123',
            'count': '456',
            'total': '0'
        }

        integer_fields = ['id', 'count', 'total']
        result = base_transform([], integer_fields, obj)
        self.assertEqual(result['id'], 123)
        self.assertEqual(result['count'], 456)
        self.assertEqual(result['total'], 0)
        self.assertIsInstance(result['id'], int)

    @patch('pendulum.parse')
    def test_base_transform_date_conversion(self, mock_parse):
        """Test base_transform converts date fields"""
        from tap_frontapp.streams import base_transform
        mock_date = MagicMock()
        mock_date.isoformat.return_value = '2024-01-01T00:00:00Z'
        mock_parse.return_value = mock_date
        obj = {
            'created_at': '2024-01-01 00:00:00',
            'name': 'test'
        }
        date_fields = ['created_at']
        result = base_transform(date_fields, [], obj)
        self.assertEqual(result['created_at'], '2024-01-01T00:00:00Z')
        mock_parse.assert_called_once_with('2024-01-01 00:00:00')

    def test_base_transform_handles_none_values(self):
        """Test base_transform preserves None values"""
        from tap_frontapp.streams import base_transform

        obj = {
            'id': None,
            'count': None,
            'created_at': None
        }

        result = base_transform(['created_at'], ['id', 'count'], obj)
        self.assertIsNone(result['id'])
        self.assertIsNone(result['count'])
        self.assertIsNone(result['created_at'])


class TestGetReportMetrics(unittest.TestCase):
    """Test get_report_metrics function"""

    def test_get_report_metrics_success(self):
        """Test successful retrieval of report metrics"""
        mock_atx = MagicMock()
        mock_atx.client.get_report_metrics.return_value = [
            {'id': 'metric1', 'value': 100},
            {'id': 'metric2', 'value': 200}
        ]
        result = get_report_metrics(mock_atx, 'https://api.frontapp.com/reports/123')
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['id'], 'metric1')
        self.assertEqual(result[0]['value'], 100)


class TestSyncMetric(unittest.TestCase):
    """Test sync_metric function"""

    @patch('tap_frontapp.streams.write_records')
    @patch('tap_frontapp.streams.get_report_metrics')
    @patch('tap_frontapp.streams.create_report')
    def test_sync_metric_writes_records(self, mock_create_report,
                                       mock_get_metrics, mock_write_records):
        """Test sync_metric writes records"""
        from tap_frontapp.streams import sync_metric
        mock_atx = MagicMock()
        mock_atx.client.list_metrics.return_value = [
            {'id': 'team_123', 'name': 'Team A'}
        ]
        mock_create_report.return_value = 'https://api.frontapp.com/reports/123'
        mock_get_metrics.return_value = [
            {'id': 'avg_first_response_time', 'value': 3600},
            {'id': 'avg_response_time', 'value': 7200}
        ]
        sync_metric(mock_atx, 'teams_table', 1609459200, 1609545600)
        # Verify write_records was called
        mock_write_records.assert_called_once()
        # Verify the record structure
        call_args = mock_write_records.call_args
        stream_name = call_args[0][0]
        records = call_args[0][1]
        self.assertEqual(stream_name, 'teams_table')
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['metric_id'], 'team_123')
        self.assertEqual(records[0]['metric_description'], 'Team A')
        self.assertEqual(records[0]['avg_first_response_time'], 3600)

    @patch('tap_frontapp.streams.write_records')
    @patch('tap_frontapp.streams.get_report_metrics')
    @patch('tap_frontapp.streams.create_report')
    def test_sync_metric_applies_metadata_filtering(self, mock_create_report,
                                                   mock_get_metrics, mock_write_records):
        """sync_metric should filter record fields based on Singer metadata map."""
        from tap_frontapp.streams import sync_metric

        mock_atx = MagicMock()
        mock_atx.client.list_metrics.return_value = [
            {'id': 'team_123', 'name': 'Team A'}
        ]
        mock_create_report.return_value = 'https://api.frontapp.com/reports/123'
        mock_get_metrics.return_value = [
            {'id': 'avg_first_response_time', 'value': 3600},
            {'id': 'avg_response_time', 'value': 7200},
        ]

        # Minimal metadata map: keep key properties and one metric field,
        # drop others (e.g., metric_description and avg_response_time).
        mdata = {
            ('properties', 'report_id'): {'inclusion': 'automatic'},
            ('properties', 'analytics_date'): {'inclusion': 'automatic'},
            ('properties', 'analytics_range'): {'inclusion': 'automatic'},
            ('properties', 'metric_id'): {'inclusion': 'automatic'},
            ('properties', 'avg_first_response_time'): {'selected': True},
        }

        sync_metric(mock_atx, 'teams_table', 1609459200, 1609545600, mdata)

        mock_write_records.assert_called_once()
        _, kwargs = mock_write_records.call_args
        stream_name, records = mock_write_records.call_args[0]
        self.assertEqual(stream_name, 'teams_table')
        self.assertEqual(len(records), 1)

        record = records[0]
        # Key properties / automatic fields are preserved
        self.assertIn('report_id', record)
        self.assertIn('analytics_date', record)
        self.assertIn('analytics_range', record)
        self.assertIn('metric_id', record)
        # Selected metric field is preserved
        self.assertIn('avg_first_response_time', record)
        # Fields without metadata should be filtered out
        self.assertNotIn('metric_description', record)
        self.assertNotIn('avg_response_time', record)

    @patch('tap_frontapp.streams.write_records')
    @patch('tap_frontapp.streams.create_report')
    def test_sync_metric_skips_when_no_report_url(self, mock_create_report,
                                                   mock_write_records):
        """Test sync_metric skips when create_report returns None"""
        from tap_frontapp.streams import sync_metric
        mock_atx = MagicMock()
        mock_atx.client.list_metrics.return_value = [
            {'id': 'team_123', 'name': 'Team A'}
        ]
        mock_create_report.return_value = None
        sync_metric(mock_atx, 'teams_table', 1609459200, 1609545600)
        mock_write_records.assert_not_called()
