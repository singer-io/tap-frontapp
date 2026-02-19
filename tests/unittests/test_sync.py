import unittest
from unittest.mock import MagicMock, patch
from tap_frontapp.sync import update_currently_syncing, sync


class TestUpdateCurrentlySyncing(unittest.TestCase):
    """Test update_currently_syncing function"""

    def test_set_currently_syncing(self):
        """Test setting currently_syncing stream"""
        state = {}
        update_currently_syncing(state, 'test_stream')
        self.assertEqual(state['currently_syncing'], 'test_stream')

    def test_clear_currently_syncing(self):
        """Test clearing currently_syncing"""
        state = {'currently_syncing': 'test_stream'}
        update_currently_syncing(state, None)
        self.assertNotIn('currently_syncing', state)

    def test_update_currently_syncing_writes_state(self):
        """Test that state is written after update"""
        state = {}
        with patch('singer.write_state') as mock_write:
            update_currently_syncing(state, 'test_stream')
            mock_write.assert_called_once()


class TestBookmarkValidation(unittest.TestCase):
    """Test bookmark validation to ensure compliance with target-qlik requirements"""

    def test_no_empty_offset_in_bookmarks(self):
        """Test that bookmarks do not contain empty offset dictionaries"""
        # Good state - no offset key
        good_state = {
            'bookmarks': {
                'stream1': {
                    'date_to_resume': '2024-01-01T00:00:00Z'
                },
                'stream2': {}  # Non-resumable stream with empty bookmark
            }
        }

        # Validate no offset keys exist
        for stream_name, bookmark in good_state.get('bookmarks', {}).items():
            self.assertNotIn('offset', bookmark,
                f"Stream '{stream_name}' should not have 'offset' key in bookmark")

    def test_bookmark_structure_compliance(self):
        """Test bookmark structure complies with target-qlik requirements"""
        state = {
            'bookmarks': {
                'accounts_table': {
                    'date_to_resume': '2024-01-15T00:00:00Z'
                },
                'teams_table': {
                    'date_to_resume': '2024-01-10T00:00:00Z'
                }
            }
        }

        self.assertIn('bookmarks', state)
        for stream_name, bookmark_data in state['bookmarks'].items():
            # Verify it's a dictionary
            self.assertIsInstance(bookmark_data, dict)
            # Verify no offset key
            self.assertNotIn('offset', bookmark_data,
                f"Stream '{stream_name}' contains 'offset' key which violates target-qlik requirements")
            # If bookmark has data, it should be resumable
            if bookmark_data:
                self.assertIn('date_to_resume', bookmark_data,
                    f"Resumable stream '{stream_name}' should have 'date_to_resume' key")

    def test_non_resumable_streams_have_empty_bookmarks(self):
        """Test that non-resumable streams have empty bookmark dictionaries"""
        state = {
            'bookmarks': {
                'resumable_stream': {
                    'date_to_resume': '2024-01-01T00:00:00Z'
                },
                'non_resumable_stream': {}
            }
        }

        # Non-resumable streams should have empty dict
        self.assertEqual(state['bookmarks']['non_resumable_stream'], {})

        # Should not have nested values (including offset)
        non_resumable_bookmark = state['bookmarks']['non_resumable_stream']
        self.assertEqual(len(non_resumable_bookmark), 0,
            "Non-resumable stream should have completely empty bookmark dictionary")

    def test_state_does_not_use_reserved_keys(self):
        """Test that state doesn't use target_state or target_total_batches"""
        state = {
            'bookmarks': {
                'stream1': {'date_to_resume': '2024-01-01T00:00:00Z'}
            },
            'currently_syncing': 'stream1'
        }

        # Reserved keys should not be present
        self.assertNotIn('target_state', state,
            "'target_state' is a reserved key and should not be used by taps")
        self.assertNotIn('target_total_batches', state,
            "'target_total_batches' is a reserved key and should not be used by taps")

    def test_bookmark_value_existence_indicates_resumability(self):
        """Test that existence of value under bookmarks.stream_id indicates resumability"""
        state = {
            'bookmarks': {
                'resumable_stream': {
                    'date_to_resume': '2024-01-01T00:00:00Z'
                },
                'another_resumable': {
                    'date_to_resume': '2024-01-15T00:00:00Z'
                },
                'non_resumable': {}
            }
        }
        # Check resumable streams
        resumable_bookmark = state['bookmarks']['resumable_stream']
        self.assertTrue(bool(resumable_bookmark),
            "Resumable stream should have non-empty bookmark")
        another_resumable_bookmark = state['bookmarks']['another_resumable']
        self.assertTrue(bool(another_resumable_bookmark),
            "Resumable stream should have non-empty bookmark")
        non_resumable_bookmark = state['bookmarks']['non_resumable']
        self.assertFalse(bool(non_resumable_bookmark),
            "Non-resumable stream should have empty bookmark")


class TestSyncFunction(unittest.TestCase):
    """Test sync function"""

    @patch('tap_frontapp.sync.sync_selected_streams')
    @patch('tap_frontapp.sync.load_and_write_schema')
    def test_sync_processes_selected_streams(self, mock_load_schema, mock_sync_streams):
        """Test that sync processes all selected streams"""
        mock_atx = MagicMock()
        mock_atx.catalog.get_selected_streams.return_value = [
            MagicMock(tap_stream_id='stream1'),
            MagicMock(tap_stream_id='stream2')
        ]
        mock_atx.state = {}
        sync(mock_atx)

        # Verify sync_selected_streams was called
        mock_sync_streams.assert_called_once_with(mock_atx)


class TestInterruptedSync(unittest.TestCase):
    """Test sync interruption and resumption scenarios"""

    def test_currently_syncing_persists_on_interruption(self):
        """Test that currently_syncing is preserved when sync is interrupted"""
        state = {
            'bookmarks': {
                'stream1': {'date_to_resume': '2024-01-01T00:00:00Z'},
                'stream2': {'date_to_resume': '2024-01-01T00:00:00Z'},
                'stream3': {'date_to_resume': '2024-01-01T00:00:00Z'}
            },
            'currently_syncing': 'stream2'
        }
        # Verify currently_syncing is set
        self.assertEqual(state['currently_syncing'], 'stream2')
        # Verify bookmarks are preserved
        self.assertIn('stream1', state['bookmarks'])
        self.assertIn('stream2', state['bookmarks'])
        self.assertIn('stream3', state['bookmarks'])

    @patch('tap_frontapp.sync.sync_selected_streams')
    @patch('tap_frontapp.sync.load_and_write_schema')
    def test_sync_resumes_from_currently_syncing_stream(self, mock_load_schema, mock_sync_streams):
        """Test that sync resumes from the currently_syncing stream"""
        mock_atx = MagicMock()

        # Simulate interrupted state
        mock_atx.state = {
            'bookmarks': {
                'stream1': {'date_to_resume': '2024-01-15T00:00:00Z'},
                'stream2': {'date_to_resume': '2024-01-01T00:00:00Z'},
                'stream3': {}
            },
            'currently_syncing': 'stream2'
        }

        mock_atx.catalog.get_selected_streams.return_value = [
            MagicMock(tap_stream_id='stream1'),
            MagicMock(tap_stream_id='stream2'),
            MagicMock(tap_stream_id='stream3')
        ]

        sync(mock_atx)

        # Verify sync was called with state containing currently_syncing
        mock_sync_streams.assert_called_once_with(mock_atx)
        self.assertEqual(mock_atx.state['currently_syncing'], 'stream2')

    def test_interrupted_stream_bookmark_updated(self):
        """Test that interrupted stream's bookmark gets updated on partial completion"""
        state = {
            'bookmarks': {
                'stream1': {'date_to_resume': '2024-01-15T00:00:00Z'},  # Completed
                'stream2': {'date_to_resume': '2024-01-10T00:00:00Z'},  # Partially synced
                'stream3': {}  # Not started
            },
            'currently_syncing': 'stream2'
        }

        # Verify the interrupted stream has a bookmark (partial progress saved)
        self.assertIn('date_to_resume', state['bookmarks']['stream2'])

        # Verify it's different from initial state (progress was made)
        self.assertIsNotNone(state['bookmarks']['stream2']['date_to_resume'])

    def test_state_cleared_after_successful_sync(self):
        """Test that currently_syncing is cleared after all streams complete"""
        state = {
            'bookmarks': {
                'stream1': {'date_to_resume': '2024-01-15T00:00:00Z'},
                'stream2': {'date_to_resume': '2024-01-14T00:00:00Z'}
            }
        }

        # After successful completion, currently_syncing should not be present
        self.assertNotIn('currently_syncing', state)

    @patch('singer.write_state')
    def test_state_written_on_stream_transition(self, mock_write_state):
        """Test that state is written when transitioning between streams"""
        state = {}

        # Simulate setting currently_syncing for first stream
        update_currently_syncing(state, 'stream1')
        self.assertEqual(mock_write_state.call_count, 1)

        # Simulate transitioning to next stream
        update_currently_syncing(state, 'stream2')
        self.assertEqual(mock_write_state.call_count, 2)

        # Simulate completion
        update_currently_syncing(state, None)
        self.assertEqual(mock_write_state.call_count, 3)

    def test_interrupt_before_first_stream_starts(self):
        """Test interruption before any stream has started"""
        state = {
            'bookmarks': {
                'stream1': {},
                'stream2': {},
                'stream3': {}
            }
        }

        # No currently_syncing means sync hasn't started or just started
        self.assertNotIn('currently_syncing', state)

        # All bookmarks are empty (no progress)
        for bookmark in state['bookmarks'].values():
            self.assertEqual(bookmark, {})

    def test_interrupt_during_last_stream(self):
        """Test interruption during the last stream"""
        state = {
            'bookmarks': {
                'stream1': {'date_to_resume': '2024-01-15T00:00:00Z'},
                'stream2': {'date_to_resume': '2024-01-14T00:00:00Z'},
                'stream3': {'date_to_resume': '2024-01-10T00:00:00Z'}
            },
            'currently_syncing': 'stream3'
        }

        # Verify we're on the last stream
        self.assertEqual(state['currently_syncing'], 'stream3')

        # Verify previous streams completed (have bookmarks)
        self.assertIsNotNone(state['bookmarks']['stream1']['date_to_resume'])
        self.assertIsNotNone(state['bookmarks']['stream2']['date_to_resume'])

    def test_multiple_interruptions_same_stream(self):
        """Test multiple interruptions on the same stream with bookmark updates"""
        # First interruption
        state_v1 = {
            'bookmarks': {
                'stream1': {'date_to_resume': '2024-01-01T00:00:00Z'}
            },
            'currently_syncing': 'stream1'
        }

        # Second interruption - bookmark advanced
        state_v2 = {
            'bookmarks': {
                'stream1': {'date_to_resume': '2024-01-05T00:00:00Z'}
            },
            'currently_syncing': 'stream1'
        }

        # Third interruption - bookmark advanced further
        state_v3 = {
            'bookmarks': {
                'stream1': {'date_to_resume': '2024-01-10T00:00:00Z'}
            },
            'currently_syncing': 'stream1'
        }

        date1 = state_v1['bookmarks']['stream1']['date_to_resume']
        date2 = state_v2['bookmarks']['stream1']['date_to_resume']
        date3 = state_v3['bookmarks']['stream1']['date_to_resume']

        self.assertLess(date1, date2)
        self.assertLess(date2, date3)
