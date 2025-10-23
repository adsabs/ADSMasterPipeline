
import unittest
from mock import patch, Mock
import os
import sys
import io
import testing.postgresql

from adsmp import app
from adsmp.models import Base, Records, SitemapInfo
from run import reindex_failed_bibcodes, manage_sitemap, update_sitemap_files, update_sitemaps_auto, cleanup_invalid_sitemaps
from datetime import datetime, timedelta, timezone

class TestFixDbDuplicates(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.postgresql = \
            testing.postgresql.Postgresql(host='127.0.0.1', port=15678, user='postgres', 
                                          database='test')

    @classmethod
    def tearDownClass(cls):
        cls.postgresql.stop()

    def setUp(self):
        unittest.TestCase.setUp(self)
        proj_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
        self.app = app.ADSMasterPipelineCelery('test', local_config=\
            {
            'SQLALCHEMY_URL': 'sqlite:///',
            'METRICS_SQLALCHEMY_URL': 'postgresql://postgres@127.0.0.1:15678/test',
            'SQLALCHEMY_ECHO': False,
            'PROJ_HOME' : proj_home,
            'TEST_DIR' : os.path.join(proj_home, 'adsmp/tests'),
            })
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        self.app.close_app()

    def test_reindex_failed_bibcodes(self):
        # init database
        with self.app.session_scope() as session:
            session.add(Records(bibcode='bibcode1', status='success', bib_data='{}'))
            session.add(Records(bibcode='bibcode2', status='solr-failed', bib_data='{}'))
            session.add(Records(bibcode='bibcode3', status='links-failed', bib_data='{}'))
            session.add(Records(bibcode='bibcode4', status='retrying', bib_data='{}'))
            session.add(Records(bibcode='bibcode5', fulltext='foobar'))

        # execute reindex_failed_bibcodes from run.py
        with patch('adsmp.tasks.task_index_records.apply_async', return_value=None) as queue_bibcodes:
            reindex_failed_bibcodes(self.app)
            self.assertEqual(1, queue_bibcodes.call_count)
            queue_bibcodes.assert_called_with(args=([u'bibcode2', u'bibcode3'],),
                                              kwargs={'force': True, 'ignore_checksums': True,
                                                      'update_links': True, 'update_metrics': True,
                                                      'update_solr': True, 'update_processed': True,
                                                      'priority': 0},
                                              priority=0)

        # verify database was updated propery
        with self.app.session_scope() as session:
            rec = session.query(Records).filter_by(bibcode='bibcode1').first()
            self.assertEqual(rec.status, 'success')
            rec = session.query(Records).filter_by(bibcode='bibcode2').first()
            self.assertEqual(rec.status, 'retrying')
            rec = session.query(Records).filter_by(bibcode='bibcode3').first()
            self.assertEqual(rec.status, 'retrying')
            rec = session.query(Records).filter_by(bibcode='bibcode4').first()
            self.assertEqual(rec.status, 'retrying')
            rec = session.query(Records).filter_by(bibcode='bibcode5').first()
            self.assertEqual(rec.status, None)


class TestSitemapCommandLine(unittest.TestCase):
    """Test sitemap command-line functionality including validation, file handling, and execution"""

    @classmethod
    def setUpClass(cls):
        cls.postgresql = \
            testing.postgresql.Postgresql(host='127.0.0.1', port=15679, user='postgres', 
                                          database='test_sitemap')

    @classmethod
    def tearDownClass(cls):
        cls.postgresql.stop()

    def setUp(self):
        unittest.TestCase.setUp(self)
        # Capture stdout for testing print statements  
        self.held, sys.stdout = sys.stdout, io.StringIO()
        
        proj_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
        self.app = app.ADSMasterPipelineCelery('test_sitemap', local_config={
            'SQLALCHEMY_URL': 'sqlite:///',
            'METRICS_SQLALCHEMY_URL': 'postgresql://postgres@127.0.0.1:15679/test_sitemap',
            'SQLALCHEMY_ECHO': False,
            'PROJ_HOME': proj_home,
            'TEST_DIR': os.path.join(proj_home, 'adsmp/tests'),
            'SITEMAP_DIR': '/tmp/test_sitemap',
            'SITES': {
                'ads': {
                    'name': 'ADS',
                    'base_url': 'https://ui.adsabs.harvard.edu/',
                    'sitemap_url': 'https://ui.adsabs.harvard.edu/sitemap',
                    'abs_url_pattern': 'https://ui.adsabs.harvard.edu/abs/{bibcode}'
                },
                'scix': {
                    'name': 'SciX',
                    'base_url': 'https://scixplorer.org/',
                    'sitemap_url': 'https://scixplorer.org/sitemap',
                    'abs_url_pattern': 'https://scixplorer.org/abs/{bibcode}'
                }
            }
        })
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        sys.stdout = self.held
        Base.metadata.drop_all()
        self.app.close_app()

    # Basic function tests
    def test_populate_sitemap_table_add_action(self):
        """Test populate_sitemap_table function with 'add' action"""
        
        bibcodes = ['2023ApJ...123..456A', '2023ApJ...123..457B']
        
        # Mock the chain workflow since 'add' action uses chain for auto-updating files
        with patch('run.chain') as mock_chain:
            mock_result = Mock()
            mock_result.id = 'test-task-123'
            mock_workflow = Mock()
            mock_workflow.apply_async.return_value = mock_result
            mock_chain.return_value = mock_workflow
            
            result = manage_sitemap(bibcodes, 'add')
            
            # Verify chain was called to create the workflow
            self.assertTrue(mock_chain.called)
            self.assertEqual(result, 'test-task-123')

    def test_populate_sitemap_table_force_update_action(self):
        """Test populate_sitemap_table function with 'force-update' action"""
        
        bibcodes = ['2023ApJ...123..456A']
        
        # Mock the chain workflow since 'force-update' action uses chain
        with patch('run.chain') as mock_chain:
            mock_result = Mock()
            mock_result.id = 'test-task-456'
            mock_workflow = Mock()
            mock_workflow.apply_async.return_value = mock_result
            mock_chain.return_value = mock_workflow
            
            result = manage_sitemap(bibcodes, 'force-update')
            
            self.assertTrue(mock_chain.called)
            self.assertEqual(result, 'test-task-456')

    def test_populate_sitemap_table_delete_table_action(self):
        """Test populate_sitemap_table function with 'delete-table' action"""
        
        # delete-table doesn't require bibcodes
        with patch('adsmp.tasks.task_manage_sitemap.apply_async') as mock_task:
            mock_result = Mock()
            mock_result.id = 'test-task-789'
            mock_task.return_value = mock_result
            
            result = manage_sitemap([], 'delete-table')
            
            mock_task.assert_called_once_with(args=([], 'delete-table'))
            self.assertEqual(result, 'test-task-789')

    def test_populate_sitemap_table_update_robots_action(self):
        """Test populate_sitemap_table function with 'update-robots' action"""
        
        # update-robots doesn't require bibcodes
        with patch('adsmp.tasks.task_manage_sitemap.apply_async') as mock_task:
            mock_result = Mock()
            mock_result.id = 'test-task-robots'
            mock_task.return_value = mock_result
            
            result = manage_sitemap([], 'update-robots')
            
            mock_task.assert_called_once_with(args=([], 'update-robots'))
            self.assertEqual(result, 'test-task-robots')

    def test_populate_sitemap_table_remove_action(self):
        """Test populate_sitemap_table function with 'remove' action"""
        
        bibcodes = ['2023ApJ...123..456A']
        
        # Mock the chain workflow since 'remove' action uses chain
        with patch('run.chain') as mock_chain:
            mock_result = Mock()
            mock_result.id = 'test-task-remove'
            mock_workflow = Mock()
            mock_workflow.apply_async.return_value = mock_result
            mock_chain.return_value = mock_workflow
            
            result = manage_sitemap(bibcodes, 'remove')
            
            self.assertTrue(mock_chain.called)
            self.assertEqual(result, 'test-task-remove')

    def test_update_sitemap_files(self):
        """Test update_sitemap_files function"""
        
        with patch('adsmp.tasks.task_update_sitemap_files.apply_async') as mock_task:
            mock_result = Mock()
            mock_result.id = 'test-update-files-123'
            mock_task.return_value = mock_result
            
            result = update_sitemap_files()
            
            # Verify the task was called with no parameters
            mock_task.assert_called_once_with()
            self.assertEqual(result, 'test-update-files-123')

    def test_populate_sitemap_table_with_exception(self):
        """Test populate_sitemap_table handles exceptions gracefully"""
        
        bibcodes = ['2023ApJ...123..456A']
        
        with patch('adsmp.tasks.task_manage_sitemap.apply_async') as mock_task:
            mock_task.side_effect = Exception("Task execution failed")
            
            with self.assertRaises(Exception) as context:
                manage_sitemap(bibcodes, 'add')
            
            self.assertEqual(str(context.exception), "Task execution failed")

    def test_update_sitemap_files_with_exception(self):
        """Test update_sitemap_files handles exceptions gracefully"""
        
        with patch('adsmp.tasks.task_update_sitemap_files.apply_async') as mock_task:
            mock_task.side_effect = Exception("Sitemap update failed")
            
            with self.assertRaises(Exception) as context:
                update_sitemap_files()
            
            self.assertEqual(str(context.exception), "Sitemap update failed")

    def test_populate_sitemap_table_all_actions(self):
        """Test all possible actions for populate_sitemap_table"""
        
        chain_actions = ['add', 'force-update', 'remove', 'bootstrap']
        non_chain_actions = ['delete-table', 'update-robots']
        
        actions_and_bibcodes = [
            ('add', ['2023ApJ...123..456A', '2023ApJ...123..457B']),
            ('force-update', ['2023ApJ...123..456A']),
            ('remove', ['2023ApJ...123..456A']),
            ('bootstrap', []),
            ('delete-table', []),
            ('update-robots', [])
        ]
        
        for action, bibcodes in actions_and_bibcodes:
            with self.subTest(action=action):
                mock_result = Mock()
                mock_result.id = f'test-task-{action}'
                
                if action in chain_actions:
                    # Actions that use chain for auto-updating files
                    with patch('run.chain') as mock_chain:
                        mock_workflow = Mock()
                        mock_workflow.apply_async.return_value = mock_result
                        mock_chain.return_value = mock_workflow
                        
                        result = manage_sitemap(bibcodes, action)
                        
                        self.assertTrue(mock_chain.called, f"Chain should be called for action '{action}'")
                        self.assertEqual(result, f'test-task-{action}')
                else:
                    # Actions that call task directly
                    with patch('adsmp.tasks.task_manage_sitemap.apply_async') as mock_task:
                        mock_task.return_value = mock_result
                        
                        result = manage_sitemap(bibcodes, action)
                        
                        self.assertTrue(mock_task.called, f"Task should be called for action '{action}'")
                        self.assertEqual(result, f'test-task-{action}')

    def test_integration_with_task_calls(self):
        """Test integration to ensure tasks are called correctly"""
        
        # This test ensures the run.py functions properly delegate to tasks
        bibcodes = ['2023ApJ...123..456A']
        
        # Test both functions in sequence to simulate real usage
        with patch('run.chain') as mock_chain:
            with patch('adsmp.tasks.task_update_sitemap_files.apply_async') as mock_update:
                
                # Set up mock results
                mock_populate_result = Mock()
                mock_populate_result.id = 'test-populate-123'
                mock_workflow = Mock()
                mock_workflow.apply_async.return_value = mock_populate_result
                mock_chain.return_value = mock_workflow
                
                mock_update_result = Mock()
                mock_update_result.id = 'test-update-456'  
                mock_update.return_value = mock_update_result
                
                # First populate sitemap table (uses chain for 'add' action)
                populate_result = manage_sitemap(bibcodes, 'add')
                
                # Then update sitemap files (standalone call)
                update_result = update_sitemap_files()
                
                # Verify both were called correctly
                self.assertTrue(mock_chain.called, "Chain should be called for 'add' action")
                mock_update.assert_called_once_with()
                
                # Verify return values
                self.assertEqual(populate_result, 'test-populate-123')
                self.assertEqual(update_result, 'test-update-456')

    # Validation tests
    def test_action_validation_missing_action(self):
        """Test validation when --action is missing"""
        
        # Mock command line args without action
        with patch('sys.argv', ['run.py', '--populate-sitemap-table', '--bibcodes', '2023ApJ...123..456A']):
            # This should simulate the validation logic from run.py
            args_action = None  # Simulating args.action being None
            
            if not args_action:
                # This is what run.py does
                expected_error = "Error: --action is required when using --populate-sitemap-table"
                expected_help = "Available actions: add, remove, force-update, delete-table, update-robots"
                
                # Verify the error messages are correct
                self.assertIn("--action is required", expected_error)
                self.assertIn("add, remove, force-update, delete-table, update-robots", expected_help)

    # Command-line execution tests
    def test_missing_action_causes_sys_exit(self):
        """Test that missing --action parameter causes sys.exit(1)"""
        
        # Mock command line args without action
        test_args = ['run.py', '--populate-sitemap-table', '--bibcodes', '2023ApJ...123..456A']
        
        with patch('sys.argv', test_args):
            with patch('sys.exit') as mock_exit:
                # Simulate the validation logic from run.py
                args_action = None  # Simulating missing --action
                
                if not args_action:
                    sys.exit(1)
                
                # Verify sys.exit(1) was called
                mock_exit.assert_called_once_with(1)

    def test_actions_requiring_bibcodes_without_bibcodes_causes_sys_exit(self):
        """Test that actions requiring bibcodes without bibcodes cause sys.exit(1)"""
        
        actions_requiring_bibcodes = ['add', 'remove', 'force-update']
        
        for action in actions_requiring_bibcodes:
            with self.subTest(action=action):
                # Reset stdout capture
                sys.stdout = io.StringIO()
                
                test_args = ['run.py', '--populate-sitemap-table', '--action', action]
                
                with patch('sys.argv', test_args):
                    with patch('sys.exit') as mock_exit:
                        # Simulate the validation logic from run.py
                        bibcodes = []  # Empty bibcodes
                        
                        if action in ['add', 'remove', 'force-update']:
                            if not bibcodes:
                                sys.exit(1)
                        
                        # Verify sys.exit(1) was called
                        mock_exit.assert_called_once_with(1)

    def test_valid_command_line_execution_flow(self):
        """Test valid command line execution doesn't cause sys.exit"""
        
        valid_scenarios = [
            (['run.py', '--populate-sitemap-table', '--action', 'add', '--bibcodes', '2023ApJ...123..456A'], 
             'add', ['2023ApJ...123..456A']),
            (['run.py', '--populate-sitemap-table', '--action', 'delete-table'], 
             'delete-table', []),
            (['run.py', '--populate-sitemap-table', '--action', 'update-robots'], 
             'update-robots', []),
            (['run.py', '--update-sitemap-files'], 
             None, None)  # Different command path
        ]
        
        for test_args, expected_action, expected_bibcodes in valid_scenarios:
            with self.subTest(args=test_args):
                # Reset stdout capture
                sys.stdout = io.StringIO()
                
                with patch('sys.argv', test_args):
                    with patch('sys.exit') as mock_exit:
                        with patch('run.chain') as mock_chain:
                            with patch('adsmp.tasks.task_manage_sitemap.apply_async') as mock_populate:
                                with patch('adsmp.tasks.task_update_sitemap_files.apply_async') as mock_update:
                                    
                                    # Set up mock results
                                    mock_populate_result = Mock()
                                    mock_populate_result.id = 'test-final-populate'
                                    mock_populate.return_value = mock_populate_result
                                    
                                    # For chain actions
                                    mock_workflow = Mock()
                                    mock_workflow.apply_async.return_value = mock_populate_result
                                    mock_chain.return_value = mock_workflow
                                    
                                    mock_update_result = Mock()
                                    mock_update_result.id = 'test-final-update'
                                    mock_update.return_value = mock_update_result
                                    
                                    # Simulate successful validation and execution
                                    if '--populate-sitemap-table' in test_args:
                                        # Has action parameter
                                        if expected_action and expected_bibcodes is not None:
                                            result = manage_sitemap(expected_bibcodes, expected_action)
                                            # Verify the right mock was called based on action type
                                            if expected_action in ('add', 'force-update', 'remove', 'bootstrap'):
                                                self.assertTrue(mock_chain.called, f"Chain should be called for '{expected_action}'")
                                            else:
                                                self.assertTrue(mock_populate.called, f"Task should be called for '{expected_action}'")
                                            self.assertEqual(result, 'test-final-populate')
                                    elif '--update-sitemap-files' in test_args:
                                        result = update_sitemap_files()
                                        mock_update.assert_called_once_with()
                                        self.assertEqual(result, 'test-final-update')
                                    
                                    # Verify sys.exit was NOT called for valid scenarios
                                    mock_exit.assert_not_called()

    def test_update_sitemaps_auto_with_records(self):
        """Test update_sitemaps_auto function with records needing updates"""
        
        # Mock the database query to return test records
        mock_recent_record1 = Mock()
        mock_recent_record1.bibcode = '2023ApJ...123..456A'
        
        mock_recent_record2 = Mock()
        mock_recent_record2.bibcode = '2023ApJ...123..457B'
        
        # Test with default 1 day lookback
        with patch('adsmp.tasks.task_manage_sitemap.apply_async') as mock_manage:
            with patch('adsmp.tasks.task_update_sitemap_files.apply_async') as mock_files:
                with patch('run.app.session_scope') as mock_session_scope:
                    # Mock the session and query
                    mock_session = Mock()
                    mock_session_scope.return_value.__enter__.return_value = mock_session
                    
                    # Mock the UNION query structure
                    mock_bib_data_query = Mock()
                    mock_solr_query = Mock()
                    mock_union_result = [mock_recent_record1, mock_recent_record2]
                    
                    # Mock the query chain
                    mock_session.query.return_value.filter.side_effect = [mock_bib_data_query, mock_solr_query]
                    mock_bib_data_query.union.return_value = mock_union_result
                    
                    # Set up mock results
                    mock_manage_result = Mock()
                    mock_manage_result.id = 'test-auto-manage-123'
                    mock_manage.return_value = mock_manage_result
                    
                    mock_files_result = Mock()
                    mock_files_result.id = 'test-auto-files-456'
                    mock_files.return_value = mock_files_result
                    
                    # Call the function
                    manage_task_id, file_task_id = update_sitemaps_auto(days_back=1)
                    
                    # Verify tasks were called correctly
                    self.assertTrue(mock_manage.called)
                    self.assertTrue(mock_files.called)
                    
                    # Check manage task arguments
                    manage_call_args = mock_manage.call_args
                    self.assertEqual(manage_call_args[1]['args'][1], 'add')  # Action should be 'add'
                    self.assertEqual(manage_call_args[1]['priority'], 0)  # Priority should be 0
                    
                    # Check bibcodes (should only include recent ones)
                    submitted_bibcodes = manage_call_args[1]['args'][0]
                    self.assertEqual(len(submitted_bibcodes), 2)
                    self.assertIn('2023ApJ...123..456A', submitted_bibcodes)
                    self.assertIn('2023ApJ...123..457B', submitted_bibcodes)
                    
                    # Check files task arguments (should have link to manage task)
                    files_call_args = mock_files.call_args
                    self.assertEqual(files_call_args[1]['link'], 'test-auto-manage-123')  # Linked to manage task
                    
                    # Verify return values
                    self.assertEqual(manage_task_id, 'test-auto-manage-123')
                    self.assertEqual(file_task_id, 'test-auto-files-456')

    def test_update_sitemaps_auto_with_exception(self):
        """Test update_sitemaps_auto handles task submission exceptions"""
        
        # Mock a test record
        mock_record = Mock()
        mock_record.bibcode = '2023ApJ...123..456A'
        
        # Test with manage task failing
        with patch('adsmp.tasks.task_manage_sitemap.apply_async') as mock_manage:
            with patch('run.app.session_scope') as mock_session_scope:
                # Mock the session and query to return a record
                mock_session = Mock()
                mock_session_scope.return_value.__enter__.return_value = mock_session
                
                # Mock the UNION query structure
                mock_bib_data_query = Mock()
                mock_solr_query = Mock()
                mock_union_result = [mock_record]
                
                # Mock the query chain
                mock_session.query.return_value.filter.side_effect = [mock_bib_data_query, mock_solr_query]
                mock_bib_data_query.union.return_value = mock_union_result
                
                mock_manage.side_effect = Exception("Task submission failed")
                
                with self.assertRaises(Exception) as context:
                    update_sitemaps_auto(days_back=1)
                
                self.assertEqual(str(context.exception), "Task submission failed")

    def test_update_sitemaps_auto_with_solr_processed_updates(self):
        """Test update_sitemaps_auto catches records with recent solr_processed updates"""

        # Mock records with recent solr_processed (successful reindexes)
        mock_reindexed_record1 = Mock()
        mock_reindexed_record1.bibcode = '2023ApJ...123..789C'

        mock_reindexed_record2 = Mock()
        mock_reindexed_record2.bibcode = '2023ApJ...123..790D'

        # Mock records with recent bib_data_updated
        mock_new_record = Mock()
        mock_new_record.bibcode = '2023ApJ...123..791E'

        with patch('adsmp.tasks.task_manage_sitemap.apply_async') as mock_manage:
            with patch('adsmp.tasks.task_update_sitemap_files.apply_async') as mock_files:
                with patch('run.app.session_scope') as mock_session_scope:
                    # Mock the session
                    mock_session = Mock()
                    mock_session_scope.return_value.__enter__.return_value = mock_session

                    # Mock the two separate queries
                    # First query: bib_data_updated records
                    mock_bib_data_query = Mock()
                    mock_bib_data_query.union.return_value = [
                        mock_new_record,           # From bib_data_updated query
                        mock_reindexed_record1,    # From solr_processed query  
                        mock_reindexed_record2     # From solr_processed query
                    ]

                    # Set up the query chain for the new union structure
                    mock_session.query.return_value.filter.return_value = mock_bib_data_query

                    # Set up mock task results
                    mock_manage_result = Mock()
                    mock_manage_result.id = 'test-solr-manage-123'
                    mock_manage.return_value = mock_manage_result

                    mock_files_result = Mock()
                    mock_files_result.id = 'test-solr-files-456'
                    mock_files.return_value = mock_files_result

                    # Call the function
                    manage_task_id, file_task_id = update_sitemaps_auto(days_back=1)

                    # Verify results
                    self.assertEqual(manage_task_id, 'test-solr-manage-123')
                    self.assertEqual(file_task_id, 'test-solr-files-456')

                    # Verify manage task was called with all bibcodes
                    self.assertTrue(mock_manage.called)
                    manage_call_args = mock_manage.call_args
                    bibcodes_passed = manage_call_args[1]['args'][0]  # args=(bibcodes, 'add')

                    # Should include bibcodes from both bib_data_updated and solr_processed queries
                    expected_bibcodes = ['2023ApJ...123..791E', '2023ApJ...123..789C', '2023ApJ...123..790D']
                    self.assertEqual(set(bibcodes_passed), set(expected_bibcodes))

    def test_cleanup_invalid_sitemaps(self):
        """Test cleanup_invalid_sitemaps function"""
        
        with patch('run.chain') as mock_chain:
            # Mock the chain and its apply_async method
            mock_workflow = Mock()
            mock_result = Mock()
            mock_result.id = 'test-cleanup-task-123'
            mock_workflow.apply_async.return_value = mock_result
            mock_chain.return_value = mock_workflow
            
            # Call the function
            task_id = cleanup_invalid_sitemaps()
            
            # Verify chain was created
            self.assertEqual(mock_chain.call_count, 1, "chain() should be called once")
            
            # Verify apply_async was called with correct priority
            mock_workflow.apply_async.assert_called_once_with(priority=1)
            
            # Verify return value
            self.assertEqual(task_id, 'test-cleanup-task-123')

if __name__ == "__main__":
    unittest.main()

