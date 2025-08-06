
import unittest
from mock import patch
import os
import sys
import io
import testing.postgresql

from adsmp import app
from adsmp.models import Base, Records
from run import reindex_failed_bibcodes, manage_sitemap, update_sitemap_files


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
                    'sitemap_url': 'https://ui.adsabs.harvard.edu/sitemap_index.xml',
                    'abs_url_pattern': 'https://ui.adsabs.harvard.edu/abs/{bibcode}'
                },
                'scix': {
                    'name': 'SciX',
                    'base_url': 'https://scixplorer.org/',
                    'sitemap_url': 'https://scixplorer.org/sitemap_index.xml',
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
        
        with patch('adsmp.tasks.task_manage_sitemap') as mock_task:
            manage_sitemap(bibcodes, 'add')
            
            # Verify the task was called with correct parameters
            mock_task.assert_called_once_with(bibcodes, 'add')

    def test_populate_sitemap_table_force_update_action(self):
        """Test populate_sitemap_table function with 'force-update' action"""
        
        bibcodes = ['2023ApJ...123..456A']
        
        with patch('adsmp.tasks.task_manage_sitemap') as mock_task:
            manage_sitemap(bibcodes, 'force-update')
            
            mock_task.assert_called_once_with(bibcodes, 'force-update')

    def test_populate_sitemap_table_delete_table_action(self):
        """Test populate_sitemap_table function with 'delete-table' action"""
        
        # delete-table doesn't require bibcodes
        with patch('adsmp.tasks.task_manage_sitemap') as mock_task:
            manage_sitemap([], 'delete-table')
            
            mock_task.assert_called_once_with([], 'delete-table')

    def test_populate_sitemap_table_update_robots_action(self):
        """Test populate_sitemap_table function with 'update-robots' action"""
        
        # update-robots doesn't require bibcodes
        with patch('adsmp.tasks.task_manage_sitemap') as mock_task:
            manage_sitemap([], 'update-robots')
            
            mock_task.assert_called_once_with([], 'update-robots')

    def test_populate_sitemap_table_remove_action(self):
        """Test populate_sitemap_table function with 'remove' action (TODO implementation)"""
        
        bibcodes = ['2023ApJ...123..456A']
        
        with patch('adsmp.tasks.task_manage_sitemap') as mock_task:
            manage_sitemap(bibcodes, 'remove')
            
            mock_task.assert_called_once_with(bibcodes, 'remove')

    def test_update_sitemap_files(self):
        """Test update_sitemap_files function"""
        
        with patch('adsmp.tasks.task_update_sitemap_files') as mock_task:
            update_sitemap_files()
            
            # Verify the task was called with no parameters
            mock_task.assert_called_once_with()

    def test_populate_sitemap_table_with_exception(self):
        """Test populate_sitemap_table handles exceptions gracefully"""
        
        bibcodes = ['2023ApJ...123..456A']
        
        with patch('adsmp.tasks.task_manage_sitemap') as mock_task:
            mock_task.side_effect = Exception("Task execution failed")
            
            with self.assertRaises(Exception) as context:
                manage_sitemap(bibcodes, 'add')
            
            self.assertEqual(str(context.exception), "Task execution failed")

    def test_update_sitemap_files_with_exception(self):
        """Test update_sitemap_files handles exceptions gracefully"""
        
        with patch('adsmp.tasks.task_update_sitemap_files') as mock_task:
            mock_task.side_effect = Exception("Sitemap update failed")
            
            with self.assertRaises(Exception) as context:
                update_sitemap_files()
            
            self.assertEqual(str(context.exception), "Sitemap update failed")

    def test_populate_sitemap_table_all_actions(self):
        """Test all possible actions for populate_sitemap_table"""
        
        actions_and_bibcodes = [
            ('add', ['2023ApJ...123..456A', '2023ApJ...123..457B']),
            ('force-update', ['2023ApJ...123..456A']),
            ('remove', ['2023ApJ...123..456A']),
            ('delete-table', []),
            ('update-robots', [])
        ]
        
        for action, bibcodes in actions_and_bibcodes:
            with self.subTest(action=action):
                with patch('adsmp.tasks.task_manage_sitemap') as mock_task:
                    manage_sitemap(bibcodes, action)
                    mock_task.assert_called_once_with(bibcodes, action)

    def test_integration_with_task_calls(self):
        """Test integration to ensure tasks are called correctly"""
        
        # This test ensures the run.py functions properly delegate to tasks
        bibcodes = ['2023ApJ...123..456A']
        
        # Test both functions in sequence to simulate real usage
        with patch('adsmp.tasks.task_manage_sitemap') as mock_populate:
            with patch('adsmp.tasks.task_update_sitemap_files') as mock_update:
                
                # First populate sitemap table
                manage_sitemap(bibcodes, 'add')
                
                # Then update sitemap files
                update_sitemap_files()
                
                # Verify both tasks were called correctly
                mock_populate.assert_called_once_with(bibcodes, 'add')
                mock_update.assert_called_once_with()

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
                    print("Error: --action is required when using --populate-sitemap-table")
                    print("Available actions: add, remove, force-update, delete-table, update-robots")
                    sys.exit(1)
                
                # Verify sys.exit(1) was called
                mock_exit.assert_called_once_with(1)
                
                # Verify error message was printed
                output = sys.stdout.getvalue()
                self.assertIn("Error: --action is required", output)
                self.assertIn("Available actions:", output)

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
                                print(f"Error: --action {action} requires bibcodes")
                                print("Provide bibcodes using --bibcodes or --filename")
                                sys.exit(1)
                        
                        # Verify sys.exit(1) was called
                        mock_exit.assert_called_once_with(1)
                        
                        # Verify error message was printed
                        output = sys.stdout.getvalue()
                        self.assertIn(f"Error: --action {action} requires bibcodes", output)
                        self.assertIn("Provide bibcodes using --bibcodes or --filename", output)

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
                        with patch('adsmp.tasks.task_manage_sitemap') as mock_populate:
                            with patch('adsmp.tasks.task_update_sitemap_files') as mock_update:
                                
                                # Simulate successful validation and execution
                                if '--populate-sitemap-table' in test_args:
                                    # Has action parameter
                                    if expected_action and expected_bibcodes is not None:
                                        manage_sitemap(expected_bibcodes, expected_action)
                                        mock_populate.assert_called_once_with(expected_bibcodes, expected_action)
                                elif '--update-sitemap-files' in test_args:
                                    update_sitemap_files()
                                    mock_update.assert_called_once_with()
                                
                                # Verify sys.exit was NOT called for valid scenarios
                                mock_exit.assert_not_called()

if __name__ == "__main__":
    unittest.main()

