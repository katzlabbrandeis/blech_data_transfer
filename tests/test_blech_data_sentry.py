import pytest
import os
import pandas as pd
import tempfile
import shutil
from unittest.mock import patch, MagicMock
from glob import glob
from io import StringIO
import sys

# Update import path to use the src module
from src.blech_data_sentry import (
    parse_arguments,
    get_server_path,
    setup_server_home_dir,
    handle_blacklist,
    get_directories_to_scan,
    scan_for_info_files,
    create_dataset_frame,
    check_metadata,
    write_results,
    main
)

@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)

@pytest.fixture
def mock_server_path(temp_dir):
    """Create a mock server path structure"""
    # Create local_only_files directory with server path file
    local_only_dir = os.path.join(temp_dir, 'local_only_files')
    os.makedirs(local_only_dir, exist_ok=True)
    
    # Create server path file
    server_path = os.path.join(temp_dir, 'server')
    os.makedirs(server_path, exist_ok=True)
    
    with open(os.path.join(local_only_dir, 'blech_server_path.txt'), 'w') as f:
        f.write(server_path)
    
    return temp_dir, server_path

@pytest.fixture
def mock_server_structure(mock_server_path):
    """Create a mock server directory structure with test data"""
    _, server_path = mock_server_path
    
    # Create data_management directory
    data_mgmt_dir = os.path.join(server_path, 'data_management')
    os.makedirs(data_mgmt_dir, exist_ok=True)
    
    # Create user directories
    user1_dir = os.path.join(server_path, 'user1')
    user2_dir = os.path.join(server_path, 'user2')
    os.makedirs(user1_dir, exist_ok=True)
    os.makedirs(user2_dir, exist_ok=True)
    
    # Create subdirectories
    user1_sub1 = os.path.join(user1_dir, 'experiment1')
    user1_sub2 = os.path.join(user1_dir, 'experiment2')
    user2_sub1 = os.path.join(user2_dir, 'experiment3')
    os.makedirs(user1_sub1, exist_ok=True)
    os.makedirs(user1_sub2, exist_ok=True)
    os.makedirs(user2_sub1, exist_ok=True)
    
    # Create info.rhd files
    os.makedirs(os.path.join(user1_sub1, 'session1'), exist_ok=True)
    os.makedirs(os.path.join(user1_sub2, 'session1'), exist_ok=True)
    os.makedirs(os.path.join(user2_sub1, 'session1'), exist_ok=True)
    
    with open(os.path.join(user1_sub1, 'session1', 'info.rhd'), 'w') as f:
        f.write('test info file')
    
    with open(os.path.join(user1_sub2, 'session1', 'info.rhd'), 'w') as f:
        f.write('test info file')
    
    with open(os.path.join(user2_sub1, 'session1', 'info.rhd'), 'w') as f:
        f.write('test info file')
    
    # Create metadata file for one dataset
    with open(os.path.join(user1_sub1, 'session1', 'metadata.info'), 'w') as f:
        f.write('test metadata file')
    
    return mock_server_path, data_mgmt_dir

def test_parse_arguments():
    """Test argument parsing with default values"""
    with patch('sys.argv', ['blech_data_sentry.py']):
        args = parse_arguments()
        assert not args.ignore_blacklist
    
    """Test argument parsing with ignore_blacklist flag"""
    with patch('sys.argv', ['blech_data_sentry.py', '--ignore_blacklist']):
        args = parse_arguments()
        assert args.ignore_blacklist

def test_get_server_path(mock_server_path):
    """Test getting server path from configuration file"""
    dir_path, server_path = mock_server_path
    
    # Capture stdout to avoid printing during tests
    with patch('sys.stdout', new=StringIO()):
        result = get_server_path(dir_path)
    
    assert result == server_path
    
    # Test with non-existent server path
    with patch('os.path.exists', return_value=False), \
         patch('sys.exit') as mock_exit, \
         patch('sys.stdout', new=StringIO()):
        get_server_path(dir_path)
        mock_exit.assert_called_once()

def test_setup_server_home_dir(mock_server_path):
    """Test setting up server home directory"""
    _, server_path = mock_server_path
    
    # Remove data_management directory if it exists
    data_mgmt_dir = os.path.join(server_path, 'data_management')
    if os.path.exists(data_mgmt_dir):
        os.rmdir(data_mgmt_dir)
    
    # Test creating the directory
    with patch('sys.stdout', new=StringIO()):
        result = setup_server_home_dir(server_path)
    
    assert result == data_mgmt_dir
    assert os.path.exists(data_mgmt_dir)
    
    # Test with existing directory
    with patch('sys.stdout', new=StringIO()):
        result = setup_server_home_dir(server_path)
    
    assert result == data_mgmt_dir

def test_handle_blacklist(mock_server_structure):
    """Test handling blacklist file"""
    (dir_path, server_path), data_mgmt_dir = mock_server_structure
    
    # Test with no blacklist file
    with patch('sys.stdout', new=StringIO()):
        blacklist, blacklist_file, blacklist_str = handle_blacklist(
            data_mgmt_dir, dir_path, False)
    
    assert blacklist == []
    assert blacklist_file is None
    assert blacklist_str == 'None'
    
    # Test with blacklist file in server_home_dir
    blacklist_content = "user1\nignore_dir"
    with open(os.path.join(data_mgmt_dir, 'sentry_blacklist.txt'), 'w') as f:
        f.write(blacklist_content)
    
    with patch('sys.stdout', new=StringIO()):
        blacklist, blacklist_file, blacklist_str = handle_blacklist(
            data_mgmt_dir, dir_path, False)
    
    assert blacklist == ['user1', 'ignore_dir']
    assert blacklist_file == os.path.join(data_mgmt_dir, 'sentry_blacklist.txt')
    assert blacklist_str == blacklist_content
    
    # Test with ignore_blacklist=True
    with patch('sys.stdout', new=StringIO()):
        blacklist, blacklist_file, blacklist_str = handle_blacklist(
            data_mgmt_dir, dir_path, True)
    
    assert blacklist == []
    assert blacklist_file is None
    assert blacklist_str == 'None'

def test_get_directories_to_scan(mock_server_structure):
    """Test getting directories to scan"""
    (_, server_path), _ = mock_server_structure
    
    # Test without blacklist
    with patch('sys.stdout', new=StringIO()):
        sub_dirs, top_level_dirs_str = get_directories_to_scan(server_path, [])
    
    # Should include all directories except data_management
    assert set(sub_dirs) == {'user1/experiment1', 'user1/experiment2', 'user2/experiment3'}
    assert 'user1' in top_level_dirs_str
    assert 'user2' in top_level_dirs_str
    
    # Test with blacklist
    with patch('sys.stdout', new=StringIO()):
        sub_dirs, top_level_dirs_str = get_directories_to_scan(server_path, ['user1'])
    
    assert set(sub_dirs) == {'user2/experiment3'}
    assert 'user1' not in top_level_dirs_str
    assert 'user2' in top_level_dirs_str

def test_scan_for_info_files(mock_server_structure):
    """Test scanning for info.rhd files"""
    (_, server_path), _ = mock_server_structure
    sub_dirs = ['user1/experiment1', 'user1/experiment2', 'user2/experiment3']
    
    with patch('sys.stdout', new=StringIO()):
        info_file_paths = scan_for_info_files(server_path, sub_dirs)
    
    assert len(info_file_paths) == 3
    assert all('info.rhd' in path for path in info_file_paths)

def test_create_dataset_frame(mock_server_structure):
    """Test creating dataset frame from info file paths"""
    (_, server_path), _ = mock_server_structure
    
    # Get info file paths
    info_file_paths = [
        os.path.join(server_path, 'user1/experiment1/session1/info.rhd'),
        os.path.join(server_path, 'user1/experiment2/session1/info.rhd'),
        os.path.join(server_path, 'user2/experiment3/session1/info.rhd')
    ]
    
    with patch('sys.stdout', new=StringIO()):
        dataset_frame, data_dirs = create_dataset_frame(info_file_paths, server_path)
    
    assert len(dataset_frame) == 3
    assert 'root_dir' in dataset_frame.columns
    assert 'data_dir' in dataset_frame.columns
    assert set(dataset_frame['root_dir']) == {'user1', 'user2'}
    assert len(data_dirs) == 3

def test_check_metadata(mock_server_structure):
    """Test checking for metadata files"""
    (_, server_path), _ = mock_server_structure
    
    # Create data directories
    data_dirs = [
        os.path.join(server_path, 'user1/experiment1/session1'),
        os.path.join(server_path, 'user1/experiment2/session1'),
        os.path.join(server_path, 'user2/experiment3/session1')
    ]
    
    with patch('sys.stdout', new=StringIO()):
        metadata_files, metadata_present = check_metadata(data_dirs)
    
    assert len(metadata_files) == 3
    assert len(metadata_present) == 3
    assert metadata_present[0] is True  # First directory has metadata
    assert metadata_present[1] is False  # Second directory has no metadata
    assert metadata_present[2] is False  # Third directory has no metadata

def test_write_results(mock_server_structure):
    """Test writing results to files"""
    (_, _), data_mgmt_dir = mock_server_structure
    
    # Create test dataset frame
    dataset_frame = pd.DataFrame({
        'root_dir': ['user1', 'user2'],
        'data_dir': ['user1/experiment1', 'user2/experiment3'],
        'metadata_file': [['metadata.info'], None],
        'metadata_present': [True, False]
    })
    
    start_time = 1000  # Mock start time
    blacklist_str = "user3"
    top_level_dirs_str = "user1\nuser2"
    
    with patch('sys.stdout', new=StringIO()):
        write_results(dataset_frame, data_mgmt_dir, start_time, blacklist_str, top_level_dirs_str)
    
    # Check if files were created
    assert os.path.exists(os.path.join(data_mgmt_dir, 'dataset_frame.csv'))
    assert os.path.exists(os.path.join(data_mgmt_dir, 'last_scan.txt'))
    
    # Check content of last_scan.txt
    with open(os.path.join(data_mgmt_dir, 'last_scan.txt'), 'r') as f:
        content = f.read()
        assert 'Time taken:' in content
        assert 'Blacklist:\nuser3' in content
        assert 'Top level directories processed:\nuser1\nuser2' in content

@patch('src.blech_data_sentry.parse_arguments')
@patch('src.blech_data_sentry.get_server_path')
@patch('src.blech_data_sentry.setup_server_home_dir')
@patch('src.blech_data_sentry.handle_blacklist')
@patch('src.blech_data_sentry.get_directories_to_scan')
@patch('src.blech_data_sentry.scan_for_info_files')
@patch('src.blech_data_sentry.create_dataset_frame')
@patch('src.blech_data_sentry.check_metadata')
@patch('src.blech_data_sentry.write_results')
def test_main(mock_write, mock_check, mock_create, mock_scan, mock_get_dirs, 
              mock_handle, mock_setup, mock_get_path, mock_parse):
    """Test the main function with mocked dependencies"""
    # Setup mocks
    mock_args = MagicMock()
    mock_args.ignore_blacklist = False
    mock_parse.return_value = mock_args
    
    mock_dir_path = '/mock/dir/path'
    with patch('os.path.realpath', return_value='/mock/dir/path/blech_data_sentry.py'), \
         patch('os.path.dirname', return_value=mock_dir_path):
        
        mock_server_path = '/mock/server/path'
        mock_get_path.return_value = mock_server_path
        
        mock_server_home_dir = '/mock/server/path/data_management'
        mock_setup.return_value = mock_server_home_dir
        
        mock_blacklist = ['user3']
        mock_blacklist_file = '/mock/blacklist/file'
        mock_blacklist_str = 'user3'
        mock_handle.return_value = (mock_blacklist, mock_blacklist_file, mock_blacklist_str)
        
        mock_sub_dirs = ['user1/exp1', 'user2/exp2']
        mock_top_level_dirs_str = 'user1\nuser2'
        mock_get_dirs.return_value = (mock_sub_dirs, mock_top_level_dirs_str)
        
        mock_info_file_paths = ['/mock/server/path/user1/exp1/info.rhd']
        mock_scan.return_value = mock_info_file_paths
        
        mock_dataset_frame = pd.DataFrame({'col1': [1, 2]})
        mock_data_dirs = ['/mock/data/dir1', '/mock/data/dir2']
        mock_create.return_value = (mock_dataset_frame, mock_data_dirs)
        
        mock_metadata_files = [['file1.info'], None]
        mock_metadata_present = [True, False]
        mock_check.return_value = (mock_metadata_files, mock_metadata_present)
        
        # Call main function
        main()
        
        # Verify all functions were called with expected arguments
        mock_parse.assert_called_once()
        mock_get_path.assert_called_once_with(mock_dir_path)
        mock_setup.assert_called_once_with(mock_server_path)
        mock_handle.assert_called_once_with(mock_server_home_dir, mock_dir_path, False)
        mock_get_dirs.assert_called_once_with(mock_server_path, mock_blacklist)
        mock_scan.assert_called_once_with(mock_server_path, mock_sub_dirs)
        mock_create.assert_called_once_with(mock_info_file_paths, mock_server_path)
        mock_check.assert_called_once_with(mock_data_dirs)
        mock_write.assert_called_once()
