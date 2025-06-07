# To be completed - deprioritised due to GCP updates 2025
from unittest.mock import patch
import unittest
from src.data_cleaning.data_cleaning import standardize_name, header_standardization, deduplicate, clean_and_impute_data, clean_strings


def test_standardize_name():
    banned_name_test = '_TABLE_55helloworld'
    valid_char_test = ',,, 93helloworld'
    max_len_test = 'helloworld' * 301
    empty_test = ''

    assert standardize_name(banned_name_test) == 'default55helloworld'
    assert standardize_name(valid_char_test) == '_93helloworld'
    assert len(standardize_name(max_len_test)) == 300
    assert standardize_name(empty_test) == 'column_'


@patch("src.data_cleaning.data_cleaning.setup_cloud_logger")
def test_header_standardization(mock_logger):
    import pandas as pd 
    cols = ["_TABLE_55helloworld", ",,, 93helloworld", "helloworld", "helloworld", ""]
    df = pd.DataFrame(columns=cols)
 
    # Call the function. mock_logger is automatically passed here as input_logger.
    new_df = header_standardization(mock_logger, df, "test_df")
    # Expected columns after standardization
    expected_cols = ["default55helloworld", "_93helloworld", "helloworld", "helloworld_1","column_"]
    case = unittest.TestCase()
    case.assertCountEqual(new_df.columns, expected_cols)
    
    # Assertions to check if correct logger messages were called
    mock_logger.info.assert_any_call("----------- Standardizing test_df headers ---------")
    mock_logger.info.assert_any_call("Successfully standardized test_df headers")

