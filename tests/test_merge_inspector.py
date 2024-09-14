# test_defensive_merge.py

import pandas as pd
import pytest
from defensive_pandas_merge.merge_inspector import MergeInspector, MergeInspectorException

def test_defensive_merge_error_handling():
    """
    Test the DefensiveMerge class to ensure it raises an exception when specified errors occur.
    Specifically, we test for 'rows_duplicated_error' and 'matched_keys_error'.
    """
    # Create sample DataFrames that will cause the specified errors
    df_left = pd.DataFrame({
        'customer_id': ['C001', 'C002', 'C003', 'C004', 'C002'],
        'loyalty_member': ['L001', 'L002', 'L003', 'L004', 'L002']  # Duplicated 'L002'
    })

    df_right = pd.DataFrame({
        'order_id': [101, 102, 103, 104],
        'loyalty_member': ['L001', 'L005', 'L006', 'L007']  # 'L005', 'L006', 'L007' do not match
    })

    # Specify error flags that should raise an exception
    raise_on_errors = ['rows_duplicated_error', 'matched_keys_error']

    # Initialize the DefensiveMerge with error flags
    inspector = DefensiveMerge(
        df_left=df_left,
        df_right=df_right,
        raise_on_errors=raise_on_errors,
        how='inner',
        on='loyalty_member'
    )

    # Perform the merge and expect an exception
    with pytest.raises(DefensiveMergeException) as exc_info:
        df_merged = inspector.perform_merge()

    # Access the exception and the report
    exception = exc_info.value
    report = exception.report

    # Assertions to verify that the correct errors are reported
    assert 'matched_keys_error is True' in str(exception)
    assert report['matched_keys_error'] is True
    assert report['rows_duplicated_error'] is False  # No duplicated rows in this case

    # Additional assertions to check the report content
    assert report['left_duplicated_keys_in_keys']['number'] == 1  # 'L002' is duplicated in left
    assert report['right_duplicated_keys_in_keys']['number'] == 0  # No duplicates in right
    assert report['number_of_matched_keys'] == 1  # Only 'L001' matches
    assert report['percentage_of_matched_keys'] < 100.0  # Less than 100% keys matched

    print("Test passed: DefensiveMergeException was raised with the expected errors.")

