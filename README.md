# defensive-pandas-merge

[![PyPI version](https://badge.fury.io/py/defensive-pandas-merge.svg)](https://badge.fury.io/py/defensive-pandas-merge)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Python library that enhances pandas DataFrame merge operations by providing detailed diagnostics, error reporting, and customizable error handling.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
  - [Basic Usage](#basic-usage)
  - [Example Output](#example-output)
  - [Custom Thresholds](#custom-thresholds)
  - [Error Handling](#error-handling)
- [API Reference](#api-reference)
- [Contributing](#contributing)
- [License](#license)

## Features

- **Detailed Merge Diagnostics**: Provides statistics before and after merging DataFrames.
- **Error Reporting**: Identifies duplicates, null values, unmatched keys, and row duplications.
- **Customizable Thresholds**: Set thresholds to control when issues are flagged as errors.
- **Actionable Insights**: Offers sample cases for duplicates and null keys to aid debugging.
- **Custom Error Handling**: Specify which errors should raise exceptions during the merge.
- **Seamless Integration**: Designed to work with pandas DataFrames and merge operations.

## Installation

You can install `defensive-pandas-merge` via pip:

```bash
pip install defensive-pandas-merge
```

## Usage

```python
import pandas as pd
from defensive_pandas_merge.merge_inspector import MergeInspector

# Sample DataFrames
df_customers = pd.DataFrame({
    'customer_id': ['C001', 'C002', 'C003', 'C004'],
    'loyalty_member': ['L001', 'L002', 'L003', 'L004']
})

df_orders = pd.DataFrame({
    'order_id': [101, 102, 103],
    'loyalty_member': ['L001', 'L002', 'L005']
})

# Initialize the MergeInspector
inspector = MergeInspector(
    df_left=df_customers,
    df_right=df_orders,
    how='inner',
    on='loyalty_member'
)

# Perform the merge
df_merged = inspector.perform_merge()

# Get the report
report = inspector.get_report()

# Display the report
print("Merge Report:")
for key, value in report.items():
    print(f"{key}: {value}")
```

## Example Output

Here is an example of the merge report when running the MergeInspector on a large dataset containing hundreds of thousands of rows:

```bash
left_number_of_rows_before_merge: 300000
right_number_of_rows_before_merge: 200000
left_duplicated_keys_in_keys: {'number': 50000, 'percentage': 16.67, 'cases': [{'id': 12345}, {'id': 67890}, {'id': 54321}]}
right_duplicated_keys_in_keys: {'number': 10000, 'percentage': 5.0, 'cases': [{'id': 22222}, {'id': 33333}, {'id': 44444}]}
duplicated_keys_error: True
left_null_keys_in_keys: {'number': 3000, 'percentage': 1.0, 'cases': [{'id': nan}, {'id': nan}, {'id': nan}]}
right_null_keys_in_keys: {'number': 1000, 'percentage': 0.5, 'cases': [{'id': nan}, {'id': nan}, {'id': nan}]}
null_keys_error: True
number_of_rows_after_merge: 150000
number_of_matched_keys: 140000
percentage_of_matched_keys: 70.0
number_of_left_keys_without_match: 160000
number_of_right_keys_without_match: 60000
percentage_of_matched_keys_error: True
matched_keys_error: True
number_of_rows_duplicated: 5000
rows_duplicated_error: True
```

Merge Report Keys

Below is a description of each key in the merge report:

- left_number_of_rows_before_merge: The number of rows in the left DataFrame before the merge operation.

- right_number_of_rows_before_merge: The number of rows in the right DataFrame before the merge operation.

- left_duplicated_keys_in_keys: A dictionary containing:
        number: The number of duplicated keys in the left DataFrame.
        percentage: The percentage of duplicated keys in relation to the total number of unique keys.
        cases: A sample of duplicated keys.

- right_duplicated_keys_in_keys: A dictionary similar to the left but for the right DataFrame, showing duplicated keys in the right DataFrame.

- duplicated_keys_error: A boolean that is True if the number of duplicated keys exceeds the set threshold.

- left_null_keys_in_keys: A dictionary showing:
        number: The number of null keys in the left DataFrame.
        percentage: The percentage of null keys in relation to the total number of keys.
        cases: A sample of rows with null keys.

- right_null_keys_in_keys: Similar to left_null_keys_in_keys, but for the right DataFrame, showing rows with null keys.

- null_keys_error: A boolean that is True if the number of null keys exceeds the set threshold.

- number_of_rows_after_merge: The number of rows in the DataFrame after the merge operation.

- number_of_matched_keys: The number of keys that matched between the left and right DataFrames during the merge.

- percentage_of_matched_keys: The percentage of matched keys in relation to the total number of unique keys between both DataFrames.

- number_of_left_keys_without_match: The number of keys that are present in the left DataFrame but have no match in the right DataFrame.

- number_of_right_keys_without_match: The number of keys that are present in the right DataFrame but have no match in the left DataFrame.

- percentage_of_matched_keys_error: A boolean that is True if the percentage of matched keys is lower than the set threshold.

- matched_keys_error: A boolean that is True if there is a significant mismatch between the number of expected and actual matched keys.

- number_of_rows_duplicated: The number of duplicated rows in the merged DataFrame after the merge operation.

- rows_duplicated_error: A boolean that is True if the number of duplicated rows exceeds the set threshold.

## Custom thresholds

```python
error_thresholds = {
    'duplicated_keys': 1,            # Error if more than 1% keys are duplicated
    'null_keys': 1,                  # Error if more than 1% keys are null
    'percentage_matched_keys': 95,   # Error if less than 95% of keys are matched
    'rows_duplicated': 1             # Error if more than 1% rows are duplicated
}

inspector = MergeInspector(
    df_left=df_customers,
    df_right=df_orders,
    error_thresholds=error_thresholds,
    how='inner',
    on='loyalty_member'
)
```

## Error handling

```python
from defensive_pandas_merge.merge_inspector import MergeInspectorException

raise_on_errors = ['rows_duplicated_error', 'matched_keys_error']

inspector = MergeInspector(
    df_left=df_customers,
    df_right=df_orders,
    raise_on_errors=raise_on_errors,
    how='inner',
    on='loyalty_member'
)

try:
    df_merged = inspector.perform_merge()
except MergeInspectorException as e:
    print(f"Merge failed due to errors:\n{e}")
    # Access the full report if needed
    report = e.report
else:
    report = inspector.get_report()
    # Proceed with the merged DataFrame
```

## API Reference

MergeInspector

```python
MergeInspector(
    df_left,
    df_right,
    error_thresholds=None,
    raise_on_errors=None,
    **merge_kwargs
)
```

    df_left (pd.DataFrame): Left DataFrame to merge.
    df_right (pd.DataFrame): Right DataFrame to merge.
    error_thresholds (dict, optional): Thresholds for error detection.
    raise_on_errors (list of str, optional): List of error flags that should raise an exception.
    **merge_kwargs: Additional keyword arguments for the pandas merge function.

Methods

    perform_merge(): Performs the merge and analyzes the result.
    get_report(): Returns a dictionary containing the merge report.

MergeInspectorException

Custom exception raised when specified errors are detected during the merge.
Attributes

    message (str): Error message.
    report (dict): Full merge report.

## Contributing

Contributions are welcome! Please follow these steps:

    Fork the repository.
    Create a new branch for your feature or bug fix.
    Write tests and ensure all tests pass.
    Submit a pull request with a detailed description of your changes.
