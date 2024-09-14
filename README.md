# defensive-pandas-merge

[![PyPI version](https://badge.fury.io/py/defensive-pandas-merge.svg)](https://badge.fury.io/py/defensive-pandas-merge)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Python library that enhances pandas DataFrame merge operations by providing detailed diagnostics, error reporting, and customizable error handling.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
  - [Basic Usage](#basic-usage)
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
from defensive_pandas_merge import DefensiveMerge

# Sample DataFrames
df_customers = pd.DataFrame({
    'customer_id': ['C001', 'C002', 'C003', 'C004'],
    'loyalty_member': ['L001', 'L002', 'L003', 'L004']
})

df_orders = pd.DataFrame({
    'order_id': [101, 102, 103],
    'loyalty_member': ['L001', 'L002', 'L005']
})

# Initialize the DefensiveMerge
inspector = DefensiveMerge(
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

## Custom thresholds

```python
error_thresholds = {
    'duplicated_keys': 1,            # Error if more than 1% keys are duplicated
    'null_keys': 1,                  # Error if more than 1% keys are null
    'percentage_matched_keys': 95,   # Error if less than 95% of keys are matched
    'rows_duplicated': 1             # Error if more than 1% rows are duplicated
}

inspector = DefensiveMerge(
    df_left=df_customers,
    df_right=df_orders,
    error_thresholds=error_thresholds,
    how='inner',
    on='loyalty_member'
)
```

## Error handling

```python
from defensive_pandas_merge import DefensiveMergeException

raise_on_errors = ['rows_duplicated_error', 'matched_keys_error']

inspector = DefensiveMerge(
    df_left=df_customers,
    df_right=df_orders,
    raise_on_errors=raise_on_errors,
    how='inner',
    on='loyalty_member'
)

try:
    df_merged = inspector.perform_merge()
except DefensiveMergeException as e:
    print(f"Merge failed due to errors:\n{e}")
    # Access the full report if needed
    report = e.report
else:
    report = inspector.get_report()
    # Proceed with the merged DataFrame
```

## API Reference

```python
DefensiveMerge(
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

DefensiveMergeException

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
