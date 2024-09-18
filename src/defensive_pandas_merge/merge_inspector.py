import pandas as pd


class MergeInspectorException(Exception):
    """Custom exception for MergeInspector errors."""
    def __init__(self, message, report):
        super().__init__(message)
        self.report = report

class MergeInspector:
    def __init__(self, df_left, df_right, error_thresholds=None, raise_on_errors=None, **merge_kwargs):
        """
        Initializes the MergeInspector with DataFrames, merge parameters, error thresholds,
        and specifies which errors should raise an exception.
        """
        self.df_left = df_left.copy()
        self.df_right = df_right.copy()
        self.merge_kwargs = merge_kwargs
        self.error_thresholds = error_thresholds or {
            'duplicated_keys': 0,
            'null_keys': 0,
            'percentage_matched_keys': 100,  # Expect 100% matched keys in the ideal case
            'rows_duplicated': 0
        }
        self.raise_on_errors = raise_on_errors or []
        self.report = {}
        self._perform_checks()

    def _perform_checks(self):
        """
        Performs initial checks on the DataFrames before merging.
        """
        # Record the number of rows before merge
        self.report['left_number_of_rows_before_merge'] = self.df_left.shape[0]
        self.report['right_number_of_rows_before_merge'] = self.df_right.shape[0]

        # Get merge keys
        on_keys = self.merge_kwargs.get('on')
        left_on = self.merge_kwargs.get('left_on', on_keys)
        right_on = self.merge_kwargs.get('right_on', on_keys)

        # Ensure keys are lists for consistency
        if isinstance(left_on, str):
            left_on = [left_on]
        if isinstance(right_on, str):
            right_on = [right_on]

        self.left_on = left_on
        self.right_on = right_on

        # Exclude nulls when checking for duplicates
        df_left_no_nulls = self.df_left.dropna(subset=left_on)
        df_right_no_nulls = self.df_right.dropna(subset=right_on)

        # Check for duplicates in merge keys and collect sample cases
        left_duplicates = df_left_no_nulls[df_left_no_nulls.duplicated(subset=left_on, keep=False)]
        right_duplicates = df_right_no_nulls[df_right_no_nulls.duplicated(subset=right_on, keep=False)]

        left_duplicate_keys = left_duplicates[left_on].drop_duplicates()
        right_duplicate_keys = right_duplicates[right_on].drop_duplicates()

        # Calculate total unique keys excluding nulls
        left_total_keys = df_left_no_nulls[left_on].drop_duplicates().shape[0]
        right_total_keys = df_right_no_nulls[right_on].drop_duplicates().shape[0]

        left_duplicate_keys_percentage = round((left_duplicate_keys.shape[0] / left_total_keys) * 100,
                                               4) if left_total_keys else 0
        right_duplicate_keys_percentage = round((right_duplicate_keys.shape[0] / right_total_keys) * 100,
                                                4) if right_total_keys else 0

        self.report['left_duplicated_keys_in_keys'] = {
            'number': left_duplicate_keys.shape[0],
            'percentage': left_duplicate_keys_percentage,
            'cases': left_duplicate_keys.head(3).to_dict('records')
        }
        self.report['right_duplicated_keys_in_keys'] = {
            'number': right_duplicate_keys.shape[0],
            'percentage': right_duplicate_keys_percentage,
            'cases': right_duplicate_keys.head(3).to_dict('records')
        }

        # Error flag for duplicated keys with threshold
        self.report['duplicated_keys_error'] = (
                left_duplicate_keys_percentage > self.error_thresholds.get('duplicated_keys', 0) and
                right_duplicate_keys_percentage > self.error_thresholds.get('duplicated_keys', 0)
        )

        # Check for nulls in merge keys and collect sample cases
        left_nulls = self.df_left[self.df_left[left_on].isnull().any(axis=1)]
        right_nulls = self.df_right[self.df_right[right_on].isnull().any(axis=1)]

        # Count total rows with null keys
        left_null_rows = left_nulls.shape[0]
        right_null_rows = right_nulls.shape[0]

        # Calculate percentage of null keys based on total number of rows
        left_null_keys_percentage = round((left_null_rows / self.report['left_number_of_rows_before_merge']) * 100, 4)
        right_null_keys_percentage = round((right_null_rows / self.report['right_number_of_rows_before_merge']) * 100,
                                           4)

        self.report['left_null_keys_in_keys'] = {
            'number': left_null_rows,
            'percentage': left_null_keys_percentage,
            'cases': left_nulls[left_on].head(3).to_dict('records')
        }
        self.report['right_null_keys_in_keys'] = {
            'number': right_null_rows,
            'percentage': right_null_keys_percentage,
            'cases': right_nulls[right_on].head(3).to_dict('records')
        }

        # Error flag for null keys with threshold (using 'or' as per your requirement)
        self.report['null_keys_error'] = (
                left_null_keys_percentage > self.error_thresholds.get('null_keys', 0) or
                right_null_keys_percentage > self.error_thresholds.get('null_keys', 0)
        )

    def perform_merge(self):
        """
        Performs the merge and updates the report with post-merge statistics.
        If any specified errors are True, raises an exception.
        """
        # Perform the merge
        self.df_merged = self.df_left.merge(self.df_right, **self.merge_kwargs)

        # Record the number of rows after merge
        self.report['number_of_rows_after_merge'] = self.df_merged.shape[0]

        # Analyze the merge result
        self._analyze_merge()

        # Check for errors and raise exception if needed
        self._check_for_errors()

        return self.df_merged

    def _analyze_merge(self):
        """
        Analyzes the merged DataFrame to provide additional statistics.
        """
        # Identify matching and non-matching keys
        left_keys = self.df_left[self.left_on].drop_duplicates()
        right_keys = self.df_right[self.right_on].drop_duplicates()

        # Merge keys to find matches and mismatches
        keys_merged = left_keys.merge(
            right_keys,
            left_on=self.left_on,
            right_on=self.right_on,
            how='outer',
            indicator=True
        )

        total_unique_keys = len(keys_merged)
        matched_keys = keys_merged[keys_merged['_merge'] == 'both']
        matched_keys_count = matched_keys.shape[0]
        percentage_matched_keys = round((matched_keys_count / total_unique_keys) * 100, 2) if total_unique_keys else 0

        self.report['number_of_matched_keys'] = matched_keys_count
        self.report['percentage_of_matched_keys'] = percentage_matched_keys

        # Updated to include example cases
        left_keys_without_match = keys_merged[keys_merged['_merge'] == 'left_only']
        right_keys_without_match = keys_merged[keys_merged['_merge'] == 'right_only']

        self.report['number_of_left_keys_without_match'] = {
            'number': left_keys_without_match.shape[0],
            'cases': left_keys_without_match[self.left_on].head(3).to_dict('records')
        }
        self.report['number_of_right_keys_without_match'] = {
            'number': right_keys_without_match.shape[0],
            'cases': right_keys_without_match[self.right_on].head(3).to_dict('records')
        }

        # Error flag for percentage of matched keys with threshold
        self.report['percentage_of_matched_keys_error'] = (
                percentage_matched_keys < self.error_thresholds.get('percentage_matched_keys', 100)
        )

        # Error flag for matched keys (maintaining consistency with previous 'matched_keys_error' field)
        self.report['matched_keys_error'] = self.report['percentage_of_matched_keys_error']

        # Detect if rows have been duplicated
        expected_rows = self._expected_row_count()
        if expected_rows and self.report['number_of_rows_after_merge'] > expected_rows:
            self.report['number_of_rows_duplicated'] = self.report['number_of_rows_after_merge'] - expected_rows
            duplicated_rows_percentage = round((self.report['number_of_rows_duplicated'] / expected_rows) * 100, 2)
        else:
            self.report['number_of_rows_duplicated'] = 0
            duplicated_rows_percentage = 0

        # Error flag for duplicated rows with threshold
        self.report['rows_duplicated_error'] = (
                duplicated_rows_percentage > self.error_thresholds.get('rows_duplicated', 0)
        )

    def _check_for_errors(self):
        """
        Checks if any of the specified error flags are True and raises an exception if so.
        Includes the full report in the exception message.
        """
        errors_triggered = [error for error in self.raise_on_errors if self.report.get(error)]
        if errors_triggered:
            error_messages = [f"{error} is True" for error in errors_triggered]
            full_message = (
                    "Errors detected during merge analysis:\n" +
                    "; ".join(error_messages) +
                    "\n\nFull Report:\n" +
                    "\n".join([f"{key}: {value}" for key, value in self.report.items()])
            )
            raise MergeInspectorException(full_message, self.report)

    def _expected_row_count(self):
        """
        Estimates the expected number of rows after merge based on the 'how' parameter.
        """
        how = self.merge_kwargs.get('how', 'inner')
        if how == 'inner':
            return min(self.report['left_number_of_rows_before_merge'],
                       self.report['right_number_of_rows_before_merge'])
        elif how == 'left':
            return self.report['left_number_of_rows_before_merge']
        elif how == 'right':
            return self.report['right_number_of_rows_before_merge']
        elif how == 'outer':
            return None  # Cannot determine without key analysis
        else:
            return None

    def get_report(self):
        """
        Returns the report dictionary containing merge statistics.
        """
        return self.report

