import pandas as pd
import os

file = "data/sample_csv.csv"
file1 = "data/sample_csv copy.csv"


class CSVValidator:
    def __init__(self, file):
        self._file = file
        self._df = None
        self._required_columns = ["date", "description", "amount", "type"]

    def validate(self):
        self._load_csv()
        self._check_columns()
        self._check_nulls()
        self._check_amount()
        self._check_type()
        self._check_date()
        return self._df

    def _load_csv(self):
        if not os.path.exists(self._file):
            raise FileNotFoundError(f"The file '{self._file}' does not exist.")

        try:
            self._df = pd.read_csv(self._file)
            if self._df.empty:
                raise ValueError(f"The file '{self._file}' is empty.")
            return self._df
        except pd.errors.EmptyDataError:
            raise ValueError(f"CSV is empty: {self._file}")
        except Exception as e:
            raise Exception(f"Loading failed: {e}")

    def _check_columns(self):
        missing_columns = set(self._required_columns) - set(self._df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")

    def _check_nulls(self):
        problem_rows = self._df[self._df[self._required_columns].isnull().any(axis=1)]
        if not problem_rows.empty:
            error_messages = []
            for idx, row in problem_rows.iterrows():
                null_cols = row[self._required_columns][
                    row[self._required_columns].isnull()
                ].index.tolist()
                line_num = idx + 2
                error_messages.append(
                    f"Line {line_num}: Missing {', '.join(null_cols)}"
                )
            raise ValueError(
                "Data Integrity Issues Found:\n" + "\n".join(error_messages)
            )

    def _check_amount(self):
        invalid_rows = self._df[self._df["amount"] < 0]
        error_messages = []
        for idx, row in invalid_rows.iterrows():
            line_num = idx + 2
            error_messages.append(f"Line {line_num}: Invalid amount {row['amount']}")
        if error_messages:
            raise ValueError("Amount Validation Failed:\n" + "\n".join(error_messages))

    def _check_type(self):
        self._df["type"] = self._df["type"].str.lower()
        allowed_types = {"income", "expense"}
        invalid_rows = self._df[~self._df["type"].isin(allowed_types)]
        error_messages = []
        for idx, row in invalid_rows.iterrows():
            line_num = idx + 2
            error_messages.append(f"Line {line_num}: Invalid type '{row['type']}'")
        if error_messages:
            raise ValueError("Type Validation Failed:\n" + "\n".join(error_messages))

    def _check_date(self):
        self._df["date"] = pd.to_datetime(
            self._df["date"], format="%Y-%m-%d", errors="coerce")
        invalid_rows = self._df[self._df["date"].isna()]
        if not invalid_rows.empty:
            error_messages = []
            for idx, row in invalid_rows.iterrows():
                line_num = idx + 2  # CSV line number
                error_messages.append(f"Line {line_num}: Invalid date '{row['date']}'")
            raise ValueError("Date Validation Failed:\n" + "\n".join(error_messages))


validator = CSVValidator(file)
clean_df = validator.validate()
print(clean_df)
