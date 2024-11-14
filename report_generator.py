import pandas as pd
from report import QualityReport
from concurrent.futures import ThreadPoolExecutor


class QualityReportGenerator:

    df: pd.DataFrame
    report_name = ""
    completeness_columns = dict[str, QualityReport.Completeness]()
    uniqueness_columns = dict[str, QualityReport.Uniqueness]()
    validity_columns = dict[str, QualityReport.Validity]()
    consistency_columns = dict[str, QualityReport.Consistency]()
    completeness = QualityReport.Completeness()
    uniqueness = QualityReport.Uniqueness()
    validity = QualityReport.Validity()
    consistency = QualityReport.Consistency()
    timeliness = QualityReport.Timeliness()

    _validation_map = dict[str, any]()
    _column_pairing_map = dict[str, str]()
    _date_columns = list()
    _completeness_task = None
    _uniqueness_task = None
    _validity_task = None
    _consistency_task = None
    _timeliness_task = None
    _executor: ThreadPoolExecutor

    def __init__(self, max_worker_count=None):
        self._executor = ThreadPoolExecutor(max_workers=max_worker_count)

    def set_report_name(self, name: str):
        self.report_name = name

    def set_dataframe(self, df: pd.DataFrame):
        self.df = df

    def set_validation_map(self, map: dict[str, any]):
        self._validation_map = map

    def get_validation_map(self):
        return self._validation_map

    def set_column_pairing_map(self, map: dict[str, str]):
        self._column_pairing_map = map

    def get_column_pairing_map(self):
        return self._column_pairing_map

    def set_date_columns(self, columns: list):
        self._date_columns = columns

    def get_date_columns(self) -> list:
        return self._date_columns

    def check_completeness(self):
        df = self.df
        total_count = len(df.index)
        null_counts = df.isnull().sum()

        column_metrics = dict[str, QualityReport.Completeness]()
        for column_name in df:
            null_count = int(null_counts[column_name])

            consolidated_metrics = QualityReport.Completeness()
            consolidated_metrics.total_count = total_count
            consolidated_metrics.null_count = null_count
            consolidated_metrics.value_count = total_count - null_count

            score = (
                (consolidated_metrics.value_count / total_count) * 100
                if total_count
                else 100
            )
            consolidated_metrics.score = round(score, 2)

            column_metrics[column_name] = consolidated_metrics

        # calculating consolidated metrics
        column_metrics_values = column_metrics.values()

        consolidated_metrics = QualityReport.Completeness()
        consolidated_metrics.total_count = sum(
            [metrics.total_count for metrics in column_metrics_values]
        )
        consolidated_metrics.value_count = sum(
            [metrics.value_count for metrics in column_metrics_values]
        )
        consolidated_metrics.null_count = sum(
            [metrics.null_count for metrics in column_metrics_values]
        )

        consolidated_metrics.score = (
            round(
                sum([metrics.score for metrics in column_metrics_values])
                / len(column_metrics_values),
                2,
            )
            if len(column_metrics_values)
            else 0
        )

        self.completeness = consolidated_metrics
        self.completeness_columns = column_metrics

    def check_uniqueness(self):
        df = self.df

        column_metrics = dict[str, QualityReport.Uniqueness]()
        for column_name in df:
            column_df = df[column_name].dropna()
            total_count = len(column_df.index)
            duplicate_count = int(column_df.duplicated().sum())
            unique_count = total_count - duplicate_count

            consolidated_metrics = QualityReport.Uniqueness()
            consolidated_metrics.total_count = total_count
            consolidated_metrics.unique_count = unique_count
            consolidated_metrics.duplicate_count = total_count - unique_count

            score = (
                (consolidated_metrics.duplicate_count / total_count) * 100
                if total_count
                else 100
            )
            consolidated_metrics.score = round(score, 2)

            column_metrics[column_name] = consolidated_metrics

        # calculating consolidated metrics
        column_metrics_values = column_metrics.values()

        consolidated_metrics = QualityReport.Uniqueness()
        consolidated_metrics.total_count = sum(
            [metrics.total_count for metrics in column_metrics_values]
        )
        consolidated_metrics.unique_count = sum(
            [metrics.unique_count for metrics in column_metrics_values]
        )
        consolidated_metrics.duplicate_count = sum(
            [metrics.duplicate_count for metrics in column_metrics_values]
        )

        consolidated_metrics.score = (
            round(
                sum([metrics.score for metrics in column_metrics_values])
                / len(column_metrics_values),
                2,
            )
            if len(column_metrics_values)
            else 0
        )

        self.uniqueness = consolidated_metrics
        self.uniqueness_columns = column_metrics

    def check_validity(self):
        df = self.df

        column_metrics = dict[str, QualityReport.Validity]()
        for column_name in df:
            column_df = df[column_name].dropna()
            total_count = int(len(column_df.index))

            validator = self._validation_map.get(column_name)
            if validator is None:
                valid_count = total_count
            else:
                valid_count = int(column_df.apply(validator).count())

            consolidated_metrics = QualityReport.Validity()
            consolidated_metrics.total_count = total_count
            consolidated_metrics.valid_count = valid_count
            consolidated_metrics.invalid_count = total_count - valid_count

            score = (
                (consolidated_metrics.valid_count / total_count) * 100
                if total_count
                else 100
            )
            consolidated_metrics.score = round(score, 2)

            column_metrics[column_name] = consolidated_metrics

        # calculating consolidated metrics
        column_metrics_values = column_metrics.values()

        consolidated_metrics = QualityReport.Validity()
        consolidated_metrics.total_count = sum(
            [metrics.total_count for metrics in column_metrics_values]
        )
        consolidated_metrics.valid_count = sum(
            [metrics.valid_count for metrics in column_metrics_values]
        )
        consolidated_metrics.invalid_count = sum(
            [metrics.invalid_count for metrics in column_metrics_values]
        )

        consolidated_metrics.score = (
            round(
                sum([metrics.score for metrics in column_metrics_values])
                / len(column_metrics_values),
                2,
            )
            if len(column_metrics_values)
            else 0
        )

        self.validity = consolidated_metrics
        self.validity_columns = column_metrics

    def check_timeliness(self):
        pass

    def check_consistency(self):

        column_pairing_map = self._column_pairing_map

        column_metrics = dict[str, QualityReport.Consistency]()

        for column_name in self.df.columns:

            column_df = self.df[column_name].dropna()
            total_count = int(len(column_df))
            inconsistency_count = 0

            if column_name not in column_pairing_map:
                inconsistency_count = 0
            else:
                temp_consistency = {}
                pair = column_pairing_map[column_name]
                for _, row in self.df.iterrows():
                    if row[column_name] not in temp_consistency:
                        temp_consistency[row[column_name]] = row[pair]
                    elif (
                        row[column_name] in temp_consistency
                        and row[pair] != temp_consistency[row[column_name]]
                    ):
                        inconsistency_count += 1

            metric = QualityReport.Consistency()
            metric.total_count = total_count
            metric.consistent_count = total_count - inconsistency_count
            metric.inconsistent_count = inconsistency_count
            metric.score = round(
                (metric.consistent_count / total_count) * 100 if total_count else 100, 2
            )

            column_metrics[column_name] = metric

        # calculating consolidated metrics
        column_metrics_values = column_metrics.values()

        consolidated_metrics = QualityReport.Consistency()
        consolidated_metrics.total_count = sum(
            [metrics.total_count for metrics in column_metrics_values]
        )
        consolidated_metrics.consistent_count = sum(
            [metrics.consistent_count for metrics in column_metrics_values]
        )
        consolidated_metrics.inconsistent_count = sum(
            [metrics.inconsistent_count for metrics in column_metrics_values]
        )

        consolidated_metrics.score = (
            round(
                sum([metrics.score for metrics in column_metrics_values])
                / len(column_metrics_values),
                2,
            )
            if len(column_metrics_values)
            else 0
        )

        self.consistency = consolidated_metrics
        self.consistency_columns = column_metrics

    def check_completeness_async(self):
        self._completeness_task = self._executor.submit(self.check_completeness)

    def check_uniqueness_async(self):
        self._uniqueness_task = self._executor.submit(self.check_uniqueness)

    def check_validity_async(self):
        self._validity_task = self._executor.submit(self.check_validity)

    def check_consistency_async(self):
        self._consistency_task = self._executor.submit(self.check_consistency)

    def check_timeliness_async(self):
        self._timeliness_task = self._executor.submit(self.check_timeliness)

    def generate_report(self):
        name = self.report_name or self.df.Name

        self._executor.shutdown()

        report = QualityReport()
        report.name = name
        report.completeness_columns = self.completeness_columns
        report.validity_columns = self.validity_columns
        report.uniqueness_columns = self.uniqueness_columns
        report.consistency_columns = self.consistency_columns
        report.completeness = self.completeness
        report.validity = self.validity
        report.uniqueness = self.uniqueness
        report.consistency = self.consistency
        report.timeliness = self.timeliness

        return report
