import pandas as pd
from report import QualityReport


class QualityReportGenerator:

    df: pd.DataFrame
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
                (consolidated_metrics.invalid_count / total_count) * 100
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

    # def check_timeliness(self):

    #     column_metrics = dict[str, QualityReport.Timeliness]()

    #     for column_name in self._date_columns:
    #         column_df = self.df[column_name].dropna()
    #         total_count = int(len(column_df))

    #         validtor = self._validation_map.get(column_name)
    #         if validtor is None:
    #             validtor = null_validator

    #         valid_dates = column_df.apply(validtor)
    #         valid_count = int(valid_dates.count())

    #         metrics = QualityReport.Timeliness()
    #         metrics.total_count = total_count
    #         metrics.valid_count = valid_count
    #         metrics.invalid_count = total_count - valid_count
    #         metrics.score = valid_count /

    # def check_consistency(self):
    #     consistency = {}
    #     for col in self.df.columns:
    #         if col in self._column_pairing_map:
    #             temp_consistency = {}
    #             inconsistency_count = 0
    #             pair = self._column_pairing_map[col]
    #             for _, row in self.df.iterrows():
    #                 if row[col] not in temp_consistency:
    #                     temp_consistency[row[col]] = row[pair]
    #                 elif (
    #                     row[col] in temp_consistency
    #                     and row[pair] != temp_consistency[row[col]]
    #                 ):
    #                     inconsistency_count += 1
    #             consistency[(col, pair)] = inconsistency_count / len(self.df)

    #     self.inconsistency = consistency
    #     try:
    #         self.avg_consistency = sum(self.inconsistency.values()) / len(
    #             self.inconsistency
    #         )
    #     except:
    #         return

    def generate_report(self, name: str):
        name = name or self.df.Name

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
