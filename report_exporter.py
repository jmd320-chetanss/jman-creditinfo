from report import QualityReport
import json
import os
import pandas as pd


class QualityReportExporter:

    def to_json(self, report: QualityReport) -> str:
        return json.dumps(report, indent=2, default=lambda obj: obj.__dict__)

    def to_json_file(self, report: QualityReport, filepath: str):
        json_data = self.to_json(report)
        self._write_to_file(content=json_data, filepath=filepath)

    def to_csv(self, report: QualityReport) -> str:

        records = []

        def add_records(columns: list, table_name: str, metrics_name: str):

            for column_name in columns:
                metric = columns[column_name]

                record = {
                    "source_name": "dynamics-nav",
                    "table_name": table_name,
                    "column_name": column_name,
                    "metrics_name": metrics_name,
                    "score": metric.score,
                    "good_count": 0,
                    "bad_count": 0,
                }

                records.append(record)

        add_records(
            columns=report.completeness_columns,
            table_name="sales_invoice_line",
            metrics_name="completeness",
        )
        add_records(
            columns=report.uniqueness_columns,
            table_name="sales_invoice_line",
            metrics_name="uniqueness",
        )
        add_records(
            columns=report.validity_columns,
            table_name="sales_invoice_line",
            metrics_name="validity",
        )
        add_records(
            columns=report.consistency_columns,
            table_name="sales_invoice_line",
            metrics_name="consistency",
        )
        add_records(
            columns=report.timeliness_columns,
            table_name="sales_invoice_line",
            metrics_name="timeliness",
        )

        df = pd.DataFrame(records)
        csv_data = df.to_csv(index=False, lineterminator="\n")

        return csv_data

    def to_csv_file(self, report: QualityReport, filepath: str):
        csv_data = self.to_csv(report)
        self._write_to_file(content=csv_data, filepath=filepath)

    def _write_to_file(self, content: str, filepath: str):
        dirname = os.path.dirname(filepath)
        os.makedirs(dirname, exist_ok=True)

        with open(filepath, mode="w", encoding="utf8") as file:
            file.write(content)
