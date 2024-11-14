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

        column_names = report.completeness_columns.keys()
        for column_name in column_names:
            record = {
                "column_name": column_name,
                "completeness": report.completeness_columns[column_name].score,
                "uniqueness": report.uniqueness_columns[column_name].score,
                "validity": report.validity_columns[column_name].score,
                "consistency": report.consistency_columns[column_name].score,
                # "timeliness": report.timeliness_columns[column_name].score,
            }

            records.append(record)

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
