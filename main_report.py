import sys
import pandas as pd
from report_validations import (
    is_valid_number,
    is_valid_bool,
    is_valid_timestamp,
    is_valid_document_no,
    is_valid_type,
)

from report_generator import QualityReportGenerator
from report_exporter import QualityReportExporter


def generate_sales_invoice_line_report(df: pd.DataFrame):
    report_generator = QualityReportGenerator()
    report_generator.set_report_name("sales_invoice_line")
    report_generator.set_dataframe(df)

    validation_map = {
        "Document_No": is_valid_document_no,
        "Line_No": is_valid_number,
        "Sell_to_Customer_No": is_valid_number,
        "Type": is_valid_type,
        "No": is_valid_number,
        "Shipment_Date": is_valid_timestamp,
        "Quantity": is_valid_number,
        "Unit_Price": is_valid_number,
        "Unit_Cost_LCY": is_valid_number,
        "VAT_Percent": is_valid_number,
        "Line_Discount_Percent": is_valid_number,
        "Line_Discount_Amount": is_valid_number,
        "Amount": is_valid_number,
        "Amount_Including_VAT": is_valid_number,
        "Allow_Invoice_Disc": is_valid_bool,
        "Shortcut_Dimension_1_Code": is_valid_number,
        "Bill_to_Customer_No": is_valid_number,
        "Inv_Discount_Amount": is_valid_number,
        "Drop_Shipment": is_valid_bool,
        "VAT_Base_Amount": is_valid_number,
        "Unit_Cost": is_valid_number,
        "System_Created_Entry": is_valid_bool,
        "Line_Amount": is_valid_number,
        "Posting_Date": is_valid_timestamp,
        "Dimension_Set_ID": is_valid_number,
        "Qty_per_Unit_of_Measure": is_valid_number,
        "Quantity_Base": is_valid_number,
    }

    report_generator.set_validation_map(map=validation_map)

    report_generator.set_date_columns(
        [
            "invoiceDate",
            "postingDate",
            "dueDate",
            "promisedPayDate",
            "systemCreatedAt",
            "systemModifiedAt",
            "lastModifiedDateTime",
            "startingDate",
        ]
    )

    report_generator.set_column_pairing_map(
        {
            "Type": "VAT_Difference",
            "VAT_Identifier": "VAT_Prod_Posting_Group",
        }
    )

    report_generator.check_completeness_async()
    report_generator.check_uniqueness_async()
    report_generator.check_validity_async()
    report_generator.check_timeliness_async()
    report_generator.check_consistency_async()
    report = report_generator.generate_report()

    return report


def generate_sales_invoice_line_report_from_file(
    dataset_filepath: str, report_filepath: str
):
    sales_invoice_line_df = pd.read_csv(dataset_filepath)

    sales_invoice_line_report = generate_sales_invoice_line_report(
        df=sales_invoice_line_df
    )

    report_exporter = QualityReportExporter()
    report_exporter.to_csv_file(
        report=sales_invoice_line_report, filepath=report_filepath
    )


sales_invoice_line_dataset_path = sys.argv[1]
sales_invoice_line_report_path = sys.argv[2]

generate_sales_invoice_line_report_from_file(
    dataset_filepath=sales_invoice_line_dataset_path,
    report_filepath=sales_invoice_line_report_path,
)
