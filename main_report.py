import sys
import pandas as pd
from report_validations import (
    is_valid_email,
    is_valid_customer_type,
    is_valid_number,
    is_valid_balance,
    is_valid_company_number,
    is_valid_date,
    is_valid_status,
    is_valid_bool,
    is_valid_contact,
    is_valid_timestamp,
    is_valid_base_measure_code,
    is_valid_code,
    is_valid_address_format,
    is_valid_timeframe,
)

from report_generator import QualityReportGenerator
from report_exporter import QualityReportExporter


def generate_sales_invoice_line_report(df: pd.DataFrame):
    report_generator = QualityReportGenerator()
    report_generator.set_dataframe(df)

    validation_map = {
        # "Document_No": None,
        "Line_No": is_valid_number,
        "Sell_to_Customer_No": is_valid_number,
        "Type": is_valid_number,
        # "No": None,
        # "Location_Code": None,
        # "Posting_Group": None,
        "Shipment_Date": is_valid_timestamp,
        # "Description": None,
        # "Description_2": None,
        "Unit_of_Measure": is_valid_number,
        "Quantity": is_valid_number,
        "Unit_Price": is_valid_number,
        "Unit_Cost_LCY": is_valid_number,
        "VAT_Percent": is_valid_number,
        "Line_Discount_Percent": is_valid_number,
        "Line_Discount_Amount": is_valid_number,
        "Amount": is_valid_number,
        "Amount_Including_VAT": is_valid_number,
        "Allow_Invoice_Disc": is_valid_bool,
        "Gross_Weight": is_valid_number,
        "Net_Weight": is_valid_number,
        "Units_per_Parcel": is_valid_number,
        "Unit_Volume": is_valid_number,
        "Appl_to_Item_Entry": is_valid_number,
        "Shortcut_Dimension_1_Code": is_valid_number,
        "Shortcut_Dimension_2_Code": is_valid_number,
        # "Customer_Price_Group": None,
        # "Job_No": None,
        # "Work_Type_Code": None,
        # "Shipment_No": None,
        # "Shipment_Line_No": None,
        "Bill_to_Customer_No": is_valid_number,
        "Inv_Discount_Amount": is_valid_number,
        "Drop_Shipment": is_valid_bool,
        # "Gen_Bus_Posting_Group": None,
        # "Gen_Prod_Posting_Group": None,
        # "VAT_Calculation_Type": None,
        # "Transaction_Type": None,
        # "Transport_Method": None,
        # "Attached_to_Line_No": None,
        # "Exit_Point": None,
        # "Area": None,
        # "Transaction_Specification": None,
        # "Tax_Category": None,
        # "Tax_Area_Code": None,
        # "Tax_Liable": None,
        # "Tax_Group_Code": None,
        # "VAT_Clause_Code": None,
        # "VAT_Bus_Posting_Group": None,
        # "VAT_Prod_Posting_Group": None,
        # "Blanket_Order_No": None,
        # "Blanket_Order_Line_No": None,
        # "VAT_Base_Amount": None,
        # "Unit_Cost": None,
        # "System_Created_Entry": None,
        # "Line_Amount": None,
        # "VAT_Difference": None,
        # "VAT_Identifier": None,
        # "IC_Partner_Ref_Type": None,
        # "IC_Partner_Reference": None,
        # "Prepayment_Line": None,
        # "IC_Partner_Code": None,
        # "Posting_Date": None,
        # "Dimension_Set_ID": None,
        # "Job_Task_No": None,
        # "Job_Contract_Entry_No": None,
        # "Deferral_Code": None,
        # "Variant_Code": None,
        # "Bin_Code": None,
        # "Qty_per_Unit_of_Measure": None,
        # "Unit_of_Measure_Code": None,
        # "Quantity_Base": None,
        # "FA_Posting_Date": None,
        # "Depreciation_Book_Code": None,
        # "Depr_until_FA_Posting_Date": None,
        # "Duplicate_in_Depreciation_Book": None,
        # "Use_Duplication_List": None,
        # "Responsibility_Center": None,
        # "Cross_Reference_No": None,
        # "Unit_of_Measure_Cross_Ref": None,
        # "Cross_Reference_Type": None,
        # "Cross_Reference_Type_No": None,
        # "Item_Category_Code": None,
        # "Nonstock": None,
        # "Purchasing_Code": None,
        # "Product_Group_Code": None,
        # "Appl_from_Item_Entry": None,
        # "Return_Reason_Code": None,
        # "Allow_Line_Disc": None,
        # "Customer_Disc_Group": None,
        # "Resource_Type": None,
        # "Correction_Line": None,
        # "Manual_Job_line": None,
        # "ETag": None,
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
            "currencyId": "currencyCode",
            "companyNumber": "type",
            "companyNumber": "companyName",
            ("itemCategoryId", "itemCategoryCode"): "unitPrice",
            "code": "displayName",
            "customerId": "customerNumber",
            "customerId": "customerName",
            "billToCustomerId": "billToName",
        }
    )

    report_generator.check_completeness()
    report_generator.check_uniqueness()
    report_generator.check_validity()
    report_generator.check_timeliness()
    report_generator.check_consistency()
    report = report_generator.generate_report("sales_invoice_line")

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
