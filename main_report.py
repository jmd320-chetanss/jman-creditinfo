import sys
import pandas as pd
from report_validations import (
    is_valid_number,
    is_valid_bool,
    is_valid_timestamp,
    is_valid_document_no,
    is_valid_type,
    is_valid_blanket_order_no,
    is_valid_vat_identifier,
    is_valid_measure_code,
    null_validation,
)

from report_generator import QualityReportGenerator
from report_exporter import QualityReportExporter

# ------------------------------------------------------------------------------
# Sales Invoice Line
# ------------------------------------------------------------------------------


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
        "Shortcut_Dimension_2_Code": is_valid_number,
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
        "Blanket_Order_No": is_valid_blanket_order_no,
        "VAT_Identifier": is_valid_vat_identifier,
    }

    report_generator.set_validation_map(map=validation_map)

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


# ------------------------------------------------------------------------------
# Contract Line
# ------------------------------------------------------------------------------


def generate_contract_line_report(df: pd.DataFrame):
    report_generator = QualityReportGenerator()
    report_generator.set_report_name("contract_line")
    report_generator.set_dataframe(df)

    validation_map = {
        "Document_No": is_valid_document_no,
        "Line_No": is_valid_number,
        "Sell_to_Customer_No": is_valid_number,
        "No": is_valid_number,
        "Quantity": is_valid_number,
        "Outstanding_Quantity": is_valid_number,
        "Qty_to_Invoice": is_valid_number,
        "Qty_to_Ship": is_valid_number,
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
        "Recalculate_Invoice_Disc": is_valid_bool,
        "Outstanding_Amount": is_valid_number,
        "Qty_Shipped_Not_Invoiced": is_valid_number,
        "Shipped_Not_Invoiced": is_valid_number,
        "Quantity_Shipped": is_valid_number,
        "Quantity_Invoiced": is_valid_number,
        "Shipment_No": is_valid_number,
        "Shipment_Line_No": is_valid_number,
        "Profit_Percent": is_valid_number,
        "Bill_to_Customer_No": is_valid_number,
        "Inv_Discount_Amount": is_valid_number,
        "Purchase_Order_No": is_valid_number,
        "Drop_Shipment": is_valid_bool,
        # "Gen_Bus_Posting_Group": null_validation,
        # "Gen_Prod_Posting_Group": null_validation,
        # "VAT_Calculation_Type": null_validation,
        # "Transaction_Type": null_validation,
        # "Transport_Method": null_validation,
        "Attached_to_Line_No": is_valid_number,
        # "Exit_Point": null_validation,
        # "Area": null_validation,
        # "Transaction_Specification": null_validation,
        # "Tax_Category": null_validation,
        # "Tax_Area_Code": null_validation,
        "Tax_Liable": is_valid_bool,
        # "Tax_Group_Code": null_validation,
        # "VAT_Clause_Code": null_validation,
        # "VAT_Bus_Posting_Group": null_validation,
        "VAT_Prod_Posting_Group": is_valid_number,
        # "Currency_Code": null_validation,
        "Outstanding_Amount_LCY": is_valid_number,
        "Shipped_Not_Invoiced_LCY": is_valid_number,
        "Reserved_Quantity": is_valid_number,
        # "Reserve": null_validation,
        # "Blanket_Order_No": null_validation,
        "Blanket_Order_Line_No": is_valid_number,
        "VAT_Base_Amount": is_valid_number,
        "Unit_Cost": is_valid_number,
        "System_Created_Entry": is_valid_bool,
        "Line_Amount": is_valid_number,
        "VAT_Difference": is_valid_number,
        "Inv_Disc_Amount_to_Invoice": is_valid_number,
        "VAT_Identifier": is_valid_vat_identifier,
        # "IC_Partner_Ref_Type": null_validation,
        # "IC_Partner_Reference": null_validation,
        "Prepayment_Percent": is_valid_number,
        "Prepmt_Line_Amount": is_valid_number,
        "Prepmt_Amt_Inv": is_valid_number,
        "Prepmt_Amt_Incl_VAT": is_valid_number,
        "Prepayment_Amount": is_valid_number,
        "Prempt_VAT_Base_Amt": is_valid_number,
        "Prepayment_VAT_Percent": is_valid_number,
        "Prepmt_VAT_Calc_Type": is_valid_number,
        # "Prepayment_VAT_Identifier": null_validation,
        # "Prepayment_Tax_Area_Code": null_validation,
        "Prepayment_Tax_Liable": is_valid_bool,
        # "Prepayment_Tax_Group_Code": null_validation,
        "Prepmt_Amt_to_Deduct": is_valid_number,
        "Prepmt_Amt_Deducted": is_valid_number,
        "Prepayment_Line": is_valid_bool,
        "Prepmt_Amount_Inv_Incl_VAT": is_valid_number,
        "Prepmt_Amount_Inv_LCY": is_valid_number,
        "IC_Partner_Code": is_valid_number,
        "Prepmt_VAT_Amount_Inv_LCY": is_valid_number,
        "Prepayment_VAT_Difference": is_valid_number,
        "Prepmt_VAT_Diff_to_Deduct": is_valid_number,
        "Prepmt_VAT_Diff_Deducted": is_valid_number,
        "Dimension_Set_ID": is_valid_number,
        "Qty_to_Assemble_to_Order": is_valid_number,
        "Qty_to_Asm_to_Order_Base": is_valid_number,
        "ATO_Whse_Outstanding_Qty": is_valid_number,
        "ATO_Whse_Outstd_Qty_Base": is_valid_number,
        "Job_Task_No": is_valid_number,
        "Job_Contract_Entry_No": is_valid_number,
        "Posting_Date": is_valid_timestamp,
        # "Deferral_Code": null_validation,
        "Returns_Deferral_Start_Date": is_valid_timestamp,
        # "Variant_Code": null_validation,
        # "Bin_Code": null_validation,
        "Qty_per_Unit_of_Measure": is_valid_number,
        "Planned": is_valid_bool,
        "Unit_of_Measure_Code": is_valid_measure_code,
        "Quantity_Base": is_valid_number,
        "Outstanding_Qty_Base": is_valid_number,
        "Qty_to_Invoice_Base": is_valid_number,
        "Qty_to_Ship_Base": is_valid_number,
        "Qty_Shipped_Not_Invd_Base": is_valid_number,
        "Qty_Shipped_Base": is_valid_number,
        "Qty_Invoiced_Base": is_valid_number,
        "Reserved_Qty_Base": is_valid_number,
        "FA_Posting_Date": is_valid_timestamp,
        # "Depreciation_Book_Code": null_validation,
        "Depr_until_FA_Posting_Date": is_valid_bool,
        # "Duplicate_in_Depreciation_Book": null_validation,
        "Use_Duplication_List": is_valid_bool,
        # "Responsibility_Center": null_validation,
        "Out_of_Stock_Substitution": is_valid_bool,
        "Substitution_Available": is_valid_bool,
        # "Originally_Ordered_No": null_validation,
        # "Originally_Ordered_Var_Code": null_validation,
        # "Cross_Reference_No": null_validation,
        # "Unit_of_Measure_Cross_Ref": null_validation,
        # "Cross_Reference_Type": null_validation,
        # "Cross_Reference_Type_No": null_validation,
        # "Item_Category_Code": null_validation,
        "Nonstock": is_valid_bool,
        # "Purchasing_Code": null_validation,
        # "Product_Group_Code": null_validation,
        "Special_Order": is_valid_bool,
        # "Special_Order_Purchase_No": null_validation,
        "Special_Order_Purch_Line_No": is_valid_number,
        "Whse_Outstanding_Qty": is_valid_number,
        "Whse_Outstanding_Qty_Base": is_valid_number,
        "Completely_Shipped": is_valid_bool,
        "Requested_Delivery_Date": is_valid_timestamp,
        "Promised_Delivery_Date": is_valid_timestamp,
        # "Shipping_Time": null_validation,
        # "Outbound_Whse_Handling_Time": null_validation,
        "Planned_Delivery_Date": is_valid_timestamp,
        "Planned_Shipment_Date": is_valid_timestamp,
        # "Shipping_Agent_Code": null_validation,
        # "Shipping_Agent_Service_Code": null_validation,
        "Allow_Item_Charge_Assignment": is_valid_bool,
        "Qty_to_Assign": is_valid_number,
        "Qty_Assigned": is_valid_number,
        "Return_Qty_to_Receive": is_valid_number,
        "Return_Qty_to_Receive_Base": is_valid_number,
        "Return_Qty_Rcd_Not_Invd": is_valid_number,
        "Ret_Qty_Rcd_Not_Invd_Base": is_valid_number,
        "Return_Rcd_Not_Invd": is_valid_number,
        "Return_Rcd_Not_Invd_LCY": is_valid_number,
        "Return_Qty_Received": is_valid_number,
        "Return_Qty_Received_Base": is_valid_number,
        "Appl_from_Item_Entry": is_valid_number,
        # "BOM_Item_No": null_validation,
        # "Return_Receipt_No": null_validation,
        "Return_Receipt_Line_No": is_valid_number,
        # "Return_Reason_Code": null_validation,
        "Allow_Line_Disc": is_valid_bool,
        # "Customer_Disc_Group": null_validation,
        "Related_To_Line_No": is_valid_number,
        # "Salesperson_Code": null_validation,
        "Price_Not_Updated": is_valid_bool,
        "Initial_Price": is_valid_number,
        "Current_Price": is_valid_number,
        "Initial_Index": is_valid_number,
        "Contract_Line_Valid_From": is_valid_timestamp,
        "Contract_Line_Valid_To": is_valid_timestamp,
        # "Index_Type": null_validation,
        "Next_Update_Date": is_valid_timestamp,
        # "Update_Interval": null_validation,
        "Current_Inflation_Rate": is_valid_number,
        # "Contract_No": null_validation,
        "Quantity_from_Job_Ledger_Entry": is_valid_bool,
        # "Job_Filter": null_validation,
        # "Job_Task_Filter": null_validation,
        # "Work_Type_Filter": null_validation,
        # "Unit_of_Measure_Filter": null_validation,
        # "Resource_Type": null_validation,
        "Correction_Line": is_valid_bool,
        "Manual_Job_line": is_valid_bool,
        # "ETag": null_validation,
    }

    report_generator.set_validation_map(map=validation_map)

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


def generate_contract_line_report_from_file(
    dataset_filepath: str, report_filepath: str
):
    contract_line_df = pd.read_csv(dataset_filepath)

    contract_line_report = generate_contract_line_report(df=contract_line_df)

    report_exporter = QualityReportExporter()
    report_exporter.to_csv_file(report=contract_line_report, filepath=report_filepath)


# ------------------------------------------------------------------------------
# Generate Reports
# ------------------------------------------------------------------------------

dataset_path = sys.argv[1]
report_path = sys.argv[2]

generate_contract_line_report_from_file(
    dataset_filepath=dataset_path,
    report_filepath=report_path,
)
