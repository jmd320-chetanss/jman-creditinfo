import pandas as pd
import json
from datetime import datetime


class QualityReport:

    class Completeness:
        total_count: int
        value_count: int
        null_count: int
        score: float

    class Uniqueness:
        total_count: int
        unique_count: int
        duplicate_count: int
        score: float

    class Validity:
        total_count: int
        valid_count: int
        invalid_count: int
        score: float

    name: str
    completeness: Completeness
    uniqueness: Uniqueness
    validity: Validity
    completeness_columns: dict[str, Completeness]
    uniqueness_columns: dict[str, Uniqueness]
    validity_columns: dict[str, Validity]


def calc_completeness(df: pd.DataFrame) -> dict[str, QualityReport.Completeness]:
    total_count = len(df.index)
    null_counts = df.isnull().sum()

    reports = dict[str, QualityReport.Completeness]()
    for column in df:
        null_count = int(null_counts[column])

        report = QualityReport.Completeness()
        report.total_count = total_count
        report.null_count = null_count
        report.value_count = total_count - null_count

        score = (report.value_count / total_count) * 100 if total_count else 100
        report.score = round(score, 2)

        reports[column] = report

    return reports


def calc_total_completeness(
    reports: list[QualityReport.Completeness],
) -> QualityReport.Completeness:
    report = QualityReport.Completeness()
    report.total_count = sum([report.total_count for report in reports])
    report.value_count = sum([report.value_count for report in reports])
    report.null_count = sum([report.null_count for report in reports])

    report.score = (
        round(sum([report.score for report in reports]) / len(reports), 2)
        if len(reports)
        else 0
    )

    return report


def calc_uniqueness(df: pd.DataFrame) -> dict[str, QualityReport.Uniqueness]:
    reports = dict[str, QualityReport.Uniqueness]()
    for column_name in df:
        column_df = df[column_name].dropna()
        total_count = len(column_df.index)
        duplicate_count = int(column_df.duplicated().sum())
        unique_count = total_count - duplicate_count

        report = QualityReport.Uniqueness()
        report.total_count = total_count
        report.unique_count = unique_count
        report.duplicate_count = duplicate_count

        score = (unique_count / total_count) * 100 if total_count else 100
        report.score = round(score, 2)

        reports[column_name] = report

    return reports


def calc_total_uniqueness(
    reports: list[QualityReport.Uniqueness],
) -> QualityReport.Uniqueness:
    report = QualityReport.Uniqueness()
    report.total_count = sum([report.total_count for report in reports])
    report.unique_count = sum([report.unique_count for report in reports])
    report.duplicate_count = sum([report.duplicate_count for report in reports])

    report.score = (
        round(sum([report.score for report in reports]) / len(reports), 2)
        if len(reports)
        else 0
    )

    return report


def calc_validity_id(df: pd.DataFrame) -> dict[str, QualityReport.Validity]:
    id_regex = (
        r"^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$"
    )

    reports = dict[str, QualityReport.Validity]()
    for column_name in df:
        column_df = df[column_name].dropna()
        valid_ids = df[column_df.str.match(id_regex)]

        total_count = len(column_df)
        valid_count = len(valid_ids)
        score = (valid_count / total_count) * 100

        report = QualityReport.Validity()
        report.total_count = total_count
        report.valid_count = valid_count
        report.invalid_count = total_count - valid_count
        report.score = score

        reports[column_name] = report

    return reports


def calc_validity_email(df: pd.DataFrame) -> dict[str, QualityReport.Validity]:
    email_regex = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"

    reports = dict[str, QualityReport.Validity]()
    for column_name in df:
        column_df = df[column_name].dropna()
        valid_emails = column_df[column_df.str.match(email_regex)]

        total_count = len(column_df)
        valid_count = len(valid_emails)
        score = (valid_count / total_count) * 100

        report = QualityReport.Validity()
        report.total_count = total_count
        report.valid_count = valid_count
        report.invalid_count = total_count - valid_count
        report.score = score

        reports[column_name] = report

    return reports


def calc_validity_timestamp(df: pd.DataFrame) -> dict[str, QualityReport.Validity]:

    reports = dict[str, QualityReport.Validity]()
    for column_name in df:
        column_df = df[column_name]
        converted_df = pd.to_datetime(column_df, unit="s", errors="coerce")

        valid_timestamps = column_df[
            (converted_df <= datetime.now()) & (converted_df.notnull())
        ]

        total_count = len(df)
        valid_count = len(valid_timestamps)
        score = (valid_count / total_count) * 100
        score = round(score, 2)

        report = QualityReport.Validity()
        report.total_count = total_count
        report.valid_count = valid_count
        report.invalid_count = total_count - valid_count
        report.score = score

        reports[column_name] = report

    return reports


def calc_validity_datetime(df: pd.DataFrame) -> dict[str, QualityReport.Validity]:
    reports = dict[str, QualityReport.Validity]()

    for column_name in df:
        column_df = df[column_name]
        date_format = "%m/%d/%Y %H:%M"

        converted_df = pd.to_datetime(column_df, format=date_format, errors="coerce")
        valid_datetimes = df[converted_df.notnull()]

        total_count = len(df)
        valid_count = len(valid_datetimes)
        score = (valid_count / total_count) * 100
        score = round(score, 2)

        report = QualityReport.Validity()
        report.total_count = total_count
        report.valid_count = valid_count
        report.invalid_count = total_count - valid_count
        report.score = score

        reports[column_name] = report

    return reports


def calc_validity(
    df: pd.DataFrame, strategy_map: dict[str, str]
) -> dict[str, QualityReport.Validity]:

    def get_validator(id: str):
        map = {
            "id": calc_validity_id,
            "email": calc_validity_email,
            "timestamp": calc_validity_timestamp,
            "datetime": calc_validity_datetime,
        }

        return map.get(id)

    reports = dict[str, QualityReport.Validity]()
    for column_name in df:
        column_df = df[column_name]

        validator_id = strategy_map.get(column_name)
        if validator_id is None:
            continue

        validator = get_validator(validator_id)
        if validator is None:
            continue

        reports |= validator(pd.DataFrame(column_df))

    return reports


def calc_total_validity(
    reports: list[QualityReport.Validity],
) -> QualityReport.Validity:
    report = QualityReport.Validity()
    report.total_count = sum([report.total_count for report in reports])
    report.valid_count = sum([report.valid_count for report in reports])
    report.invalid_count = sum([report.invalid_count for report in reports])

    report.score = (
        round(sum([report.score for report in reports]) / len(reports), 2)
        if len(reports)
        else 0
    )

    return report


def calc_companies_report(companies: pd.DataFrame) -> QualityReport:
    report = QualityReport()
    report.name = "companies"

    report.completeness_columns = calc_completeness(companies)
    report.uniqueness_columns = calc_uniqueness(
        companies[
            [
                "id",
                "name",
                "displayName",
                "businessProfileId",
            ]
        ]
    )

    report.validity_columns = calc_validity(
        companies,
        strategy_map={
            "id": "id",
            "businessProfileId": "id",
            "systemCreatedBy": "id",
            "systemModifiedBy": "id",
            "timestamp": "timestamp",
            "systemCreatedAt": "datetime",
            "systemModifiedAt": "datetime",
        },
    )

    report.completeness = calc_total_completeness(report.completeness_columns.values())
    report.uniqueness = calc_total_uniqueness(report.uniqueness_columns.values())
    report.validity = calc_total_validity(report.validity_columns.values())

    return report


def calc_contacts_report(contacts: pd.DataFrame) -> QualityReport:
    report = QualityReport()
    report.name = "contacts"

    report.completeness_columns = calc_completeness(contacts)
    report.uniqueness_columns = calc_uniqueness(contacts)

    report.validity_columns = calc_validity(
        contacts,
        strategy_map={
            "id": "id",
            "company": "id",
            "email": "email",
            "lastInteractionDate": "datetime",
            "lastModifiedDateTime": "datetime",
        },
    )

    report.completeness = calc_total_completeness(report.completeness_columns.values())
    report.uniqueness = calc_total_uniqueness(report.uniqueness_columns.values())
    report.validity = calc_total_validity(report.validity_columns.values())

    return report


def calc_customers_report(customers: pd.DataFrame) -> QualityReport:
    report = QualityReport()
    report.name = "customers"

    report.completeness_columns = calc_completeness(customers)
    report.uniqueness_columns = calc_uniqueness(customers)

    report.validity_columns = calc_validity(
        customers,
        strategy_map={
            "id": "id",
            "taxAreaId": "id",
            "currencyId": "id",
            "paymentTermsId": "id",
            "paymentMethodId": "id",
            "company": "id",
            "email": "email",
            "lastModifiedDateTime": "datetime",
        },
    )

    report.completeness = calc_total_completeness(report.completeness_columns.values())
    report.uniqueness = calc_total_uniqueness(report.uniqueness_columns.values())
    report.validity = calc_total_validity(report.validity_columns.values())

    return report


def calc_items_report(items: pd.DataFrame) -> QualityReport:
    report = QualityReport()
    report.name = "items"

    report.completeness_columns = calc_completeness(items)
    report.uniqueness_columns = calc_uniqueness(items)

    report.validity_columns = calc_validity(
        items,
        strategy_map={
            "id": "id",
            "itemCategoryId": "id",
            "taxGroupId": "id",
            "baseUnitOfMeasureId": "id",
            "generalProductPostingGroupId": "id",
            "inventoryPostingGroupId": "id",
            "company": "id",
            "lastModifiedDateTime": "datetime",
        },
    )

    report.completeness = calc_total_completeness(report.completeness_columns.values())
    report.uniqueness = calc_total_uniqueness(report.uniqueness_columns.values())
    report.validity = calc_total_validity(report.validity_columns.values())

    return report


def calc_regions_report(regions: pd.DataFrame) -> QualityReport:
    report = QualityReport()
    report.name = "regions"

    report.completeness_columns = calc_completeness(regions)
    report.uniqueness_columns = calc_uniqueness(regions)

    report.validity_columns = calc_validity(
        regions,
        strategy_map={
            "id": "id",
            "company": "id",
            "lastModifiedDateTime": "datetime",
        },
    )

    report.completeness = calc_total_completeness(report.completeness_columns.values())
    report.uniqueness = calc_total_uniqueness(report.uniqueness_columns.values())
    report.validity = calc_total_validity(report.validity_columns.values())

    return report


def calc_sales_invoices_report(sales_invoices: pd.DataFrame) -> QualityReport:
    report = QualityReport()
    report.name = "sales_invoices"

    report.completeness_columns = calc_completeness(sales_invoices)
    report.uniqueness_columns = calc_uniqueness(sales_invoices)

    report.validity_columns = calc_validity(
        sales_invoices,
        strategy_map={
            "id": "id",
            "customerId": "id",
            "billToCustomerId": "id",
            "currencyId": "id",
            "orderId": "id",
            "paymentTermsId": "id",
            "shipmentMethodId": "id",
            "disputeStatusId": "id",
            "company": "id",
            "email": "email",
            "invoiceDate": "datetime",
            "postingDate": "datetime",
            "dueDate": "datetime",
            "promisedPayDate": "datetime",
            "lastModifiedDateTime": "datetime",
        },
    )

    report.completeness = calc_total_completeness(report.completeness_columns.values())
    report.uniqueness = calc_total_uniqueness(report.uniqueness_columns.values())
    report.validity = calc_total_validity(report.validity_columns.values())

    return report


def report_to_json(report: QualityReport) -> str:
    return json.dumps(report, indent=2, default=lambda obj: obj.__dict__)


def report_to_json_file(report: QualityReport, filepath: str):
    json_string = report_to_json(report)
    with open(filepath, "w", encoding="utf8") as file:
        file.write(json_string)


def load_companies() -> pd.DataFrame:
    df = pd.read_csv("out/companies.csv")
    df["businessProfileId"] = df["businessProfileId"].astype(str)

    return df


companies_df = load_companies()
contacts_df = pd.read_csv("out/contacts.csv")
customers_df = pd.read_csv("out/customers.csv")
items_df = pd.read_csv("out/items.csv")
regions_df = pd.read_csv("out/regions.csv")
sales_invoices_df = pd.read_csv("out/sales_invoices.csv")

companies_report = calc_companies_report(companies_df)
contacts_report = calc_contacts_report(contacts_df)
customers_report = calc_customers_report(customers_df)
items_report = calc_items_report(items_df)
regions_report = calc_regions_report(regions_df)
sales_invoices_report = calc_sales_invoices_report(sales_invoices_df)

report_to_json_file(companies_report, "reports/companies_report.json")
report_to_json_file(contacts_report, "reports/contacts_report.json")
report_to_json_file(customers_report, "reports/customers_report.json")
report_to_json_file(items_report, "reports/items_report.json")
report_to_json_file(regions_report, "reports/regions_report.json")
report_to_json_file(sales_invoices_report, "reports/sales_invoices_report.json")
