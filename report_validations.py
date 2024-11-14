import datetime
import re
import pandas as pd
from datetime import datetime

start_date = "2009-01-01"
end_date = "3000-12-31"


def null_validator(value) -> bool:
    return True


def is_valid_email(email) -> bool:
    if pd.isna(email):
        return False
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return re.fullmatch(pattern, email) is not None


def is_valid_base_measure_code(code) -> bool:
    code = str(code)
    return code.lower() in ("STK", "DAGUR", "KASSI", "KLST", "STYKKI", "KLST.")


def is_valid_customer_type(customer_type) -> bool:
    return customer_type.lower() in ("company", "person") or customer_type.lower() in (
        "inventory",
        "service",
    )


def is_valid_code(code) -> bool:
    code = str(code)
    return len(code) == 2 or code == "RUS"


def is_valid_address_format(address) -> bool:
    pattern = r"^(?:City_x002B_)?(?:Post_x0020_Code_x002B_)?(?:County_x002B_)?(?:Post_x0020_Code_x002B_)?(?:City_x002B_)?(?:County_x002B_)?(?:Post_x0020_Code_x002B_)?(?:City_x002B_)?(?:Post_x0020_Code_x002B_)?(?:City_x002B_)?(?:Post_x0020_Code_x002B_)?(?:City_x002B_)?(?:Post_x0020_Code_x002B_)?(?:City_x002B_)?$"
    return re.fullmatch(pattern, address) is not None


def is_valid_number(number) -> bool:
    try:
        float(number)
        return True
    except ValueError:
        return False


def is_valid_balance(balance) -> bool:
    return str(balance).isnumeric()


def is_valid_bool(value) -> bool:
    return str(value).lower() in ("true", "false")


def is_valid_date(date) -> bool:
    date = str(date)
    try:
        datetime.strptime(date, "%d-%m-%Y")
        datetime.strptime(date, "%Y-%m-%d")
        return True
    except:
        return False


def convert_date(date):
    date = str(date)
    if date[2] == "-" and date[5] == "-":
        date = date.split("-")
        date = date[::-1]
        return ("-").join(date)
    return date


def is_valid_status(status) -> bool:
    status = str(status)
    return status.lower() in ("draft", "open", "paid")


def is_valid_company_number(number) -> bool:
    number = str(number)
    return re.fullmatch(r"^CT", number) is not None


def is_valid_timeframe(date) -> bool:
    return convert_date(end_date) >= convert_date(date) and convert_date(
        date
    ) >= convert_date(start_date)


def is_valid_contact(name) -> bool:
    return name.lower() in ["customer", "Vendor"]


def is_valid_timestamp(date) -> bool:
    date = str(date)
    pattern = r"^\d{4}-\d{2}-\d{2}T.*"
    return re.fullmatch(pattern, date) is not None
