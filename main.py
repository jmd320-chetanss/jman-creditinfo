# %%

import requests
import dotenv
import os
import logging
from http import HTTPStatus

# ----------------------------------------------------------------------------
# Configuration setup
# ----------------------------------------------------------------------------

# Load env vars
dotenv.load_dotenv()


# constant values which could not be put into options
class Constants:
    section_separator = (
        "----------------------------------------------------------------------"
    )
    defult_log_level = "INFO"
    default_log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


constants = Constants()


# configuration
class Options:
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    tenant_id = os.getenv("TENANT_ID")
    env_name = os.getenv("ENV_NAME")
    scopes = [os.getenv("SCOPE")]
    output_dir = os.getenv("OUTPUT_FOLDER")
    log_level = os.getenv("LOG_LEVEL") or constants.defult_log_level
    log_format = os.getenv("LOG_FORMAT") or constants.default_log_format
    log_options = True


options = Options()
api_prefix = f"https://api.businesscentral.dynamics.com/v2.0/{options.tenant_id}/{options.env_name}/api/v2.0"

# ----------------------------------------------------------------------------
# Logging setup
# ----------------------------------------------------------------------------


def create_logger(name: str) -> logging.Logger:

    logger = logging.Logger(name)
    logger.setLevel(options.log_level)

    # console handler
    handler = logging.StreamHandler()
    handler.setLevel(options.log_level)

    # setting up formatter
    formatter = logging.Formatter(options.log_format)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


logger = create_logger("main")

# Log options if asked, used to debug
if options.log_options:
    logger.info(f"logging options...")
    logger.info(constants.section_separator)
    logger.info(f"client_id: {options.client_id}")
    logger.info(f"client_secret: {options.client_secret}")
    logger.info(f"tenant_id: {options.tenant_id}")
    logger.info(f"scopes: {options.scopes}")
    logger.info(f"log_level: {options.log_level}")
    logger.info(f"log_format: {options.log_format}")
    logger.info(constants.section_separator)
    logger.info(f"logging options done.")

# ----------------------------------------------------------------------------
# Fetching access token
# ----------------------------------------------------------------------------


def fetch_access_token() -> str | Exception:

    url = f"https://login.microsoftonline.com/{options.tenant_id}/oauth2/v2.0/token"
    payload = {
        "client_id": options.client_id,
        "client_secret": options.client_secret,
        "scope": options.scopes,
        "grant_type": "client_credentials",
    }

    response = requests.post(url, data=payload)

    if response.status_code != HTTPStatus.OK:
        return exception_from_response(response)

    access_token = response.json().get("access_token")
    return access_token


logger.info("fetching access token...")

access_token_result = fetch_access_token()

if access_token_result is Exception:
    print(f"fetching access token failed, error: {access_token_result}")
    exit()

access_token = access_token_result

logger.info(f"fetching access token done, access_token: {access_token}")

# %%

# ----------------------------------------------------------------------------
# Fetching companies
# ----------------------------------------------------------------------------


def exception_from_response(response: requests.Response) -> Exception:
    return Exception(f"code: {response.status_code}, data: {response.json()}")


def fetch_companies():
    url = f"{api_prefix}/companies"
    headers = {"Authorization": f"Bearer {access_token}"}

    response = requests.get(url, headers=headers)

    if response.status_code != HTTPStatus.OK:
        return exception_from_response(response)

    return response.json()["value"]


logger.info("fetching companies...")

companies_result = fetch_companies()
if companies_result is Exception:
    logger.error(f"fetching companies failed, error: {companies_result}")
    exit()

companies = companies_result

logger.info(f"fetching companies done, count: {len(companies)}")

# %%

# ----------------------------------------------------------------------------
# Fetching regions
# ----------------------------------------------------------------------------


def fetch_regions(companies):

    regions_final = []
    for company in companies:
        company_id = company["id"]
        logger.info(f"fetching regions for comapny {company_id}...")
        url = f"{api_prefix}/companies({company_id})/countriesRegions"
        headers = {"Authorization": f"Bearer {access_token}"}

        response = requests.get(url, headers=headers)

        if response.status_code != HTTPStatus.OK:
            return exception_from_response(response)

        regions = response.json()["value"]

        # adding the company id to regions
        for region in regions:
            region.pop("@odata.etag")
            region["company"] = company_id

        regions_final.extend(regions)

    return regions_final


logger.info("fetching regions...")

regions_result = fetch_regions(companies=companies)
if regions_result is Exception:
    logger.error(f"fetching regions failed, error: {regions_result}")
    exit()

regions = regions_result

logger.info(f"fetching regions done, count: {len(regions)}")

# %%

# ----------------------------------------------------------------------------
# Fetching sales invoices
# ----------------------------------------------------------------------------


def fetch_sales_invoices(companies: list) -> list | Exception:

    sales_invoices_final = []
    for company in companies:
        company_id = company["id"]

        url = f"{api_prefix}/companies({company_id})/salesInvoices"
        headers = {"Authorization": f"Bearer {access_token}"}

        response = requests.get(url, headers=headers)

        if response.status_code != HTTPStatus.OK:
            return exception_from_response(response)

        sales_invoices = response.json()["value"]
        sales_invoices_count = len(sales_invoices)
        for invoice in sales_invoices:
            invoice.pop("@odata.etag")
            invoice["company"] = company_id

        logger.info(
            f"fetching sales_invoices for company {company_id}, count: {sales_invoices_count}"
        )
        sales_invoices_final.extend(sales_invoices)

    return sales_invoices_final


logger.info("fetching sales_invoices...")

sales_invoices_result = fetch_sales_invoices(companies=companies)
if sales_invoices_result is Exception:
    logger.error(f"fetching sales_invoices failed, error: {sales_invoices_result}")
    exit()

sales_invoices = sales_invoices_result

logger.info(f"fetching sales_invoices done, count: {len(sales_invoices)}")


# %%

# ----------------------------------------------------------------------------
# Fetching items
# ----------------------------------------------------------------------------


def fetch_items(companies: list) -> list | Exception:

    items_final = []

    for company in companies:
        company_id = company["id"]
        logger.info(f"fetching items for company {company_id}...")

        url = f"{api_prefix}/companies({company_id})/items"
        headers = {"Authorization": f"Bearer {access_token}"}

        response = requests.get(url, headers=headers)

        if response.status_code != HTTPStatus.OK:
            return exception_from_response(response)

        items: list = response.json()["value"]
        items_count = len(items)

        for item in items:
            item.pop("@odata.etag")
            item["company"] = company_id

        items_final.extend(items)

        logger.info(f"fetching items for company done, count: {items_count}.")

    return items_final


logger.info("fetching items...")

items_result = fetch_items(companies)
if items_result is Exception:
    logger.error(f"fetching items failed, error: {items_result}")
    exit()

items = items_result
items_count = len(items)

logger.info(f"fetching items done, count: {items_count}.")

# %%

# ----------------------------------------------------------------------------
# Fetching customers
# ----------------------------------------------------------------------------


def fetch_customers(companies: list) -> list | Exception:

    customers_final = []

    for company in companies:
        company_id = company["id"]
        logger.info(f"fetching customers for company {company_id}...")

        url = f"{api_prefix}/companies({company_id})/customers"
        headers = {"Authorization": f"Bearer {access_token}"}

        response = requests.get(url, headers=headers)

        if response.status_code != HTTPStatus.OK:
            return exception_from_response(response)

        customers: list = response.json()["value"]
        customers_count = len(customers)

        for customer in customers:
            customer.pop("@odata.etag")
            customer["company"] = company_id

        customers_final.extend(customers)

        logger.info(
            f"fetching customers for company {company_id} done, count: {customers_count}."
        )

    return customers_final


logger.info("fetching customers...")

customers_result = fetch_customers(companies)
if customers_result is Exception:
    logger.error(f"fetching customers failed, error: {customers_result}")
    exit()

customers = customers_result
customers_count = len(customers)

logger.info(f"fetching customers done, count: {items_count}.")

# %%

# ----------------------------------------------------------------------------
# Fetching contacts
# ----------------------------------------------------------------------------


def fetch_company_resources(resource_name: str, companies: list) -> None:

    logger.info(f"fetching {resource_name}...")
    resources_final = []

    for company in companies:
        company_id = company["id"]
        logger.info(f"fetching {resource_name} for company {company_id}...")

        url = f"{api_prefix}/companies({company_id})/{resource_name}"
        headers = {"Authorization": f"Bearer {access_token}"}

        response = requests.get(url, headers=headers)

        if response.status_code != HTTPStatus.OK:
            ex = exception_from_response(response)
            logger.error(f"fetching resources failed, error: {ex}")
            return None

        resources: list = response.json()["value"]
        resources_count = len(resources)

        for customer in resources:
            customer.pop("@odata.etag")
            customer["company"] = company_id

        resources_final.extend(resources)

        logger.info(
            f"fetching {resource_name} for company {company_id} done, count: {resources_count}."
        )

    resources_count = len(resources_final)
    logger.info(f"fetching {resource_name} done, count: {resources_count}.")

    return resources_final


contacts = fetch_company_resources("contacts", companies=companies)

# %%

# ----------------------------------------------------------------------------
# Writing to csv files
# ----------------------------------------------------------------------------

import csv


def write_to_csv(data: list, file_name: str) -> None:
    os.makedirs(options.output_dir, exist_ok=True)
    file_path = f"{options.output_dir}/{file_name}.csv"

    with open(file_path, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)


write_to_csv(companies, "companies")
write_to_csv(regions, "regions")
write_to_csv(items, "items")
write_to_csv(customers, "customers")
write_to_csv(sales_invoices, "sales_invoices")
write_to_csv(contacts, "contacts")
