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
# Utils
# ----------------------------------------------------------------------------


def exception_from_response(response: requests.Response) -> Exception:
    return Exception(f"code: {response.status_code}, data: {response.json()}")


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

assert access_token, "failed to get access_token"

# %%

# ----------------------------------------------------------------------------
# Fetching companies
# ----------------------------------------------------------------------------


def fetch_companies():
    logger.info("fetching companies...")

    url = f"{api_prefix}/companies"
    headers = {"Authorization": f"Bearer {access_token}"}

    response = requests.get(url, headers=headers)

    if response.status_code != HTTPStatus.OK:
        ex = exception_from_response(response)
        logger.error(f"fetching companies failed, error: {ex}")
        return None

    companies = response.json()["value"]
    companies_count = len(companies)

    logger.info(f"fetching companies done, count: {companies_count}")
    return companies


companies = fetch_companies()
assert companies, "failed to fetch companies"


# %%

# ----------------------------------------------------------------------------
# Fetching company based resources
# ----------------------------------------------------------------------------


def fetch_company_resources(
    resource_name: str, companies: list, resource_id: str = None
) -> list | None:

    if resource_id is None:
        resource_id = resource_name

    logger.info(f"fetching {resource_name}...")
    resources_final = []

    for company in companies:
        company_id = company["id"]
        logger.info(f"fetching {resource_name} for company {company_id}...")

        url = f"{api_prefix}/companies({company_id})/{resource_id}"
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


customers = fetch_company_resources("customers", companies=companies)
regions = fetch_company_resources(
    "regions", resource_id="countriesRegions", companies=companies
)
items = fetch_company_resources("items", companies=companies)
sales_invoices = fetch_company_resources(
    "sales_invoices", resource_id="salesInvoices", companies=companies
)
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

assert companies, "failed to fetch companies"
assert regions, "failed to fetch regions"
assert items, "failed to fetch items"
assert customers, "failed to fetch customers"
assert sales_invoices, "failed to fetch sales_invoices"
assert contacts, "failed to fetch contacts"

# %%
