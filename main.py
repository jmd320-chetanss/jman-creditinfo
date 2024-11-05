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
    scopes = [os.getenv("SCOPE")]
    company_id = os.getenv("COMPANY_ID")
    log_level = os.getenv("LOG_LEVEL") or constants.defult_log_level
    log_format = os.getenv("LOG_FORMAT") or constants.default_log_format
    log_options = True


options = Options()

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
    logger.info(f"company_id: {options.company_id}")
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
        return Exception(f"code: {response.status_code}, data: {response.json()}")

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
# trying to fetch dummy data
# ----------------------------------------------------------------------------

url = f"https://api.businesscentral.dynamics.com/v2.0/{options.tenant_id}/sandbox/ODataV4/Company('{options.company_id}')/dynamics_account"
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    data = response.json()
    print(data)
else:
    print("Failed to fetch data:", response.status_code, response.text)
