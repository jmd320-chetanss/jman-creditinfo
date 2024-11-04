# %%

import requests
from msal import ConfidentialClientApplication
import dotenv
import os
import logging

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


# setup logging
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

# ----------------------------------------------------------------------------
# log options if asked
# ----------------------------------------------------------------------------

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
# MSAL Confidential Client Application
# ----------------------------------------------------------------------------


def get_token():
    logger.info("creating app...")

    app = ConfidentialClientApplication(
        options.client_id,
        authority=f"https://login.microsoftonline.com/{options.tenant_id}",
        client_credential=options.client_secret,
    )

    logger.info("creating app done.")

    # Fetch the access token
    logger.info("fetching access token...")

    token_response = app.acquire_token_for_client(scopes=options.scopes)
    if "access_token" not in token_response:
        raise Exception("authentication failed")

    access_token = token_response["access_token"]

    logger.info(f"fetching access token done, access_token: {access_token}")

    return access_token


access_token = get_token()

# %%

# trying to fetch dummy data
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
