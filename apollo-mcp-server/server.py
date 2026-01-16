import json
import logging
import os
from pathlib import Path
from typing import Optional

import httpx
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP


SERVER_DIR = Path(__file__).parent
load_dotenv(SERVER_DIR / ".env")


LOG_FILE = SERVER_DIR / "apollo.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
)
logger = logging.getLogger(__name__)


mcp = FastMCP("apollo")

APOLLO_API_BASE = "https://api.apollo.io/api/v1"


def get_api_key() -> str:
    api_key = os.environ.get("APOLLO_API_KEY")
    if not api_key:
        raise ValueError("APOLLO_API_KEY not found. Add it to .env file.")
    return api_key


@mcp.tool()
async def search_people(
    person_fields: list[str] = [
        "first_name",
        "last_name_obfuscated",
        "title",
        "organization.name",
    ],
    person_titles: Optional[list[str]] = None,
    include_similar_titles: Optional[bool] = None,
    q_keywords: Optional[str] = None,
    person_locations: Optional[list[str]] = None,
    person_seniorities: Optional[list[str]] = None,
    organization_locations: Optional[list[str]] = None,
    q_organization_domains_list: Optional[list[str]] = None,
    contact_email_status: Optional[list[str]] = None,
    organization_ids: Optional[list[str]] = None,
    organization_num_employees_ranges: Optional[list[str]] = None,
    revenue_range_min: Optional[int] = None,
    revenue_range_max: Optional[int] = None,
    currently_using_all_of_technology_uids: Optional[list[str]] = None,
    currently_using_any_of_technology_uids: Optional[list[str]] = None,
    currently_not_using_any_of_technology_uids: Optional[list[str]] = None,
    q_organization_job_titles: Optional[list[str]] = None,
    organization_job_locations: Optional[list[str]] = None,
    organization_num_jobs_range_min: Optional[int] = None,
    organization_num_jobs_range_max: Optional[int] = None,
    organization_job_posted_at_range_min: Optional[str] = None,
    organization_job_posted_at_range_max: Optional[str] = None,
    page: int = 1,
    per_page: int = 25,
) -> dict:
    """
    Args:
    person_fields: Fields to include in each person object.
        Defaults to: ["first_name", "last_name_obfuscated", "title", "organization.name"]

        Top-level: "id", "first_name", "last_name_obfuscated", "title", "last_refreshed_at", "has_email", "has_city", "has_state", "has_country", "has_direct_phone"

        Organization (use "organization.field" syntax): "name", "has_industry", "has_phone", "has_city", "has_state", "has_country", "has_zip_code", "has_revenue", "has_employee_count"
    person_titles: eg, ["sales director", "ceo"]
    include_similar_titles: (default: true)
    q_keywords: Keywords to filter results
    person_locations: (eg, ["california", "ireland"])
    person_seniorities: (eg, ["director", "vp", "c_suite"])
    organization_locations: (eg, ["texas", "tokyo"])
    q_organization_domains_list: (eg, ["apollo.io", "microsoft.com"])
    contact_email_status: (eg, ["verified", "likely to engage"])
    organization_ids: Apollo organization IDs
    organization_num_employees_ranges: Ranges (eg, ["1,10", "250,500"])
    revenue_range_min: Minimum revenue (no currency symbols or commas)
    revenue_range_max: Maximum revenue (no currency symbols or commas)
    currently_using_all_of_technology_uids: Technologies employer uses (all)
    currently_using_any_of_technology_uids: Technologies employer uses (any)
    currently_not_using_any_of_technology_uids: Technologies employer doesn't use
    q_organization_job_titles: Job titles in active postings
    organization_job_locations: Locations of active job postings
    organization_num_jobs_range_min: Minimum number of active job postings
    organization_num_jobs_range_max: Maximum number of active job postings
    organization_job_posted_at_range_min: Earliest job posting date (YYYY-MM-DD)
    organization_job_posted_at_range_max: Latest job posting date (YYYY-MM-DD)
    page: (1-500)
    per_page: (1-100)
    """
    api_key = get_api_key()

    # Build payload with only non-None values
    payload = {
        "page": page,
        "per_page": min(per_page, 100),
    }

    if person_titles is not None:
        payload["person_titles"] = person_titles
    if include_similar_titles is not None:
        payload["include_similar_titles"] = include_similar_titles
    if q_keywords is not None:
        payload["q_keywords"] = q_keywords
    if person_locations is not None:
        payload["person_locations"] = person_locations
    if person_seniorities is not None:
        payload["person_seniorities"] = person_seniorities
    if organization_locations is not None:
        payload["organization_locations"] = organization_locations
    if q_organization_domains_list is not None:
        payload["q_organization_domains_list"] = q_organization_domains_list
    if contact_email_status is not None:
        payload["contact_email_status"] = contact_email_status
    if organization_ids is not None:
        payload["organization_ids"] = organization_ids
    if organization_num_employees_ranges is not None:
        payload["organization_num_employees_ranges"] = organization_num_employees_ranges
    if revenue_range_min is not None or revenue_range_max is not None:
        payload["revenue_range"] = {}
        if revenue_range_min is not None:
            payload["revenue_range"]["min"] = revenue_range_min
        if revenue_range_max is not None:
            payload["revenue_range"]["max"] = revenue_range_max
    if currently_using_all_of_technology_uids is not None:
        payload["currently_using_all_of_technology_uids"] = (
            currently_using_all_of_technology_uids
        )
    if currently_using_any_of_technology_uids is not None:
        payload["currently_using_any_of_technology_uids"] = (
            currently_using_any_of_technology_uids
        )
    if currently_not_using_any_of_technology_uids is not None:
        payload["currently_not_using_any_of_technology_uids"] = (
            currently_not_using_any_of_technology_uids
        )
    if q_organization_job_titles is not None:
        payload["q_organization_job_titles"] = q_organization_job_titles
    if organization_job_locations is not None:
        payload["organization_job_locations"] = organization_job_locations
    if (
        organization_num_jobs_range_min is not None
        or organization_num_jobs_range_max is not None
    ):
        payload["organization_num_jobs_range"] = {}
        if organization_num_jobs_range_min is not None:
            payload["organization_num_jobs_range"][
                "min"
            ] = organization_num_jobs_range_min
        if organization_num_jobs_range_max is not None:
            payload["organization_num_jobs_range"][
                "max"
            ] = organization_num_jobs_range_max
    if (
        organization_job_posted_at_range_min is not None
        or organization_job_posted_at_range_max is not None
    ):
        payload["organization_job_posted_at_range"] = {}
        if organization_job_posted_at_range_min is not None:
            payload["organization_job_posted_at_range"][
                "min"
            ] = organization_job_posted_at_range_min
        if organization_job_posted_at_range_max is not None:
            payload["organization_job_posted_at_range"][
                "max"
            ] = organization_job_posted_at_range_max

    async with httpx.AsyncClient() as client:
        logger.info(f"search_people request (payload): {json.dumps(payload)}")
        logger.info(f"search_people request (person_fields): {person_fields}")
        response = await client.post(
            f"{APOLLO_API_BASE}/mixed_people/api_search",
            headers={"Content-Type": "application/json", "X-Api-Key": api_key},
            json=payload,
            timeout=30.0,
        )
        response.raise_for_status()
        data = response.json()

        # Filter person fields to only return requested fields
        people = data.get("people", [])
        filtered_people = []

        for person in people:
            filtered_person = {}

            for field in person_fields:
                if "." in field:
                    # Handle nested fields (e.g., "organization.name")
                    parent, child = field.split(".", 1)
                    if parent in person and person[parent] is not None:
                        if parent not in filtered_person:
                            filtered_person[parent] = {}
                        if isinstance(person[parent], dict) and child in person[parent]:
                            filtered_person[parent][child] = person[parent][child]
                else:
                    # Handle top-level fields
                    if field in person:
                        filtered_person[field] = person[field]

            filtered_people.append(filtered_person)

        # Return only pagination and people
        response = {
            "total_entries": data.get("total_entries", {}),
            "people": filtered_people,
        }
        logger.info(f"search_people response: {json.dumps(response)}")
        return response


@mcp.tool()
async def enrich_person(
    person_fields: list[str] = [
        "first_name",
        "last_name",
        "title",
        "email",
        "organization.name",
    ],
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
    name: Optional[str] = None,
    email: Optional[str] = None,
    hashed_email: Optional[str] = None,
    organization_name: Optional[str] = None,
    domain: Optional[str] = None,
    id: Optional[str] = None,
    linkedin_url: Optional[str] = None,
    reveal_personal_emails: bool = False,
) -> dict:
    """
    Args:
    person_fields: Fields to include in person object.
        Defaults to: ["id", "first_name", "last_name", "name", "title", "email", "organization.name"]

        Available top-level: id, first_name, last_name, name, linkedin_url, title, email_status, photo_url, twitter_url, github_url, facebook_url, extrapolated_email_confidence, headline, email, organization_id, state, city, country, contact_id, time_zone, free_domain, is_likely_to_engage, revealed_for_current_team

        Available nested (use "organization.field", "contact.field" syntax): organization.id, organization.name, organization.website_url, organization.linkedin_url, organization.primary_domain, organization.industry, contact.id, contact.name, contact.email

        Note: employment_history, contact_emails, phone_numbers arrays are always included if present
    first_name: First name (typically used with last_name). Eg: tim
    last_name: Last name (typically used with first_name). Eg: zheng
    name: Full name (first and last separated by space). Eg: tim zheng
    email: Email address. Eg: example@email.com
    hashed_email: MD5 or SHA-256 hashed email. Eg: 8d935115b9ff4489f2d1f9249503cadf (MD5) or 97817c0c49994eb500ad0a5e7e2d8aed51977b26424d508f66e4e8887746a152 (SHA-256)
    organization_name: Employer name (current or previous). Eg: apollo
    domain: Employer domain (no www., @, etc). Eg: apollo.io, microsoft.com
    id: Apollo person ID. Eg: 587cf802f65125cad923a266
    linkedin_url: LinkedIn profile URL. Eg: http://www.linkedin.com/in/tim-zheng-677ba010
    reveal_personal_emails: Enrich with personal emails (consumes credits, respects GDPR). Default: false
    """
    api_key = get_api_key()

    # Build query params with only non-None values
    params = {}

    if first_name is not None:
        params["first_name"] = first_name
    if last_name is not None:
        params["last_name"] = last_name
    if name is not None:
        params["name"] = name
    if email is not None:
        params["email"] = email
    if hashed_email is not None:
        params["hashed_email"] = hashed_email
    if organization_name is not None:
        params["organization_name"] = organization_name
    if domain is not None:
        params["domain"] = domain
    if id is not None:
        params["id"] = id
    if linkedin_url is not None:
        params["linkedin_url"] = linkedin_url
    if reveal_personal_emails:
        params["reveal_personal_emails"] = "true"

    async with httpx.AsyncClient() as client:
        logger.info(f"enrich_person request (params): {json.dumps(params)}")
        logger.info(f"enrich_person request (person_fields): {person_fields}")
        response = await client.get(
            f"{APOLLO_API_BASE}/people/match",
            headers={"Content-Type": "application/json", "X-Api-Key": api_key},
            params=params,
            timeout=30.0,
        )
        response.raise_for_status()
        data = response.json()

        # Filter person fields to only return requested fields
        person = data.get("person", {})
        filtered_person = {}

        # Handle arrays that should always be included if present
        always_include_arrays = [
            "employment_history",
            "contact_emails",
            "phone_numbers",
        ]

        for field in person_fields:
            if "." in field:
                # Handle nested fields (e.g., "organization.name", "contact.email")
                parent, child = field.split(".", 1)
                if parent in person and person[parent] is not None:
                    if parent not in filtered_person:
                        filtered_person[parent] = {}
                    if isinstance(person[parent], dict) and child in person[parent]:
                        filtered_person[parent][child] = person[parent][child]
            else:
                # Handle top-level fields
                if field in person:
                    filtered_person[field] = person[field]

        # Always include array fields if present
        for array_field in always_include_arrays:
            if array_field in person:
                filtered_person[array_field] = person[array_field]

        # Also include contact and organization objects if referenced
        if any(f.startswith("contact.") for f in person_fields):
            if "contact" in person and "contact" not in filtered_person:
                filtered_person["contact"] = person["contact"]
        if any(f.startswith("organization.") for f in person_fields):
            if "organization" in person and "organization" not in filtered_person:
                filtered_person["organization"] = person["organization"]

        response = {"person": filtered_person}
        logger.info(f"enrich_person response: {json.dumps(response)}")
        return response


@mcp.tool()
async def search_companies(
    organization_fields: list[str] = [
        "name",
        "website_url",
    ],
    q_organization_domains_list: Optional[list[str]] = None,
    organization_num_employees_ranges: Optional[list[str]] = None,
    organization_locations: Optional[list[str]] = None,
    organization_not_locations: Optional[list[str]] = None,
    revenue_range_min: Optional[int] = None,
    revenue_range_max: Optional[int] = None,
    currently_using_any_of_technology_uids: Optional[list[str]] = None,
    q_organization_keyword_tags: Optional[list[str]] = None,
    q_organization_name: Optional[str] = None,
    organization_ids: Optional[list[str]] = None,
    latest_funding_amount_range_min: Optional[int] = None,
    latest_funding_amount_range_max: Optional[int] = None,
    total_funding_range_min: Optional[int] = None,
    total_funding_range_max: Optional[int] = None,
    latest_funding_date_range_min: Optional[str] = None,
    latest_funding_date_range_max: Optional[str] = None,
    q_organization_job_titles: Optional[list[str]] = None,
    organization_job_locations: Optional[list[str]] = None,
    organization_num_jobs_range_min: Optional[int] = None,
    organization_num_jobs_range_max: Optional[int] = None,
    organization_job_posted_at_range_min: Optional[str] = None,
    organization_job_posted_at_range_max: Optional[str] = None,
    page: int = 1,
    per_page: int = 25,
) -> dict:
    """
    Args:
    organization_fields: Fields to include in each organization object.
        Defaults to: ["name", "website_url"]

        Available: id, name, website_url, blog_url, angellist_url, linkedin_url, twitter_url, facebook_url, primary_phone, languages, alexa_ranking, phone, linkedin_uid, founded_year, publicly_traded_symbol, publicly_traded_exchange, logo_url, crunchbase_url, primary_domain, sanitized_phone, owned_by_organization_id, intent_strength, show_intent, has_intent_signal_account, intent_signal_account, model_ids
    q_organization_domains_list: (eg, ["apollo.io", "microsoft.com"])
    organization_num_employees_ranges: (eg, ["1,10", "250,500"])
    organization_locations: HQ locations (eg, ["texas", "tokyo"])
    organization_not_locations: Exclude locations (eg, ["minnesota", "ireland"])
    revenue_range_min: Min revenue (no symbols/commas)
    revenue_range_max: Max revenue (no symbols/commas)
    currently_using_any_of_technology_uids: Technologies used
    q_organization_keyword_tags: Keywords (eg, ["mining", "consulting"])
    q_organization_name: Company name (partial matches accepted)
    organization_ids: Apollo organization IDs
    latest_funding_amount_range_min: Min latest funding (no symbols/commas)
    latest_funding_amount_range_max: Max latest funding (no symbols/commas)
    total_funding_range_min: Min total funding (no symbols/commas)
    total_funding_range_max: Max total funding (no symbols/commas)
    latest_funding_date_range_min: Earliest latest funding date (YYYY-MM-DD)
    latest_funding_date_range_max: Latest funding date (YYYY-MM-DD)
    q_organization_job_titles: Job titles in postings
    organization_job_locations: Job posting locations
    organization_num_jobs_range_min: Min job postings
    organization_num_jobs_range_max: Max job postings
    organization_job_posted_at_range_min: Earliest job date (YYYY-MM-DD)
    organization_job_posted_at_range_max: Latest job date (YYYY-MM-DD)
    page: (1-500)
    per_page: (1-100)
    """
    api_key = get_api_key()

    # Build payload with only non-None values
    payload = {
        "page": page,
        "per_page": min(per_page, 100),
    }

    if q_organization_domains_list is not None:
        payload["q_organization_domains_list"] = q_organization_domains_list
    if organization_num_employees_ranges is not None:
        payload["organization_num_employees_ranges"] = organization_num_employees_ranges
    if organization_locations is not None:
        payload["organization_locations"] = organization_locations
    if organization_not_locations is not None:
        payload["organization_not_locations"] = organization_not_locations
    if revenue_range_min is not None or revenue_range_max is not None:
        payload["revenue_range"] = {}
        if revenue_range_min is not None:
            payload["revenue_range"]["min"] = revenue_range_min
        if revenue_range_max is not None:
            payload["revenue_range"]["max"] = revenue_range_max
    if currently_using_any_of_technology_uids is not None:
        payload["currently_using_any_of_technology_uids"] = (
            currently_using_any_of_technology_uids
        )
    if q_organization_keyword_tags is not None:
        payload["q_organization_keyword_tags"] = q_organization_keyword_tags
    if q_organization_name is not None:
        payload["q_organization_name"] = q_organization_name
    if organization_ids is not None:
        payload["organization_ids"] = organization_ids
    if (
        latest_funding_amount_range_min is not None
        or latest_funding_amount_range_max is not None
    ):
        payload["latest_funding_amount_range"] = {}
        if latest_funding_amount_range_min is not None:
            payload["latest_funding_amount_range"][
                "min"
            ] = latest_funding_amount_range_min
        if latest_funding_amount_range_max is not None:
            payload["latest_funding_amount_range"][
                "max"
            ] = latest_funding_amount_range_max
    if total_funding_range_min is not None or total_funding_range_max is not None:
        payload["total_funding_range"] = {}
        if total_funding_range_min is not None:
            payload["total_funding_range"]["min"] = total_funding_range_min
        if total_funding_range_max is not None:
            payload["total_funding_range"]["max"] = total_funding_range_max
    if (
        latest_funding_date_range_min is not None
        or latest_funding_date_range_max is not None
    ):
        payload["latest_funding_date_range"] = {}
        if latest_funding_date_range_min is not None:
            payload["latest_funding_date_range"]["min"] = latest_funding_date_range_min
        if latest_funding_date_range_max is not None:
            payload["latest_funding_date_range"]["max"] = latest_funding_date_range_max
    if q_organization_job_titles is not None:
        payload["q_organization_job_titles"] = q_organization_job_titles
    if organization_job_locations is not None:
        payload["organization_job_locations"] = organization_job_locations
    if (
        organization_num_jobs_range_min is not None
        or organization_num_jobs_range_max is not None
    ):
        payload["organization_num_jobs_range"] = {}
        if organization_num_jobs_range_min is not None:
            payload["organization_num_jobs_range"][
                "min"
            ] = organization_num_jobs_range_min
        if organization_num_jobs_range_max is not None:
            payload["organization_num_jobs_range"][
                "max"
            ] = organization_num_jobs_range_max
    if (
        organization_job_posted_at_range_min is not None
        or organization_job_posted_at_range_max is not None
    ):
        payload["organization_job_posted_at_range"] = {}
        if organization_job_posted_at_range_min is not None:
            payload["organization_job_posted_at_range"][
                "min"
            ] = organization_job_posted_at_range_min
        if organization_job_posted_at_range_max is not None:
            payload["organization_job_posted_at_range"][
                "max"
            ] = organization_job_posted_at_range_max

    async with httpx.AsyncClient() as client:
        logger.info(f"search_companies request (payload): {json.dumps(payload)}")
        logger.info(
            f"search_companies request (organization_fields): {organization_fields}"
        )
        response = await client.post(
            f"{APOLLO_API_BASE}/mixed_companies/search",
            headers={"Content-Type": "application/json", "X-Api-Key": api_key},
            json=payload,
            timeout=30.0,
        )
        response.raise_for_status()
        data = response.json()

        # Filter organization fields to only return requested fields
        organizations = data.get("organizations", [])
        filtered_organizations = []

        for org in organizations:
            filtered_org = {}

            for field in organization_fields:
                if field in org:
                    filtered_org[field] = org[field]

            filtered_organizations.append(filtered_org)

        # Return only pagination and organizations
        response = {
            "pagination": data.get("pagination", {}),
            "organizations": filtered_organizations,
        }
        logger.info(f"search_companies response: {json.dumps(response)}")
        return response


if __name__ == "__main__":
    mcp.run()
