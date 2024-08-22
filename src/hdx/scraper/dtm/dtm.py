#!/usr/bin/python
"""
WHO:
------------

Reads WHO API and creates datasets

"""

import logging
from typing import Optional

import requests

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.location.country import Country
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)

# TODO: move to config

_ADMIN_LEVELS = [0, 1, 2]
# _ADMIN_LEVELS = [0]

_HXL_TAGS = {
    "id": "#id+code",
    "operation": "#operation+name",
    "admin0Name": "#country+name",
    "admin0Pcode": "#country+code",
    "admin1Name": "#adm1+name",
    "admin1Pcode": "#adm1+code",
    "admin2Name": "#adm2+name",
    "admin2Pcode": "#adm2+code",
    "numPresentIdpInd": "affected+idps",
    "reportingDate": "#date+reported",
    "yearReportingDate": "#date+year+reported",
    "monthReportingDate": "#date+month+reported",
    "roundNumber": "#round+code",
    "assessmentType": "#assessment+type",
}


class Dtm:
    def __init__(
        self, configuration: Configuration, retriever: Retrieve, temp_dir: str
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir

    def generate_dataset(self) -> Optional[Dataset]:
        dataset = Dataset()
        dataset.add_tags(self._configuration["tags"])
        # Generate resources
        # Need all country ISO3s to loop through until DTM has endpoint
        all_iso3s = Country.countriesdata()["countries"].keys()
        # TODO delete
        # all_iso3s = list(all_iso3s)[:10]
        # One per admin level
        for admin_level in _ADMIN_LEVELS:
            global_data_for_single_admin_level = []
            for iso3 in all_iso3s:
                url = self._configuration["API_URL"].format(
                    admin_level=admin_level, iso3=iso3
                )
                # Use the requests library to quickly check if there is any
                # data (skip downloading files as it's too slow)
                response = requests.get(url).json()
                if not response["isSuccess"]:
                    continue
                # Add country to dataset
                try:
                    dataset.add_country_location(iso3)
                except HDXError:
                    logger.error(f"Couldn't find country {iso3}, skipping")
                    return
                # Only download files once we're sure there is data
                data = self._retriever.download_json(url=url)["result"]
                global_data_for_single_admin_level += data
            # TODO: move to config
            filename = f"global-iom-dtm-from-api-admin{admin_level}.csv"
            resourcedata = {
                "name": f"Global IOM DTM data admin {admin_level}",
                "description": f"Global IOM displacement "
                f"tracking matrix data, "
                f"at the admin {admin_level}, taken from their API",
            }
            dataset.generate_resource_from_iterable(
                headers=list(global_data_for_single_admin_level[0].keys()),
                iterable=global_data_for_single_admin_level,
                hxltags=_HXL_TAGS,
                folder=self._temp_dir,
                filename=filename,
                resourcedata=resourcedata,
                datecol="reportingDate",
            )
        return dataset
