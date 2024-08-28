#!/usr/bin/python
"""
WHO:
------------

Reads WHO API and creates datasets

"""

import logging
from typing import List

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.location.country import Country
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)

# TODO: move to config

_ADMIN_LEVELS = [0, 1, 2]

_HXL_TAGS = {
    "id": "#id+code",
    "operation": "#operation+name",
    "admin0Name": "#country+name",
    "admin0Pcode": "#country+code",
    "admin1Name": "#adm1+name",
    "admin1Pcode": "#adm1+code",
    "admin2Name": "#adm2+name",
    "admin2Pcode": "#adm2+code",
    "numPresentIdpInd": "#affected+idps",
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

    def get_countries(self) -> List[str]:
        """Get list of ISO3s to query the API with"""
        # TODO: switch to using endpoint once it's available
        countries = Country.countriesdata()["countries"].keys()
        # TODO delete
        # countries = list(countries)[:50]
        return countries

    def generate_dataset(
        self, countries: List[str], qc_indicators: dict
    ) -> [Dataset, tuple]:
        dataset = Dataset()
        dataset.add_tags(self._configuration["tags"])
        # Generate resources, one per admin level
        for admin_level in _ADMIN_LEVELS:
            global_data_for_single_admin_level = []
            for iso3 in countries:
                url = self._configuration["API_URL"].format(
                    admin_level=admin_level, iso3=iso3
                )
                # Add country to dataset
                try:
                    dataset.add_country_location(iso3)
                except HDXError:
                    logger.error(f"Couldn't find country {iso3}, skipping")
                    continue
                # Only download files once we're sure there is data
                data = self._retriever.download_json(url=url)["result"]
                # Data is empty if country is not present
                if not data:
                    logger.warning(
                        f"Country {iso3} has no data "
                        f"for admin level {admin_level}"
                    )
                    continue
                global_data_for_single_admin_level += data
            # TODO: move to config?
            filename = f"global-iom-dtm-from-api-admin{admin_level}.csv"
            resourcedata = {
                "name": f"Global IOM DTM data admin {admin_level}",
                "description": f"Global IOM displacement "
                f"tracking matrix data "
                f"at the admin {admin_level} level, sourced from the DTM API",
            }
            if admin_level == 0:
                quickcharts = _get_quichcharts_from_indicators(
                    qc_indicators=qc_indicators
                )
            else:
                quickcharts = None
            _, results = dataset.generate_resource_from_iterable(
                headers=list(global_data_for_single_admin_level[0].keys()),
                iterable=global_data_for_single_admin_level,
                hxltags=_HXL_TAGS,
                folder=self._temp_dir,
                filename=filename,
                resourcedata=resourcedata,
                datecol="reportingDate",
                quickcharts=quickcharts,
            )
            if admin_level == 0:
                bites_disabled = results["bites_disabled"]
        return dataset, bites_disabled


def _get_quichcharts_from_indicators(qc_indicators: dict) -> dict:
    # TODO: move to config
    return {
        "hashtag": "#country+code",
        "values": [x["code"] for x in qc_indicators],
        "numeric_hashtag": "#affected+idps",
        "cutdown": 2,
        "cutdownhashtags": [
            "#country+code",
            "#date+reported",
            "#affected+idps",
        ],
        "date_format": "%Y-%m-%dT%H:%M:%S",
    }
