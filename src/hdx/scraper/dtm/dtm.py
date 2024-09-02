#!/usr/bin/python
"""DTM scraper"""

import logging
from typing import List

from hdx.api.configuration import Configuration
from hdx.utilities.retriever import Retrieve

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError

logger = logging.getLogger(__name__)


class Dtm:
    def __init__(
        self, configuration: Configuration, retriever: Retrieve, temp_dir: str
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir

    def get_countries(self) -> List[str]:
        """Get list of ISO3s to query the API with"""
        data = self._retriever.download_json(
            url=self._configuration["COUNTRIES_URL"]
        )["result"]
        countries = [country_dict["admin0Pcode"] for country_dict in data]
        return countries

    def generate_dataset(
        self, countries: List[str], qc_indicators: dict
    ) -> [Dataset, tuple]:
        dataset = Dataset()
        dataset.add_tags(self._configuration["tags"])
        # Generate resources, one per admin level
        for admin_level in self._configuration["admin_levels"]:
            global_data_for_single_admin_level = []
            for iso3 in countries:
                url = self._configuration["IDPS_URL"].format(
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

            _, results = dataset.generate_resource_from_iterable(
                headers=list(global_data_for_single_admin_level[0].keys()),
                iterable=global_data_for_single_admin_level,
                hxltags=self._configuration["hxl_tags"],
                folder=self._temp_dir,
                filename=self._configuration["resource_filename"].format(
                    admin_level=admin_level
                ),
                # This takes the resource name and description from the config
                # file, and fills in the corresponding admin level
                resourcedata={
                    key: value.format(admin_level=admin_level)
                    for key, value in self._configuration[
                        "resource_data"
                    ].items()
                },
                datecol="reportingDate",
                # QuickCharts jsut for admin 0
                quickcharts=self._get_quickcharts_from_indicators(
                    qc_indicators=qc_indicators
                )
                if admin_level == 0
                else None,
            )
            if admin_level == 0:
                bites_disabled = results["bites_disabled"]
        return dataset, bites_disabled

    def _get_quickcharts_from_indicators(self, qc_indicators: dict) -> dict:
        quickcharts = self._configuration["quickcharts"]
        quickcharts["values"] = [x["code"] for x in qc_indicators]
        return quickcharts
