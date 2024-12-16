#!/usr/bin/python
"""DTM scraper"""

import logging
from collections import defaultdict
from typing import List

import pandas as pd
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.utilities.retriever import Retrieve

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

    def get_operation_status(self) -> defaultdict:
        data = self._retriever.download_json(
            url=self._configuration["OPERATION_STATUS_URL"]
        )["result"]
        operation_status = defaultdict(dict)
        for row in data:
            operation_status[row["admin0Pcode"]][row["operation"]] = row[
                "operationStatus"
            ]
        return operation_status

    def generate_dataset(
        self, countries: List[str], operation_status: defaultdict
    ) -> Dataset:
        dataset = Dataset()
        dataset.add_tags(self._configuration["tags"])
        # Generate a single resource for all admin levels
        global_data = []
        adm0_data = {}
        for iso3 in countries:
            adm0_data[iso3] = []
            for admin_level in self._configuration["admin_levels"]:
                url = self._configuration["IDPS_URL"].format(
                    admin_level=admin_level, iso3=iso3
                )
                # Add country to dataset
                try:
                    dataset.add_country_location(iso3)
                except HDXError:
                    logger.error(
                        f"Couldn't find country {iso3} for admin "
                        f"level {admin_level}, skipping"
                    )
                    continue
                # Only download files once we're sure there is data
                data = self._retriever.download_json(url=url)["result"]
                # For each row in the data add the operation status
                for row in data:
                    try:
                        row["operationStatus"] = operation_status[iso3][
                            row["operation"]
                        ]
                    except KeyError:
                        logger.warning(
                            f"Operation status {iso3}:"
                            f"{row['operation_status']} missing"
                        )
                # Data is empty if country is not present
                if not data:
                    logger.warning(
                        f"Country {iso3} has no data "
                        f"for admin level {admin_level}"
                    )
                    continue
                global_data += data
                adm0_data[iso3] += data

        dataset.generate_resource_from_iterable(
            headers=list(self._configuration["hxl_tags"].keys()),
            iterable=global_data,
            hxltags=self._configuration["hxl_tags"],
            folder=self._temp_dir,
            filename=self._configuration["resource_filename"],
            # Resource name and description from the config
            resourcedata=self._configuration["resource_data"],
            datecol="reportingDate",
        )

        # Filter data for quickcharts
        df = (
            pd.DataFrame(global_data)
            # Only take admin 0, and required countries
            .loc[
                lambda x: x["admin1Pcode"].isna()
                & x["admin0Pcode"].isin(self._configuration["qc_countries"])
            ]
            # Then drop the extra columns
            .drop(
                columns=[
                    "admin1Name",
                    "admin1Pcode",
                    "admin2Name",
                    "admin2Pcode",
                ]
            )
            # Take the latest numbers per country, year, and operation
            .loc[
                lambda x: x.groupby(
                    ["admin0Pcode", "operation", "yearReportingDate"]
                )["reportingDate"].idxmax()
            ]
        )

        # Generate quickchart resource
        dataset.generate_resource_from_iterable(
            headers=list(df.columns),
            iterable=df.to_dict("records"),
            hxltags=self._configuration["hxl_tags"],
            folder=self._temp_dir,
            filename=self._configuration["qc_resource_filename"],
            # Resource name and description from the config
            resourcedata=self._configuration["qc_resource_data"],
        )

        for iso3 in countries:
            admin0_name = adm0_data[iso3][0]["admin0Name"]
            dataset.generate_resource_from_iterable(
                headers=list(self._configuration["hxl_tags"].keys()),
                iterable=adm0_data[iso3],
                hxltags=self._configuration["hxl_tags"],
                folder=self._temp_dir,
                filename=f"{iso3}-iom-dtm-from-api-admin-0-to-2.csv",
                # Resource name and description from the config
                resourcedata={
                    "name": f"{admin0_name} IOM DTM data for admin levels 0-2",
                    "description": f"{admin0_name} IOM displacement tracking"
                    + "matrix data at admin levels 0, 1, and 2, sourced from"
                    + "the DTM API",
                },
                datecol="reportingDate",
            )

        return dataset
