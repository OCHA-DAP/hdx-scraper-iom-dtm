#!/usr/bin/python
"""DTM scraper"""

import logging
from collections import defaultdict
from typing import List

import numpy as np
import pandas as pd
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Dtm:
    def __init__(
        self, configuration: Configuration, retriever: Retrieve, temp_dir: str
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir
        self.global_data = []

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

    def get_country_data(
        self, iso3: str, dataset: Dataset, operation_status: defaultdict
    ):
        result = []
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
            result += data
        return result

    def generate_dataset(
        self, countries: List[str], operation_status: defaultdict
    ) -> Dataset:
        name = "global" if len(countries) > 1 else countries[0].lower()
        title = (
            "Global"
            if len(countries) > 1
            else Country.get_country_name_from_iso3(countries[0])
        )
        dataset = Dataset(
            {
                "name": f"{name}-iom-dtm-from-api",
                "title": f"{title} IOM Displacement Tracking Matrix (DTM) from"
                + " API",
            }
        )
        dataset.add_tags(self._configuration["tags"])
        # Generate a single resource for all admin levels
        countries_data = []
        for iso3 in countries:
            data = self.get_country_data(iso3, dataset, operation_status)
            countries_data += data

        if len(countries) > 1:
            self.global_data = countries_data

        dataset.generate_resource_from_iterable(
            headers=list(self._configuration["hxl_tags"].keys()),
            iterable=countries_data,
            hxltags=self._configuration["hxl_tags"],
            folder=self._temp_dir,
            filename=f"{name}-iom-dtm-from-api-admin-0-to-2.csv",
            resourcedata={
                "name": f"{title} IOM DTM data for admin levels 0-2",
                "description": f"{title} IOM displacement tracking"
                + " matrix data at admin levels 0, 1, and 2, sourced from"
                + " the DTM API",
            },
            datecol="reportingDate",
        )

        if len(countries) > 1:
            # Filter data for quickcharts
            df = (
                pd.DataFrame(countries_data)
                # Only take admin 0, and required countries
                .loc[
                    lambda x: x["admin1Pcode"].isna()
                    & x["admin0Pcode"].isin(
                        self._configuration["qc_countries"]
                        if len(countries) > 1
                        else countries
                    )
                ]
                # Then drop the extra columns
                .drop(
                    columns=[
                        "admin1Name",
                        "admin1Pcode",
                        "admin2Name",
                        "admin2Pcode",
                    ],
                    errors="ignore",
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

        return dataset

    def generate_hapi_dataset(self, non_hapi_dataset_name: str) -> Dataset:
        non_hapi_dataset = Dataset.read_from_hdx(non_hapi_dataset_name)
        dataset_id = non_hapi_dataset["id"]
        resource_id = non_hapi_dataset.get_resource(0)["id"]

        global_data = pd.DataFrame(self.global_data)
        global_data = global_data.replace(np.nan, None)
        global_data.rename(
            columns={
                "admin0Pcode": "location_code",
                "admin1Name": "admin1_name",
                "admin2Name": "admin2_name",
                "admin1Pcode": "admin1_code",
                "admin2Pcode": "admin2_code",
                "numPresentIdpInd": "population",
                "roundNumber": "reporting_round",
                "assessmentType": "assessment_type",
            },
            inplace=True,
        )

        # Add admin_level column
        global_data["admin_level"] = 2
        global_data.loc[global_data["admin2_code"].isna(), "admin_level"] = 1
        global_data.loc[global_data["admin1_code"].isna(), "admin_level"] = 0

        # Add dataset metadata
        global_data["dataset_id"] = dataset_id
        global_data["resource_id"] = resource_id

        # Check for duplicates in the data
        subset = global_data[
            [
                "admin2_code",
                "admin1_name",
                "admin2_name",
                "reporting_round",
                "assessment_type",
                "operation",
            ]
        ]
        subset.loc[subset["admin2_code"].isna(), "admin2_code"] = (
            global_data.loc[subset["admin2_code"].isna(), "admin1_code"]
        )
        subset.loc[subset["admin2_code"].isna(), "admin2_code"] = (
            global_data.loc[subset["admin2_code"].isna(), "location_code"]
        )
        duplicates = subset.duplicated(keep=False)

        # Loop through rows to get errors, HRP/GHO status, reference dates
        errors = []
        hrps = []
        ghos = []
        dates = []
        for i in range(len(global_data)):
            error = None
            duplicate = duplicates[i]
            if duplicate is np.True_:
                error = "Duplicate row"
            errors.append(error)

            # Get HRP and GHO status
            hrp = Country.get_hrp_status_from_iso3(
                global_data["location_code"][i]
            )
            gho = Country.get_gho_status_from_iso3(
                global_data["location_code"][i]
            )
            hrps.append(hrp)
            ghos.append(gho)

            # Parse date
            date = parse_date(global_data["reportingDate"][i])
            dates.append(date)

        global_data["error"] = errors
        global_data["has_hrp"] = hrps
        global_data["in_gho"] = ghos
        global_data["reference_period_start"] = dates
        global_data["reference_period_end"] = dates

        # Generate dataset
        dataset = Dataset(
            {
                "name": "hdx-hapi-idps-test",
                "title": "HDX HAPI - Affected People: "
                + "Internally-Displaced Persons",
            }
        )
        dataset.add_other_location("world")
        dataset.add_tags(self._configuration["tags"])
        hxl_tags = self._configuration["hapi_hxl_tags"]
        dataset.generate_resource_from_iterable(
            headers=list(hxl_tags.keys()),
            iterable=global_data.to_dict("records"),
            hxltags=hxl_tags,
            folder=self._temp_dir,
            filename="hdx_hapi_idps_global.csv",
            resourcedata=self._configuration["hapi_resource_data"],
            datecol="reportingDate",
        )

        return dataset
