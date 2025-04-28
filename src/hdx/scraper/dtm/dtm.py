#!/usr/bin/python
"""DTM scraper"""

import logging
from collections import defaultdict
from typing import List

import numpy as np
import pandas as pd
from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.location.adminlevel import AdminLevel
from hdx.location.country import Country
from hdx.scraper.framework.utilities.hapi_admins import complete_admins
from hdx.utilities.dateparse import iso_string_from_datetime, parse_date
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Dtm:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
        temp_dir: str,
        error_handler: HDXErrorHandler,
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir
        self._admins = []
        self._error_handler = error_handler
        self.global_data = []

    def get_countries(self) -> List[str]:
        """Get list of ISO3s to query the API with"""
        data = self._retriever.download_json(url=self._configuration["COUNTRIES_URL"])[
            "result"
        ]
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
            # For each row in the data add the operation status and admin level
            for row in data:
                try:
                    row["operationStatus"] = operation_status[iso3][row["operation"]]
                except KeyError:
                    logger.warning(
                        f"Operation status {iso3}:{row['operation_status']} missing"
                    )
                row["adminLevel"] = admin_level
            # Data is empty if country is not present
            if not data:
                logger.warning(
                    f"Country {iso3} has no data for admin level {admin_level}"
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
                "title": f"{title} IOM Displacement Tracking Matrix (DTM) from API",
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
                "description": f"{title} IOM displacement tracking matrix data at admin "
                f"levels 0, 1, and 2, sourced from the DTM API",
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
                        "adminLevel",
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

    def get_pcodes(self) -> None:
        for admin_level in [1, 2]:
            admin = AdminLevel(admin_level=admin_level, retriever=self._retriever)
            dataset = admin.get_libhxl_dataset(retriever=self._retriever)
            admin.setup_from_libhxl_dataset(dataset)
            admin.load_pcode_formats()
            self._admins.append(admin)

    def generate_hapi_dataset(self, non_hapi_dataset_name: str) -> Dataset:
        # Set up admin levels and p-codes
        self.get_pcodes()

        non_hapi_dataset = Dataset.read_from_hdx(non_hapi_dataset_name)
        dataset_id = non_hapi_dataset["id"]
        resource = non_hapi_dataset.get_resource(0)
        resource_id = resource["id"]
        resource_name = resource["name"]

        global_data = pd.DataFrame(self.global_data)
        global_data.replace(np.nan, None, inplace=True)
        global_data.rename(
            columns={
                "admin0Pcode": "location_code",
                "admin1Name": "provider_admin1_name",
                "admin2Name": "provider_admin2_name",
                "numPresentIdpInd": "population",
                "roundNumber": "reporting_round",
                "assessmentType": "assessment_type",
                "adminLevel": "admin_level",
            },
            inplace=True,
        )

        # Add dataset metadata
        global_data["dataset_hdx_id"] = dataset_id
        global_data["resource_hdx_id"] = resource_id

        # Set missing admin 2 names
        global_data.loc[
            (global_data["admin_level"] == 2)
            & (global_data["provider_admin2_name"].isna()),
            "provider_admin2_name",
        ] = " "

        # Check for duplicates in the data
        subset = global_data[
            [
                "admin2Pcode",
                "provider_admin1_name",
                "provider_admin2_name",
                "reporting_round",
                "assessment_type",
                "operation",
                "reportingDate",
            ]
        ]
        subset.loc[subset["admin2Pcode"].isna(), "admin2Pcode"] = global_data.loc[
            subset["admin2Pcode"].isna(), "admin1Pcode"
        ]
        subset.loc[subset["admin2Pcode"].isna(), "admin2Pcode"] = global_data.loc[
            subset["admin2Pcode"].isna(), "location_code"
        ]
        duplicates = subset.duplicated(keep=False)
        global_data["error"] = None
        global_data.loc[duplicates, "error"] = "Duplicate row"
        if sum(duplicates) > 0:
            iso_duplicates = global_data.loc[duplicates, "location_code"]
            iso_duplicates = set(list(iso_duplicates))
            for iso in iso_duplicates:
                self._error_handler.add_message(
                    "DTM",
                    non_hapi_dataset_name,
                    f"Duplicates found in {iso}",
                    resource_name=resource_name,
                    err_to_hdx=True,
                )

        # Loop through rows to check pcodes, get HRP/GHO status and dates
        global_data = global_data.to_dict("records")
        for row in global_data:
            # Get HRP and GHO status
            country_iso = row["location_code"]
            hrp = Country.get_hrp_status_from_iso3(country_iso)
            gho = Country.get_gho_status_from_iso3(country_iso)
            hrp = "Y" if hrp else "N"
            gho = "Y" if gho else "N"
            row["has_hrp"] = hrp
            row["in_gho"] = gho

            # Parse date
            date = parse_date(row["reportingDate"])
            row["reference_period_start"] = iso_string_from_datetime(date)
            row["reference_period_end"] = iso_string_from_datetime(date)

            # Check p-code
            admin_level = row["admin_level"]
            if admin_level == 0:
                continue

            provider_adm_names = [
                row["provider_admin1_name"],
                row["provider_admin2_name"],
            ]
            adm_codes = [row["admin1Pcode"], row["admin2Pcode"]]
            adm_names = ["", ""]
            adm_level, warnings = complete_admins(
                self._admins,
                country_iso,
                provider_adm_names,
                adm_codes,
                adm_names,
            )
            for warning in warnings:
                self._error_handler.add_message(
                    "DTM",
                    non_hapi_dataset_name,
                    warning,
                    message_type="warning",
                )

            row["admin1_code"] = adm_codes[0]
            row["admin2_code"] = adm_codes[1]
            row["admin1_name"] = adm_names[0]
            row["admin2_name"] = adm_names[1]
            row["warning"] = "|".join(warnings)

        # Generate dataset
        dataset = Dataset(
            {
                "name": "hdx-hapi-idps",
                "title": "HDX HAPI - Affected People: Internally-Displaced Persons",
            }
        )
        dataset.add_other_location("world")
        dataset.add_tags(self._configuration["tags"])
        hxl_tags = self._configuration["hapi_hxl_tags"]
        dataset.generate_resource_from_iterable(
            headers=list(hxl_tags.keys()),
            iterable=global_data,
            hxltags=hxl_tags,
            folder=self._temp_dir,
            filename="hdx_hapi_idps_global.csv",
            resourcedata=self._configuration["hapi_resource_data"],
            datecol="reportingDate",
            encoding="utf-8-sig",
        )

        return dataset
