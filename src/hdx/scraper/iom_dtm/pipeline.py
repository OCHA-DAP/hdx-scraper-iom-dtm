#!/usr/bin/python
"""DTM scraper"""

import logging
from collections import defaultdict
from typing import List, Tuple

import numpy as np
import pandas as pd
from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.location.adminlevel import AdminLevel
from hdx.location.country import Country
from hdx.scraper.framework.utilities.hapi_admins import complete_admins
from hdx.utilities.dateparse import (
    default_date,
    default_enddate,
    iso_string_from_datetime,
    parse_date,
)
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Pipeline:
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
        self._global_data = []

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
    ) -> Tuple[List, int]:
        highest_admin_level = 0
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
            highest_admin_level = admin_level
            result += data
        return result, highest_admin_level

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
        highest_admin_level = 0
        for iso3 in countries:
            data, admin_level = self.get_country_data(iso3, dataset, operation_status)
            if admin_level > highest_admin_level:
                highest_admin_level = admin_level
            countries_data += data

        if len(countries) > 1:
            self._global_data = countries_data

        dataset.generate_resource_from_iterable(
            headers=list(self._configuration["hxl_tags"].keys()),
            iterable=countries_data,
            hxltags=self._configuration["hxl_tags"],
            folder=self._temp_dir,
            filename=f"{name}-iom-dtm-from-api-admin-0-to-{highest_admin_level}.csv",
            resourcedata={
                "name": f"{title} IOM DTM data for admin levels 0-{highest_admin_level}",
                "description": f"{title} IOM displacement tracking matrix data at admin "
                f"levels 0-{highest_admin_level}, sourced from the DTM API",
                "p_coded": True,
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

        global_data = pd.DataFrame(self._global_data)
        global_data.drop(columns=["numberMales", "numberFemales"], inplace=True)

        # Check for duplicates in the data
        subset = global_data[
            [
                "admin2Pcode",
                "admin1Name",
                "admin2Name",
                "roundNumber",
                "displacementReason",
                "idpOriginAdmin1Name",
                "idpOriginAdmin1Pcode",
                "assessmentType",
                "operation",
                "reportingDate",
            ]
        ]
        subset.loc[subset["admin2Pcode"].isna(), "admin2Pcode"] = global_data.loc[
            subset["admin2Pcode"].isna(), "admin1Pcode"
        ]
        subset.loc[subset["admin2Pcode"].isna(), "admin2Pcode"] = global_data.loc[
            subset["admin2Pcode"].isna(), "admin0Pcode"
        ]
        duplicates = subset.duplicated(keep=False)
        global_data["error"] = None
        global_data.loc[duplicates, "error"] = "Duplicate row"

        if sum(duplicates) > 0:
            iso_duplicates = global_data.loc[duplicates, "admin0Pcode"]
            iso_duplicates = set(list(iso_duplicates))
            for iso in iso_duplicates:
                self._error_handler.add_message(
                    "DTM",
                    non_hapi_dataset_name,
                    f"Duplicates found in {iso}",
                    resource_name=resource_name,
                    err_to_hdx=True,
                )

        global_data.replace(np.nan, "***NONE***", inplace=True)

        groupby = [
            "admin0Pcode",
            "admin1Pcode",
            "admin2Pcode",
            "admin0Name",
            "admin1Name",
            "admin2Name",
            "adminLevel",
            "assessmentType",
            "operation",
            "reportingDate",
            "roundNumber",
            "operationStatus",
        ]
        result = (
            global_data.groupby(groupby)
            .agg(
                {
                    "numPresentIdpInd": "sum",
                    "displacementReason": "first",
                    "idpOriginAdmin1Name": "first",
                    "idpOriginAdmin1Pcode": "first",
                    "error": "first",
                }
            )
            .reset_index()
        )
        result.replace("***NONE***", None, inplace=True)
        result.drop(
            columns=[
                "displacementReason",
                "idpOriginAdmin1Name",
                "idpOriginAdmin1Pcode",
            ],
            inplace=True,
        )

        result.rename(
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
        # Set missing admin 2 names
        result.loc[
            (result["admin_level"] == 2) & (result["provider_admin2_name"].isna()),
            "provider_admin2_name",
        ] = " "

        min_date = default_enddate
        max_date = default_date
        # Loop through rows to check pcodes, get HRP/GHO status and dates
        result = result.to_dict("records")

        def get_rows():
            nonlocal min_date, max_date

            for row in result:
                # Get HRP and GHO status
                country_iso = row["location_code"]
                newrow = {"location_code": country_iso}
                hrp = Country.get_hrp_status_from_iso3(country_iso)
                gho = Country.get_gho_status_from_iso3(country_iso)
                hrp = "Y" if hrp else "N"
                gho = "Y" if gho else "N"
                newrow["has_hrp"] = hrp
                newrow["in_gho"] = gho
                newrow["provider_admin1_name"] = row["provider_admin1_name"]
                newrow["provider_admin2_name"] = row["provider_admin2_name"]

                # Check p-code
                admin_level = row["admin_level"]
                if admin_level == 0:
                    newrow["admin1_code"] = ""
                    newrow["admin1_name"] = ""
                    newrow["admin2_code"] = ""
                    newrow["admin2_name"] = ""
                    adm_level = admin_level
                    warnings = ""
                else:
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

                    newrow["admin1_code"] = adm_codes[0]
                    newrow["admin1_name"] = adm_names[0]
                    newrow["admin2_code"] = adm_codes[1]
                    newrow["admin2_name"] = adm_names[1]
                newrow["admin_level"] = adm_level

                newrow["operation"] = row["operation"]
                newrow["assessment_type"] = row["assessment_type"]
                newrow["population"] = row["population"]
                newrow["reporting_round"] = row["reporting_round"]

                # Parse date
                date = parse_date(row["reportingDate"])
                newrow["reference_period_start"] = iso_string_from_datetime(date)
                newrow["reference_period_end"] = iso_string_from_datetime(date)
                if date < min_date:
                    min_date = date
                if date > max_date:
                    max_date = date

                # Add dataset metadata
                newrow["dataset_hdx_id"] = dataset_id
                newrow["resource_hdx_id"] = resource_id

                newrow["warning"] = "|".join(warnings)
                newrow["error"] = row["error"]
                yield newrow

        # Generate dataset
        dataset = Dataset(
            {
                "name": "hdx-hapi-idps",
                "title": "HDX HAPI - Affected People: Internally-Displaced Persons",
            }
        )
        dataset.add_other_location("world")
        dataset.add_tags(self._configuration["tags"])
        headers = self._configuration["hapi_headers"]
        dataset.generate_resource(
            folder=self._temp_dir,
            filename="hdx_hapi_idps_global.csv",
            rows=get_rows(),
            resourcedata=self._configuration["hapi_resource_data"],
            headers=headers,
            encoding="utf-8-sig",
        )
        dataset.set_time_period(min_date, max_date)
        return dataset
