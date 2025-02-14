#!/usr/bin/python
"""DTM scraper"""

import logging
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.location.adminlevel import AdminLevel
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date
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
                        f"Operation status {iso3}:" f"{row['operation_status']} missing"
                    )
                row["adminLevel"] = admin_level
            # Data is empty if country is not present
            if not data:
                logger.warning(
                    f"Country {iso3} has no data " f"for admin level {admin_level}"
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

    def get_admin_info(
        self,
        dataset_name: str,
        iso: str,
        admin_level: int,
        pcode: str,
        admin_name: str,
        parent_pcode: Optional[str] = None,
        parent_name: Optional[str] = None,
    ) -> Tuple[Dict[str, str], Optional[str]]:
        admin_info = {
            "admin2_code": None,
            "admin2_name": None,
            "admin1_code": None,
            "admin1_name": None,
        }
        warning = None

        if not pcode:
            self._error_handler.add_missing_value_message(
                "DTM",
                dataset_name,
                f"admin {admin_level} pcode",
                pcode,
            )
            warning = "Missing pcode"
            return admin_info, warning

        if pcode not in self._admins[admin_level - 1].pcodes:
            try:
                matched_pcode = self._admins[admin_level - 1].convert_admin_pcode_length(
                    iso, pcode, parent=parent_pcode
                )
            except IndexError:
                matched_pcode = None
            if matched_pcode:
                warning = f"Pcode unknown {pcode}->{matched_pcode}"
                pcode = matched_pcode
            else:
                self._error_handler.add_missing_value_message(
                    "DTM",
                    dataset_name,
                    f"admin {admin_level} pcode",
                    pcode,
                )
                warning = f"Pcode unknown {pcode}"
                if parent_name:
                    parent_pcode, _ = self._admins[admin_level - 2].get_pcode(
                        iso, parent_name
                    )
                matched_pcode, _ = self._admins[admin_level - 1].get_pcode(
                    iso, admin_name, parent=parent_pcode
                )
                if matched_pcode:
                    warning = f"Pcode unknown {pcode}->{matched_pcode}"
                    pcode = matched_pcode
                if not matched_pcode:
                    self._error_handler.add_missing_value_message(
                        "DTM",
                        dataset_name,
                        f"admin {admin_level} pcode",
                        pcode,
                    )
                    warning = f"Pcode unknown {pcode}"
                    return admin_info, warning

        admin_info[f"admin{admin_level}_code"] = pcode
        admin_info[f"admin{admin_level}_name"] = self._admins[
            admin_level - 1
        ].pcode_to_name.get(pcode)
        if admin_level == 2:
            admin1_code = self._admins[1].pcode_to_parent.get(pcode)
            admin_info["admin1_code"] = admin1_code
            admin_info["admin1_name"] = self._admins[0].pcode_to_name.get(admin1_code)
        return admin_info, warning

    def generate_hapi_dataset(self, non_hapi_dataset_name: str) -> Dataset:
        # Set up admin levels and p-codes
        self.get_pcodes()

        non_hapi_dataset = Dataset.read_from_hdx(non_hapi_dataset_name)
        dataset_id = non_hapi_dataset["id"]
        resource_id = non_hapi_dataset.get_resource(0)["id"]

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
        subset.loc[global_data["admin_level"] == 1, "admin2Pcode"] = global_data.loc[
            global_data["admin_level"] == 1, "admin1Pcode"
        ]
        subset.loc[global_data["admin_level"] == 0, "admin2Pcode"] = global_data.loc[
            global_data["admin_level"] == 0, "location_code"
        ]
        duplicates = subset.duplicated(keep=False)
        global_data["error"] = None
        global_data.loc[duplicates, "error"] = "Duplicate row"
        if sum(duplicates) > 0:
            self._error_handler.add_message(
                "DTM",
                non_hapi_dataset_name,
                f"{sum(duplicates)} duplicates found",
            )

        # Loop through rows to check pcodes, get HRP/GHO status and dates
        global_data["has_hrp"] = None
        global_data["in_gho"] = None
        global_data["reference_period_start"] = None
        global_data["reference_period_end"] = None
        global_data["admin1_code"] = None
        global_data["admin2_code"] = None
        global_data["admin1_name"] = None
        global_data["admin2_name"] = None
        global_data["warning"] = None

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
            row["reference_period_start"] = date
            row["reference_period_end"] = date

            # Check p-code
            admin_level = row["admin_level"]
            if admin_level == 0:
                continue
            pcode = row[f"admin{admin_level}Pcode"]
            admin_name = row[f"provider_admin{admin_level}_name"]
            parent_pcode = None
            parent_name = None
            if admin_level == 2:
                parent_pcode = row[f"admin{admin_level - 1}Pcode"]
                parent_name = row[f"provider_admin{admin_level - 1}_name"]
            admin_info, warning = self.get_admin_info(
                non_hapi_dataset_name,
                country_iso,
                admin_level,
                pcode,
                admin_name,
                parent_pcode,
                parent_name,
            )
            row["admin1_code"] = admin_info["admin1_code"]
            row["admin1_name"] = admin_info["admin1_name"]
            row["admin2_code"] = admin_info["admin2_code"]
            row["admin2_name"] = admin_info["admin2_name"]
            row["warning"] = warning

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
