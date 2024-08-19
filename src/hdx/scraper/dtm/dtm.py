#!/usr/bin/python
"""
WHO:
------------

Reads WHO API and creates datasets

"""

import logging
import zipfile
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional


from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class dtm:

    def __init__(
        self, configuration: Configuration, retriever: Retrieve, temp_dir: str
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir


    def generate_dataset(self) -> Optional[Dataset]:

        # To be generated
        dataset_name = None
        dataset_title = None
        dataset_time_period = None
        dataset_tags = None
        dataset_country_iso3 = None

        # Dataset info
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
                "notes": self._configuration["dataset_notes"],
            }
        )

        dataset.set_time_period(dataset_time_period)
        dataset.add_tagsa(dataset_tags)
        dataset.set_maintainer(self._configuration["dataset_maintainer"])
        dataset.set_organization(self._configuration["dataset_organization"])
        dataset.set_expected_update_frequency(
            self._configuration["dataset_expected_update_frequency"]
        )
        # Only if needed
        dataset.set_subnational(True)
        try:
            dataset.add_country_location(dataset_country_iso3)
        except HDXError:
            logger.error(f"Couldn't find country {dataset_country_iso3}, skipping")
            return

        # Add resources here

        return dataset
