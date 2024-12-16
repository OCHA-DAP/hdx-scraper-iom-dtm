from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

from hdx.scraper.dtm.dtm import Dtm


@pytest.fixture(scope="module")
def expected_dataset():
    return {
        "caveats": "This dataset comes from the [DTM "
        "API](https://dtm.iom.int/data-and-analysis/dtm-api), which "
        "provides only non-sensitive IDP figures, aggregated at the "
        "country, Admin 1, and Admin 2 levels. For more detailed "
        "information, please see the [country-specific DTM datasets on "
        "HDX](https://data.humdata.org/dataset/?dataseries_name=IOM%20-%20DTM%20Baseline%20Assessment&dataseries_name=IOM%20-%20DTM%20Event%20and%20Flow%20Tracking&dataseries_name=IOM%20-%20DTM%20Site%20and%20Location%20Assessment&organization=international-organization-for-migration&q=&sort=last_modified%20desc&ext_page_size=25).\n",
        "data_update_frequency": 7,
        "dataset_date": "[2017-09-30T00:00:00 TO 2024-06-30T23:59:59]",
        "dataset_preview": "resource_id",
        "dataset_source": "International Organization " "for Migration (IOM)",
        "groups": [{"name": "hti"}, {"name": "afg"}, {"name": "tcd"}],
        "license_id": "hdx-other",
        "license_other": "Copyright © International Organization for "
        "Migration 2018 "
        "IOM reserves the right to assert ownership of the Materials "
        "collected on the https://data.humdata.org/ "
        "website. The Materials may be viewed, downloaded, and "
        "printed for non-commercial use only, without, inter alia, "
        "any right to sell, resell, redistribute or create "
        "derivative works therefrom. At all times the User shall "
        "credit the DTM as the source, unless otherwise stated. The "
        "user must include the URL of the Materials from the HDX "
        "Website, as well as the following credit line: Source: "
        "“International Organization for Migration (IOM), "
        "Displacement Tracking Matrix (DTM)”.\n",
        "maintainer": "80d68c27-4b7f-4865-87c6-050ebb6912ae",
        "methodology": "Other",
        "methodology_other": "[DTM Methodological "
        "Framework](https://dtm.iom.int/about/methodological-framework)\n",
        "name": "global-iom-dtm-from-api",
        "notes": "This dataset comes from the International Organization for "
        "Migration (IOM)'s displacement tracking matrix (DTM) [publicly "
        "accessible API](https://dtm.iom.int/data-and-analysis/dtm-api). "
        "This API allows the humanitarian community, academia, media, "
        "government, and non-governmental organizations to utilize the data "
        "collected by DTM. The DTM API only provides non-sensitive IDP "
        "figures, aggregated at the country, Admin 1 (states, provinces, or "
        "equivalent), and Admin 2 (smaller subnational administrative areas) "
        "levels. For more detailed information, please see the "
        "[country-specific DTM datasets on "
        "HDX](https://data.humdata.org/dataset/?dataseries_name=IOM%20-%20DTM%20Baseline%20Assessment&dataseries_name=IOM%20-%20DTM%20Event%20and%20Flow%20Tracking&dataseries_name=IOM%20-%20DTM%20Site%20and%20Location%20Assessment&organization=international-organization-for-migration&q=&sort=last_modified%20desc&ext_page_size=25).\n",
        "owner_org": "f53d32cd-132c-4ef4-bc6d-058f94d08adf",
        "package_creator": "HDX Data Systems Team",
        "private": False,
        "subnational": True,
        "tags": [
            {
                "name": "conflict-violence",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
            {
                "name": "displacement",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
            {
                "name": "forced displacement",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
            {
                "name": "hxl",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
            {
                "name": "internally displaced persons-idp",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
        ],
        "title": "Global IOM Displacement Tracking Matrix (DTM) from API",
    }


@pytest.fixture(scope="module")
def expected_resources():
    return [
        {
            "dataset_preview_enabled": "False",
            "description": "Global IOM displacement tracking matrix data "
            "at admin levels "
            "0, 1, and 2, sourced from the DTM API",
            "format": "csv",
            "name": "Global IOM DTM data for admin levels 0-2",
            "resource_type": "file.upload",
            "url_type": "upload",
        },
        {
            "dataset_preview_enabled": "True",
            "description": "Filtered and aggregated data used to "
            "create QuickCharts",
            "format": "csv",
            "name": "Data for QuickCharts",
            "resource_type": "file.upload",
            "url_type": "upload",
        },
    ]


class TestDtm:
    @pytest.fixture(scope="function")
    def configuration(self, config_dir):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=join(config_dir, "project_configuration.yaml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="class")
    def fixtures_dir(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="class")
    def input_dir(self, fixtures_dir):
        return join(fixtures_dir, "input")

    @pytest.fixture(scope="class")
    def config_dir(self, fixtures_dir):
        return join("src", "hdx", "scraper", "dtm", "config")

    def test_dtm(
        self,
        configuration,
        fixtures_dir,
        input_dir,
        config_dir,
        expected_dataset,
        expected_resources,
    ):
        with temp_dir(
            "Testdtm",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                dtm = Dtm(
                    configuration=configuration,
                    retriever=retriever,
                    temp_dir=tempdir,
                )
                countries = dtm.get_countries()
                operation_status = dtm.get_operation_status()
                dataset = dtm.generate_dataset(
                    countries=countries, operation_status=operation_status
                )
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                dataset.generate_quickcharts(
                    resource=1,
                    path=join(config_dir, "hdx_resource_view_static.yaml"),
                )
                assert dataset == expected_dataset
                assert dataset.get_resources()[:2] == expected_resources

                filename_list = [
                    "global-iom-dtm-from-api-admin-0-to-2.csv",
                    "data_for_quickcharts.csv",
                ]
                for filename in filename_list:
                    assert_files_same(
                        join("tests", "fixtures", filename),
                        join(tempdir, filename),
                    )
