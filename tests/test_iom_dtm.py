from os.path import join

import pytest
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.iom_dtm.pipeline import Pipeline


@pytest.fixture(scope="module")
def expected_dataset():
    return {
        "caveats": "This dataset comes from the [DTM "
        "API](https://dtm.iom.int/data-and-analysis/dtm-api), which "
        "provides only non-sensitive IDP figures, aggregated at the "
        "country, Admin 1, and Admin 2 levels. For more detailed "
        "information, please see the [country-specific DTM datasets on "
        "HDX](https://data.humdata.org/dataset/?dataseries_name=IOM%20-%20DTM"
        "%20Baseline%20Assessment&dataseries_name=IOM%20-%20DTM%20Event%20and"
        "%20Flow%20Tracking&dataseries_name=IOM%20-%20DTM%20Site%20and%20"
        "Location%20Assessment&organization=international-organization-for-"
        "migration&q=&sort=last_modified%20desc&ext_page_size=25). "
        "IOM is continually adding new data as well as updating data "
        "that's already been added.\n",
        "data_update_frequency": 7,
        "dataset_date": "[2010-11-30T00:00:00 TO 2025-06-30T23:59:59]",
        "dataset_preview": "resource_id",
        "dataset_source": "International Organization for Migration (IOM)",
        "groups": [{"name": "afg"}, {"name": "tcd"}, {"name": "hti"}],
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
        "methodology_other": "[DTM Methodological Framework]"
        "(https://dtm.iom.int/about/methodological-framework)\n\n"
        "Note that the assessment_type field accepts three values:\n\n"
        "  - BA: Baseline Assessment\n"
        "  - ETT: Emergency Tracking Tool, Event Tracking Tool, "
        "or Emergency Event Tracking\n"
        "  - SA: Site Assessment, Multi-Sectoral Location "
        "Assessment, or Needs Monitoring Assessment\n",
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
def expected_hapi_dataset():
    return {
        "name": "hdx-hapi-idps",
        "title": "HDX HAPI - Affected People: Internally-Displaced Persons",
        "groups": [{"name": "world"}],
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
        "dataset_date": "[2010-11-30T00:00:00 TO 2025-06-30T23:59:59]",
        "subnational": True,
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
        "methodology": "Registry",
        "caveats": "",
        "dataset_source": "International Organization for Migration (IOM)",
        "package_creator": "HDX Data Systems Team",
        "private": False,
        "maintainer": "80d68c27-4b7f-4865-87c6-050ebb6912ae",
        "owner_org": "hdx-hapi",
        "data_update_frequency": 7,
        "notes": "This dataset contains data obtained from the\n"
        "[HDX Humanitarian API](https://hapi.humdata.org/) (HDX HAPI),\n"
        "which provides standardized humanitarian indicators designed\n"
        "for seamless interoperability from multiple sources.\n"
        "The data facilitates automated workflows and visualizations\n"
        "to support humanitarian decision making.\n"
        "For more information, please see the HDX HAPI\n"
        "[landing page](https://data.humdata.org/hapi)\n"
        "and\n"
        "[documentation](https://hdx-hapi.readthedocs.io/en/latest/).\n"
        "\n"
        "Warnings typically indicate corrections have been made to\n"
        "the data or show things to look out for. Rows with only warnings\n"
        "are considered complete, and are made available via the API.\n"
        "Errors usually mean that the data is incomplete or unusable.\n"
        "Rows with any errors are not present in the API but are included\n"
        "here for transparency.\n",
    }


@pytest.fixture(scope="module")
def expected_resources():
    return [
        {
            "dataset_preview_enabled": "False",
            "description": "Global IOM displacement tracking matrix data at admin levels "
            "0, 1, and 2, sourced from the DTM API",
            "format": "csv",
            "name": "Global IOM DTM data for admin levels 0-2",
            "p_coded": True,
        },
        {
            "dataset_preview_enabled": "True",
            "description": "Filtered and aggregated data used to create QuickCharts",
            "format": "csv",
            "name": "Data for QuickCharts",
        },
    ]


@pytest.fixture(scope="module")
def expected_hapi_resources():
    return [
        {
            "name": "Global Affected People: Internally-Displaced Persons",
            "description": "IDPs data from HDX HAPI, please see [the "
            "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_"
            "usage_guides/affected_people/#internally-displaced-persons) for "
            "more information",
            "p_coded": True,
            "format": "csv",
        }
    ]


class TestDtm:
    def test_dtm(
        self,
        configuration,
        read_dataset,
        fixtures_dir,
        input_dir,
        config_dir,
        expected_dataset,
        expected_resources,
        expected_hapi_dataset,
        expected_hapi_resources,
    ):
        with HDXErrorHandler() as error_handler:
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
                    dtm = Pipeline(
                        configuration=configuration,
                        retriever=retriever,
                        temp_dir=tempdir,
                        error_handler=error_handler,
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

                    hapi_dataset = dtm.generate_hapi_dataset("global-iom-dtm-from-api")
                    hapi_dataset.update_from_yaml(
                        path=join(config_dir, "hdx_hapi_dataset_static.yaml")
                    )
                    assert hapi_dataset == expected_hapi_dataset
                    assert hapi_dataset.get_resources() == expected_hapi_resources
                    assert_files_same(
                        join("tests", "fixtures", "hdx_hapi_idps_global.csv"),
                        join(tempdir, "hdx_hapi_idps_global.csv"),
                    )
