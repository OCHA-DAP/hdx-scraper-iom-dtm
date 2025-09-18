#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from os import getenv
from os.path import expanduser, join
from typing import Optional

from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

from hdx.scraper.iom_dtm.pipeline import Pipeline

logger = logging.getLogger(__name__)

_USER_AGENT_LOOKUP = "hdx-scraper-iom-dtm"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: IOM DTM"


def main(
    dtm_api_key: Optional[str] = None,
    save: bool = False,
    use_saved: bool = False,
    err_to_hdx: Optional[bool] = None,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        dtm_api_key (Optional[str]): API key to access IOM DTM data
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.
        err_to_hdx (Optional[bool]): Whether to write any errors to HDX metadata.
        Defaults to None.

    Returns:
        None
    """
    if dtm_api_key is None:
        dtm_api_key = getenv("DTM_API_KEY")
    headers = {"Ocp-Apim-Subscription-Key": dtm_api_key}
    if err_to_hdx is None:
        err_to_hdx = getenv("ERR_TO_HDX")
    with HDXErrorHandler(write_to_hdx=err_to_hdx) as error_handler:
        with wheretostart_tempdir_batch(folder=_USER_AGENT_LOOKUP) as info:
            temp_dir = info["folder"]
            with Download(headers=headers) as downloader:
                configuration = Configuration.read()
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=temp_dir,
                    saved_dir=_SAVED_DATA_DIR,
                    temp_dir=temp_dir,
                    save=save,
                    use_saved=use_saved,
                )
                dtm = Pipeline(
                    configuration=configuration,
                    retriever=retriever,
                    temp_dir=temp_dir,
                    error_handler=error_handler,
                )

                countries_list = dtm.get_countries()
                operation_status = dtm.get_operation_status()
                for countries in [
                    countries_list,
                    *[[x] for x in countries_list],
                ]:
                    dataset = dtm.generate_dataset(
                        countries=countries, operation_status=operation_status
                    )
                    dataset.update_from_yaml(
                        path=script_dir_plus_file(
                            join("config", "hdx_dataset_static.yaml"),
                            main,
                        )
                    )
                    if len(countries) > 1:
                        dataset.generate_quickcharts(
                            resource=1,
                            path=script_dir_plus_file(
                                join("config", "hdx_resource_view_static.yaml"),
                                main,
                            ),
                        )
                    dataset.create_in_hdx(
                        remove_additional_resources=True,
                        match_resource_order=False,
                        hxl_update=False,
                        updated_by_script=_UPDATED_BY_SCRIPT,
                        batch=info["batch"],
                    )
                    if len(countries) > 1:
                        hapi_dataset = dtm.generate_hapi_dataset(dataset["name"])
                        hapi_dataset.update_from_yaml(
                            path=script_dir_plus_file(
                                join("config", "hdx_hapi_dataset_static.yaml"),
                                main,
                            ),
                        )
                        hapi_dataset.create_in_hdx(
                            remove_additional_resources=True,
                            match_resource_order=False,
                            hxl_update=False,
                            updated_by_script=_UPDATED_BY_SCRIPT,
                            batch=info["batch"],
                        )
    logger.info("HDX Scraper DTM pipeline completed!")


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_USER_AGENT_LOOKUP,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
