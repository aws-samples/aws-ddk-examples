import logging
from typing import Any, Dict

import boto3

_logger = logging.getLogger()
_logger.setLevel(logging.INFO)

logging.basicConfig(
    format="%(levelname)s %(threadName)s [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d:%H:%M:%S",
    level=logging.INFO,
)

try:
    _logger.info("Logger initialization completed")
except Exception as e:
    _logger.error(e, exc_info=True)
    init_failed = e


def publish_recipe(recipe_name: str) -> None:
    gdb_client = boto3.client("databrew")
    try:
        gdb_client.publish_recipe(Name=recipe_name)
    except Exception as e:
        raise e


def delete_recipe(recipe_name: str, recipe_version: str) -> None:
    gdb_client = boto3.client("databrew")
    try:
        gdb_client.delete_recipe_version(Name=recipe_name, RecipeVersion=recipe_version)
    except Exception as e:
        raise e


def describe_recipe(recipe_name: str) -> str:
    version = str()
    gdb_client = boto3.client("databrew")
    try:
        resp = gdb_client.describe_recipe(Name=recipe_name)
    except Exception as e:
        raise e

    version = resp["RecipeVersion"]
    return version


def on_create(
    event: Dict[str, Any], recipe_name: str, props: Dict[str, Any]
) -> Dict[str, Any]:
    publish_recipe(recipe_name=recipe_name)
    recipe_version = describe_recipe(recipe_name=recipe_name)
    _logger.info(f"Create resource {recipe_name} with props {props}")
    return {
        "PhysicalResourceId": recipe_name,
        "Data": {"RecipeVersion": recipe_version},
    }


def on_update(
    event: Dict[str, Any], recipe_name: str, props: Dict[str, Any]
) -> Dict[str, Any]:
    publish_recipe(recipe_name=recipe_name)
    recipe_version = describe_recipe(recipe_name=recipe_name)
    _logger.info(f"Update resource {recipe_name} with props {props}")
    return {
        "PhysicalResourceId": recipe_name,
        "Data": {"RecipeVersion": recipe_version},
    }


def on_delete(
    event: Dict[str, Any], recipe_name: str, props: Dict[str, Any]
) -> Dict[str, Any]:
    recipe_version = describe_recipe(recipe_name = recipe_name)
    delete_recipe(recipe_name=recipe_name, recipe_version=recipe_version)
    _logger.info(f"Delete resource {recipe_name} with props {props}")
    return {"PhysicalResourceId": recipe_name}


def handler(event: Dict[str, Any], context: Any) -> Any:
    _logger.info(f"Received event: {event}")

    request_type = event["RequestType"]
    props = event["ResourceProperties"]

    _logger.info(f"Received GlueDatabrew Properties: {props}")

    recipe_name = props["RecipeName"]
    _logger.info(f"GlueDatabrew Receipe Name: {recipe_name}")

    if request_type == "Create":
        return on_create(event=event, recipe_name=recipe_name, props=props)

    elif request_type == "Update":
        return on_update(event=event, recipe_name=recipe_name, props=props)

    elif request_type == "Delete":
        return on_delete(event=event, recipe_name=recipe_name, props=props)

    raise Exception("Invalid request type: %s" % request_type)
