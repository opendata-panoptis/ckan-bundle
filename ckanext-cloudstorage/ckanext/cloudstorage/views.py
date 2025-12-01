# -*- coding: utf-8 -*-

from flask import Blueprint

import ckanext.cloudstorage.utils as utils

cloudstorage = Blueprint("cloudstorage", __name__)


@cloudstorage.route("/dataset/<id>/resource/<resource_id>/download")
@cloudstorage.route("/dataset/<id>/resource/<resource_id>/download/<filename>")
def download(id, resource_id, filename=None, package_type="dataset"):
    return utils.resource_download(id, resource_id, filename)

@cloudstorage.route("/data-service/<id>/resource/<resource_id>/download")
@cloudstorage.route("/data-service/<id>/resource/<resource_id>/download/<filename>")
def data_service_download(id, resource_id, filename=None):
    return utils.resource_download(id, resource_id, filename)

@cloudstorage.route("/decision/<id>/recource/<resource_id>/download")
@cloudstorage.route("/decision/<id>/resource/<resource_id>/download/<filename>")
def decision_download(id, resource_id, filename=None):
    return utils.resource_download(id, resource_id, filename)

def get_blueprints():
    return [cloudstorage]
