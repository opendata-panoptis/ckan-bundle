import click
import ckan.plugins as p

import logging
from typing import Any, Callable

from flask import make_response

from ckan.plugins import toolkit
from ckan.types import Context, CKANApp
from ckan.common import CKANConfig

from .cli.tracking import tracking
from .helpers import popular
from .middleware import track_request
from .model import TrackingSummary

log = logging.getLogger(__name__)


def _tracking_pixel():
    # Το `ckanext.tracking` αποθηκεύει τα πραγματικά tracking δεδομένα σε
    # `after_request` hook (βλ. `ckanext.tracking.middleware.track_request`).
    # Παρ' όλα αυτά χρειαζόμαστε ένα κανονικό endpoint εδώ, αλλιώς το static
    # catch-all route του Flask (στο CKAN ισχύει `static_url_path=''`) θα
    # επιστρέφει 405 για POST requests στο `/_tracking` και θα γεμίζει η κονσόλα
    # του browser με σφάλματα.
    return make_response("", 204)


class TrackingPlugin(p.SingletonPlugin):
    p.implements(p.IClick)
    p.implements(p.IConfigurer)
    p.implements(p.IMiddleware, inherit=True)
    p.implements(p.IPackageController, inherit=True)
    p.implements(p.ITemplateHelpers)

    # IClick
    def get_commands(self) -> "list[click.Command]":
        return [tracking]

    # IConfigurer
    def update_config(self, config: CKANConfig) -> None:
        toolkit.add_resource("assets", "tracking")
        toolkit.add_template_directory(config, "templates")

    # IMiddleware
    def make_middleware(self, app: CKANApp, config: CKANConfig) -> Any:
        if "tracking._tracking_pixel" not in app.view_functions:
            app.add_url_rule(
                "/_tracking",
                endpoint="tracking._tracking_pixel",
                view_func=_tracking_pixel,
                methods=(u"POST",),
            )
            try:
                from ckan.config.middleware.flask_app import csrf
                csrf.exempt(_tracking_pixel)
            except Exception:
                log.exception("Failed to exempt /_tracking from CSRF protection")

        app.after_request(track_request)
        return app

    # IPackageController
    def after_dataset_show(
        self, context: Context, pkg_dict: "dict[str, Any]"
    ) -> "dict[str, Any]":
        """Appends tracking summary data to the package dict.

        Tracking data is not stored in Solr so we need to retrieve it
        from the database.
        """
        if "id" not in pkg_dict:
            return pkg_dict

        pkg_dict["tracking_summary"] = TrackingSummary.get_for_package(
            pkg_dict["id"]
        )

        for resource_dict in pkg_dict.get("resources", []):
            if "url" not in resource_dict:
                continue

            resource_dict["tracking_summary"] = (
                TrackingSummary.get_for_resource(resource_dict["url"])
            )

        return pkg_dict

    def after_dataset_search(
        self, search_results: "dict[str, Any]", search_params: "dict[str, Any]"
    ) -> "dict[str, Any]":
        """Add tracking summary to search results.

        Tracking data is indexed but not stored in Solr so we need to
        fetch it from the database. This can cause some discrepancies since
        the number of views when indexing might have been different than
        when this code is run.
        """
        for package_dict in search_results["results"]:
            if "id" not in package_dict:
                continue

            package_dict["tracking_summary"] = TrackingSummary.get_for_package(
                package_dict["id"]
            )

            for resource_dict in package_dict.get("resources", []):
                if "url" not in resource_dict:
                    continue

                resource_dict["tracking_summary"] = (
                    TrackingSummary.get_for_resource(resource_dict["url"])
                )

        return search_results

    def before_dataset_index(
        self, pkg_dict: "dict[str, Any]"
    ) -> "dict[str, Any]":
        """Index tracking information.

        This method will index (but not store) the tracking information of
        the dataset. This will only allow us to sort Solr's queries by views.
        For the actual data we will query the database after the search.

        It will also remove the tracking_summary key from the package dict
        since it is not a valid Solr field.
        """
        pkg_dict.pop("tracking_summary", None)
        for r in pkg_dict.get("resources", []):
            r.pop("tracking_summary", None)

        tracking_summary = TrackingSummary.get_for_package(pkg_dict["id"])
        pkg_dict["views_total"] = tracking_summary["total"]
        pkg_dict["views_recent"] = tracking_summary["recent"]
        return pkg_dict

    # ITemplateHelpers
    def get_helpers(self) -> "dict[str, Callable[...,Any]]":
        return {
            "popular": popular,
        }
