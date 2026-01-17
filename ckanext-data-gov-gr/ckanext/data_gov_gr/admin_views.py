from __future__ import annotations

import logging

from flask import Blueprint

import ckan.logic as logic
import ckan.model as model
import ckan.lib.jobs as bg_jobs
from ckan.common import current_user, request
from ckan.plugins import toolkit

log = logging.getLogger(__name__)

admin_blueprint = Blueprint("data_gov_gr_admin", __name__)


def _trash_purge_entity_job(username: str, ent_type: str) -> None:
    """
    Background job: purge deleted entities of one type.
    ent_type: 'package' | 'organization' | 'group'
    """
    ent_type = (ent_type or "").strip()
    action_map = {
        "package": u"dataset_purge",
        "organization": u"organization_purge",
        "group": u"group_purge",
    }
    if ent_type not in action_map:
        raise ValueError(f"Unsupported ent_type: {ent_type}")

    purge_action = action_map[ent_type]
    log.info("trash purge-entity job started by user=%s ent_type=%s", username, ent_type)

    try:
        if ent_type == "package":
            deleted_entities = model.Session.query(model.Package).filter_by(
                state=model.State.DELETED
            )
        elif ent_type == "organization":
            deleted_entities = model.Session.query(model.Group).filter_by(
                state=model.State.DELETED, is_organization=True
            )
        else:  # group
            deleted_entities = model.Session.query(model.Group).filter_by(
                state=model.State.DELETED, is_organization=False
            )

        count = 0
        for entity in deleted_entities:
            logic.get_action(purge_action)({u"user": username}, {u"id": entity.id})
            count += 1

            if count % 200 == 0:
                model.Session.remove()
                log.info("trash purge-entity progress ent_type=%s count=%s", ent_type, count)

        model.Session.remove()
        log.info("trash purge-entity job completed ent_type=%s total=%s", ent_type, count)

    finally:
        try:
            model.Session.remove()
        except Exception:
            log.exception("trash purge-entity: failed to remove SQLAlchemy session")


@admin_blueprint.route("/ckan-admin/trash/purge-entity-async", methods=["POST"])
def trash_purge_entity_async():
    """
    Admin endpoint: enqueue purge job for a single entity type and return immediately.
    Expects form field: action = package|organization|group
    """
    context = {"user": current_user.name, "auth_user_obj": current_user}
    logic.check_access(u"sysadmin", context)

    ent_type = (request.form.get(u"action") or "").strip()

    if ent_type not in ("package", "organization", "group"):
        toolkit.h.flash_error(toolkit._("Μη έγκυρη επιλογή εκκαθάρισης."))
        return toolkit.h.redirect_to("admin.trash")

    job = bg_jobs.enqueue(
        _trash_purge_entity_job,
        kwargs={u"username": current_user.name, u"ent_type": ent_type},
        title=u"Admin trash purge {t} (data_gov_gr)".format(t=ent_type),
        queue=bg_jobs.DEFAULT_QUEUE_NAME,
        rq_kwargs={u"timeout": 6 * 60 * 60},
    )

    toolkit.h.flash_success(
        toolkit._("Η εκκαθάριση ξεκίνησε στο παρασκήνιο (job id: {id}).").format(id=job.id)
    )
    return toolkit.h.redirect_to("admin.trash")


def _trash_purge_all_job(username: str) -> None:
    """
    Background job: purge deleted datasets, groups and organizations.
    Must not use request/current_user.
    """
    log.info("trash purge-all job started by user=%s", username)
    try:
        deleted_packages = model.Session.query(model.Package).filter_by(
            state=model.State.DELETED
        )
        deleted_orgs = model.Session.query(model.Group).filter_by(
            state=model.State.DELETED, is_organization=True
        )
        deleted_groups = model.Session.query(model.Group).filter_by(
            state=model.State.DELETED, is_organization=False
        )

        actions = (u"dataset_purge", u"group_purge", u"organization_purge")
        entities = (deleted_packages, deleted_groups, deleted_orgs)

        for action, deleted_entities in zip(actions, entities):
            count = 0
            for entity in deleted_entities:
                logic.get_action(action)({u"user": username}, {u"id": entity.id})
                count += 1

                if count % 200 == 0:
                    model.Session.remove()
                    log.info("trash purge-all progress action=%s count=%s", action, count)

            model.Session.remove()
            log.info("trash purge-all finished action=%s total=%s", action, count)

        log.info("trash purge-all job completed")
    finally:
        try:
            model.Session.remove()
        except Exception:
            log.exception("trash purge-all: failed to remove SQLAlchemy session")


@admin_blueprint.route("/ckan-admin/trash/purge-all-async", methods=["POST"])
def trash_purge_all_async():
    """
    Admin endpoint: enqueue purge-all job and return immediately.
    """
    context = {"user": current_user.name, "auth_user_obj": current_user}
    logic.check_access(u"sysadmin", context)

    job = bg_jobs.enqueue(
        _trash_purge_all_job,
        kwargs={u"username": current_user.name},
        title=u"Admin trash purge all (data_gov_gr)",
        queue=bg_jobs.DEFAULT_QUEUE_NAME,
        rq_kwargs={u"timeout": 6 * 60 * 60},  # 6 hours
    )

    toolkit.h.flash_success(
        toolkit._("Purge started in background (job id: {id}).").format(id=job.id)
    )
    return toolkit.h.redirect_to("admin.trash")