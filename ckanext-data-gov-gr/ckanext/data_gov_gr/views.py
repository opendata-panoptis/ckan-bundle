from flask import Blueprint, Response, redirect, stream_with_context
from flask.views import MethodView
from typing import Dict, Any
import time
import logging

from ckan.common import request, current_user, config
from ckan.lib.base import render, abort
from ckan.lib.helpers import lang
from ckan.plugins import toolkit
from ckan.logic import NotFound, NotAuthorized, get_action
from ckanext.data_gov_gr.helpers import get_config_as_bool, get_powerbi_embed_url

from ckanext.data_gov_gr.logic.mqa_calculator import MQACalculator
from ckanext.data_gov_gr.stats import DataGovStats
import requests

log = logging.getLogger(__name__)

blueprint = Blueprint('dataset_type', __name__)

GITBOOK_PDF_ENDPOINT = 'https://api.gitbook.com/v1/spaces/{space_id}/pdf'
GITBOOK_PDF_TIMEOUT = 60


@blueprint.route('/guides/pdf')
def download_guides_pdf():
    space_id = config.get('ckanext.data_gov_gr.gitbook.space_id')
    token = config.get('ckanext.data_gov_gr.gitbook.api_token')

    if not space_id or not token:
        log.warning('GitBook PDF download requested without configured credentials')
        abort(404)

    api_url = GITBOOK_PDF_ENDPOINT.format(space_id=space_id.strip())
    headers = {
        'Authorization': f'Bearer {token.strip()}',
        'Accept': 'application/json, application/pdf'
    }

    try:
        response = requests.get(api_url, headers=headers, stream=True, timeout=GITBOOK_PDF_TIMEOUT)
    except requests.RequestException as exc:
        log.error('GitBook PDF request failed: %s', exc)
        abort(502, toolkit._('Δεν ήταν δυνατή η λήψη του PDF. Προσπαθήστε ξανά αργότερα.'))

    if response.status_code >= 400:
        try:
            error_preview = response.text[:500]
        except Exception:
            error_preview = '<binary>'
        log.error('GitBook PDF request failed (%s): %s', response.status_code, error_preview)
        abort(502, toolkit._('Δεν ήταν δυνατή η λήψη του PDF. Προσπαθήστε ξανά αργότερα.'))

    content_type = response.headers.get('Content-Type', '')
    if 'application/json' in content_type.lower():
        try:
            payload = response.json()
        except ValueError:
            log.error('GitBook PDF JSON response could not be decoded')
            abort(502, toolkit._('Δεν ήταν δυνατή η λήψη του PDF. Προσπαθήστε ξανά αργότερα.'))

        download_url = payload.get('url')
        if download_url:
            return redirect(download_url)

        log.error('GitBook PDF JSON response did not include a download URL: %s', payload)
        abort(502, toolkit._('Δεν ήταν δυνατή η λήψη του PDF. Προσπαθήστε ξανά αργότερα.'))

    def generate():
        try:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    yield chunk
        finally:
            response.close()

    response_headers = {
        'Content-Type': content_type or 'application/pdf',
        'Content-Disposition': response.headers.get(
            'Content-Disposition',
            f'attachment; filename="guides-{space_id.strip()}.pdf"'
        )
    }

    return Response(stream_with_context(generate()), headers=response_headers)

def redirect_to_dataset_type():
    current_lang = lang()
    return redirect(f'/{current_lang}/dataset')

def redirect_to_data_service_type():
    current_lang = lang()
    return redirect(f'/{current_lang}/data-service')

def redirect_to_decision_type():
    current_lang = lang()
    return redirect(f'/{current_lang}/decision')

def _get_dataset_types():
    # Παίρνουμε όλους τους τύπους από το scheming αν υπάρχει
    try:
        # Δοκιμάζουμε πρώτα να πάρουμε τα schemas από το scheming extension
        schema_list = toolkit.get_action('scheming_dataset_schema_list')({}, {})
        return schema_list
    except toolkit.ObjectNotFound:
        # Αν δεν υπάρχει το scheming, επιστρέφουμε τον προεπιλεγμένο τύπο
        return ['dataset']  # Ο προεπιλεγμένος τύπος του CKAN

def show_grid_buttons():
    # Παίρνουμε την παράμετρο group από το URL
    group_name = request.args.get('group')

    # Δημιουργούμε μια λίστα με τα κουμπιά που θέλουμε να εμφανίσουμε
    # Δημιουργούμε τα κουμπιά με τα σωστά URLs
    buttons = []
    for dataset_type in _get_dataset_types():
        # Δημιουργούμε το base URL
        current_lang = lang()
        base_url = f'/{current_lang}/{dataset_type}/new'

        # Προσθήκη παραμέτρων στο URL
        params = []
        if group_name:
            params.append(f'group={group_name}')

        # Για dataset τύπου dataset, προσθέτουμε access_rights_type=open
        if dataset_type == 'dataset':
            params.append('access_rights_type=open')

        # Δημιουργούμε το τελικό URL
        if params:
            url = f'{base_url}?{"&".join(params)}'
        else:
            url = base_url

        display_dataset_type_label = dataset_type.replace("-", " ").title()
        buttons.append({
            'title': toolkit._(f'New {display_dataset_type_label}'),
            'name': f'New {display_dataset_type_label}',
            'url': url,
            'description': toolkit._(f'Create a new {display_dataset_type_label}')
        })

    if get_config_as_bool('ckanext.data_gov_gr.dataset.different_form_for_protected_data'):
        # Προσθήκη κουμπιού για Protected Dataset
        display_dataset_type_label = 'Dataset of Protected Data'

        # Δημιουργούμε το URL για τα protected data
        current_lang = lang()
        base_url = f'/{current_lang}/dataset/new'
        params = []
        if group_name:
            params.append(f'group={group_name}')
        params.append('access_rights_type=protected')

        protected_url = f'{base_url}?{"&".join(params)}'

        buttons.insert(1, {
            'title': toolkit._(f'New {display_dataset_type_label}'),
            'name': f'New {display_dataset_type_label}',
            'url': protected_url,
            'description': toolkit._(f'Create a new {display_dataset_type_label}')
        })

    # Επιστρέφουμε το template με τα κουμπιά
    template_name = 'package/snippets/add_dataset.html'
    extra_vars = {}
    extra_vars['buttons'] = buttons
    return render(template_name, extra_vars)


# Καθορίζουμε τα routes στο blueprint
blueprint.add_url_rule("/dataset_type", view_func=redirect_to_dataset_type, endpoint='dataset_type')
blueprint.add_url_rule("/data-service_type", view_func=redirect_to_data_service_type, endpoint='data-service_type')
blueprint.add_url_rule("/decision_type", view_func=redirect_to_decision_type, endpoint='decision_type')
blueprint.add_url_rule("/add_dataset", view_func=show_grid_buttons, endpoint='add_dataset')


class MQAView(MethodView):
    """View class for the MQA tab of a dataset."""

    def _prepare(self, id: str) -> tuple[Dict[str, Any], Dict[str, Any]]:
        """Prepare the context and dataset dictionary."""
        context = {
            'user': current_user.name,
            'for_view': True,
            'auth_user_obj': current_user,
            'use_cache': False
        }

        try:
            pkg_dict = get_action('package_show')(context, {'id': id})
        except (NotFound, NotAuthorized):
            abort(404, toolkit._('Dataset not found'))

        return context, pkg_dict

    def get(self, package_type: str, id: str) -> str:
        """Handle GET requests."""
        start_time = time.time()

        context, pkg_dict = self._prepare(id)
        dataset_type = pkg_dict.get('type') or package_type

        # Calculate MQA scores
        # Enable/Disable URL checking by default to improve performance
        check_urls = toolkit.asbool(toolkit.config.get('ckanext.data_gov_gr.mqa.check_urls', True))
        calculator = MQACalculator(check_urls=check_urls)

        calc_start_time = time.time()
        mqa_scores = calculator.calculate_all_scores(pkg_dict)
        calc_time = time.time() - calc_start_time



        # Prepare display data for the template
        display_data_start_time = time.time()
        display_data = calculator.prepare_display_data(pkg_dict, mqa_scores)
        display_data_time = time.time() - display_data_start_time

        # Render the template
        render_start_time = time.time()
        result = render(
            'package/mqa.html', {
                'dataset_type': dataset_type,
                'pkg_dict': pkg_dict,
                'mqa_scores': mqa_scores,
                'display_data': display_data,
            }
        )
        render_time = time.time() - render_start_time

        # Log performance metrics
        total_time = time.time() - start_time
        log.info(
            f"MQA performance metrics for dataset {id}: "
            f"Total: {total_time:.3f}s, "
            f"Calculation: {calc_time:.3f}s, "
            f"Display data preparation: {display_data_time:.3f}s, "
            f"Rendering: {render_time:.3f}s, "
            f"URL checking: {'enabled' if check_urls else 'disabled'}"
        )

        return result



# Register the MQA view
blueprint.add_url_rule(
    '/<package_type>/mqa/<id>',
    view_func=MQAView.as_view(str('mqa')),
    methods=['GET']
)


@blueprint.route('/stats/total-datasets')
def stats_total_datasets():
    stats = DataGovStats()
    raw_packages_by_week = [
        {
            'date': toolkit.h.date_str_to_datetime(week_date),
            'total_packages': cumulative_num_packages
        }
        for week_date, _num_packages, cumulative_num_packages in stats.get_num_packages_by_week()
    ]

    return render(
        'ckanext/stats/total_datasets.html',
        {
            'raw_packages_by_week': raw_packages_by_week
        }
    )


@blueprint.route('/stats/dataset-revisions')
def stats_dataset_revisions():
    stats = DataGovStats()

    raw_all_package_revisions = [
        {
            'date': toolkit.h.date_str_to_datetime(week_date),
            'total_revisions': num_revisions
        }
        for week_date, _pkgs, num_revisions, _cumulative in stats.get_by_week('package_revisions')
    ]

    raw_new_datasets = [
        {
            'date': toolkit.h.date_str_to_datetime(week_date),
            'new_packages': num_packages
        }
        for week_date, _pkgs, num_packages, _cumulative in stats.get_by_week('new_packages')
    ]

    return render(
        'ckanext/stats/dataset_revisions.html',
        {
            'raw_all_package_revisions': raw_all_package_revisions,
            'raw_new_datasets': raw_new_datasets
        }
    )


@blueprint.route('/stats/most-edited')
def stats_most_edited():
    stats = DataGovStats()
    extra_vars = {
        'most_edited_packages': stats.most_edited_packages()
    }
    return render('ckanext/stats/most_edited.html', extra_vars)


@blueprint.route('/stats/largest-groups')
def stats_largest_groups():
    stats = DataGovStats()
    extra_vars = {
        'largest_groups': stats.largest_groups()
    }
    return render('ckanext/stats/largest_groups.html', extra_vars)


@blueprint.route('/stats/top-tags')
def stats_top_tags():
    stats = DataGovStats()
    extra_vars = {
        'top_tags': stats.top_tags()
    }
    return render('ckanext/stats/top_tags.html', extra_vars)


@blueprint.route('/stats/top-creators')
def stats_top_creators():
    stats = DataGovStats()
    extra_vars = {
        'top_package_creators': stats.top_package_creators()
    }
    return render('ckanext/stats/top_creators.html', extra_vars)


@blueprint.route('/stats/datasets-by-publisher-type')
def stats_datasets_by_publisher_type():
    stats = DataGovStats()
    extra_vars = {
        'datasets_by_publisher_type': stats.datasets_by_publisher_type(),
    }
    return render('ckanext/stats/datasets_by_publisher_type.html', extra_vars)


@blueprint.route('/stats/datasets-per-organization')
def stats_datasets_per_organization():
    stats = DataGovStats()
    extra_vars = {
        'datasets_by_organization': stats.datasets_by_organization(),
    }
    return render('ckanext/stats/datasets_by_organization.html', extra_vars)


@blueprint.route('/stats/datasets-vs-services')
def stats_datasets_vs_services():
    stats = DataGovStats()
    extra_vars = {
        'datasets_vs_services': stats.datasets_vs_services(),
    }
    return render('ckanext/stats/datasets_vs_services.html', extra_vars)


@blueprint.route('/stats/datasets-by-hvd-category')
def stats_datasets_by_hvd_category():
    stats = DataGovStats()
    extra_vars = {
        'datasets_by_hvd_category': stats.datasets_by_hvd_category(),
    }
    return render('ckanext/stats/datasets_by_hvd_category.html', extra_vars)


@blueprint.route('/stats/powerbi')
def stats_powerbi():
    """
    Render the Power BI statistics page.

    The embed URL is provided via the admin-configurable
    ``ckanext.data_gov_gr.powerbi_embed_url`` option if set in
    ``/ckan-admin/config``. If it is not set there, this view will fall back
    to the ``powerbi.embed_url`` option from the configuration file.
    """
    embed_url = get_powerbi_embed_url()

    if not embed_url:
        log.info('Power BI stats page requested but powerbi.embed_url is not configured')

    return render(
        'ckanext/stats/powerbi.html',
        {
            'powerbi_embed_url': embed_url
        }
    )

def more_page():
    """Render the More page with content sections as cards"""
    template_name = 'more_base.html'
    extra_vars = {
        'page_title': 'Περισσότερα',
        'sections': [
            {
                'title': 'Blog',
                'description': 'Άρθρα με νέα, ανακοινώσεις και ενημερώσεις για το σύστημα ή τα δεδομένα',
                'url': 'blog',
                'icon': 'fa-newspaper',
                'color': 'primary'
            },
            {
                'title': 'Οδηγοί',
                'description': 'Τεκμηρίωση και οδηγοί χρήσης.',
                'url': config.get('guides_base_url', '/pages'),
                'icon': 'fa-book',
                'color': 'success'
            },
            {
                'title': 'Σχετικά με την Πύλη',
                'description': 'Πληροφορίες για το σύστημα και την αποστολή της πύλης ανοικτών δεδομένων',
                'url': 'pages/about-portal',
                'icon': 'fa-info-circle',
                'color': 'info'
            },
            {
                'title': 'Ανοικτά Δεδομένα',
                'description': 'Εισαγωγικές πληροφορίες και οδηγίες για τα ανοικτά δεδομένα',
                'url': 'pages/anoikta',
                'icon': 'fa-unlock',
                'color': 'warning'
            },
            {
                'title': 'Προστατευόμενα Δεδομένα',
                'description': 'Εισαγωγικές πληροφορίες για τα περιορισμένης πρόσβασης δεδομένα',
                'url': 'pages/prostateumena',
                'icon': 'fa-shield',
                'color': 'danger'
            },
            {
                'title': 'Αναφορές',
                'description': 'Στατιστικά στοιχεία και αναφορές ποιότητας για τα δεδομένα της πύλης',
                'url': 'report',
                'icon': 'fa-bar-chart',
                'color': 'dark'
            },
            {
                'title': 'Στατιστικά',
                'description': 'Προβολή στατιστικών χρήσης και δεικτών για την πύλη δεδομένων',
                'url': config.get('ckanext.data_gov_gr.stats.destination.url', '/'),
                'icon': 'fa-chart-line',
                'color': 'secondary'
            }
        ]
    }
    return render(template_name, extra_vars)

# Add the /more route
blueprint.add_url_rule("/more", view_func=more_page, endpoint='more_page')

def get_blueprint():
    return blueprint
