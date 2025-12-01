# encoding: utf-8

import click

from . import utils
from ckan.plugins import toolkit
from ckan import model
from ckan.lib.mailer import _mail_recipient
from ckanext.report.stale_datasets_report import stale_datasets_report
from ckan.logic import get_action


def get_commands():
    return [report]


@click.group()
def report():
    """Generates reports"""
    pass


@report.command()
def initdb():
    """Creates necessary db tables"""
    utils.initdb()
    click.secho(u'Report table is setup', fg=u"green")


@report.command()
@click.argument(u'report_list', required=False)
def generate(report_list):
    """
    Generate and cache reports - all of them unless you specify
    a comma separated list of them.
    """
    if report_list:
        report_list = [s.strip() for s in report_list.split(',')]
    timings = utils.generate(report_list)

    click.secho(u'Report generation complete %s' % timings, fg=u"green")


@report.command()
def list():
    """ Lists the reports
    """
    utils.list()


@report.command()
@click.argument(u'report_name')
@click.argument(u'report_options', nargs=-1)
def generate_for_options(report_name, report_options):
    """
    Generate and cache a report for one combination of option values.
    You can leave it with the defaults or specify options
    as more parameters: key1=value key2=value
    """
    message = utils.generate_for_options(report_name, report_options)
    if message:
        click.secho(message, fg=u"red")


@report.command()
@click.option('-o', '--organization', help='Organization name')
@click.option('-s', '--send-emails', is_flag=True, help='Send emails to organization admins')
@click.option('-t', '--test-email', help='Send test email to specific address')
def send_stale_report(organization=None, send_emails=False, test_email=None):
    """
    Generate stale datasets report and optionally send emails to organization admins.
    
    By default, sends a summary report for all organizations.
    Use -o to specify a specific organization.
    Use -s to actually send emails to organization admins.
    Use -t to send a test email to a specific address.
    """
    try:
        # Generate the report
        report_data = stale_datasets_report(organization=organization)
        
        if test_email:
            # Send test email
            _send_report_email(
                report_data, 
                'Μη Επικαιροποιημένα Σύνολα Δεδομένων',
                test_email, 
                'Test Recipient'
            )
            click.secho('Test email sent to {}'.format(test_email), fg='green')
            return
            
        if send_emails:
            # Send emails to organization admins
            org_emails_sent = _send_emails_to_org_admins(report_data, organization)
            click.secho('Sent emails to {} organizations'.format(org_emails_sent), fg='green')
        else:
            # Just display summary
            _display_report_summary(report_data)
            
    except Exception as e:
        click.secho('Error generating report: {}'.format(str(e)), fg='red')
        raise


def _send_emails_to_org_admins(report_data, organization=None):
    """
    Send emails to organization admins with their stale datasets report.
    """
    # Group datasets by organization
    org_datasets = {}
    for row in report_data['table']:
        org_name = row['organization']
        if org_name not in org_datasets:
            org_datasets[org_name] = []
        org_datasets[org_name].append(row)
    
    emails_sent = 0
    
    for org_name, datasets in org_datasets.items():
        if organization and org_name != organization:
            continue
            
        # Count stale datasets for this organization
        stale_count = len([d for d in datasets if d['status'] == toolkit._('STALE')])
        
        # Skip organizations with no stale datasets
        if stale_count == 0:
            continue
            
        # Get organization admins
        org_admins = _get_org_admins(org_name)
        
        if org_admins:
            # Generate organization-specific report data
            org_report_data = {
                'table': datasets,
                'num_packages': len(datasets),
                'num_stale': stale_count,
                'stale_percentage': (stale_count / len(datasets)) * 100 if datasets else 0,
                'total_datasets': len(datasets),
            }
            
            # Send email to each admin
            for admin in org_admins:
                _send_report_email(
                    org_report_data, 
                    toolkit._('Μη Επικαιροποιημένα Σύνολα Δεδομένων για {}').format(org_name),
                    admin['email'], 
                    admin['name'],
                    org_name
                )
                emails_sent += 1
                
    return emails_sent


def _get_org_admins(org_name):
    """
    Get organization admins with their email addresses.
    """
    try:
        # Get organization
        org = model.Group.get(org_name)
        if not org:
            return []

        admins = []

        # Get email recipient configuration
        from ckan.common import config
        email_recipients = config.get('ckanext.report.email_recipients', 'admins')

        # Add organization email if configured
        if email_recipients in ['org', 'both']:
            org_email = org.extras.get('email') if hasattr(org, 'extras') else None
            if org_email:
                admins.append({
                    'name': org.title or org.name,
                    'email': org_email
                })

        # Get organization admins
        if email_recipients in ['admins', 'both']:
            # Try to get org admins using CKAN API
            context = {'model': model, 'session': model.Session, 'ignore_auth': True}
            try:
                members = get_action('member_list')(context, {
                    'id': org.id,
                    'object_type': 'user',
                    'capacity': 'admin'
                })

                for member in members:
                    user_id = member[0]
                    user = model.User.get(user_id)
                    if user and user.email:
                        admins.append({
                            'name': user.fullname or user.name,
                            'email': user.email
                        })
            except Exception:
                # Fallback: try to get admins directly from members
                members = model.Session.query(model.Member) \
                    .filter(model.Member.group_id == org.id) \
                    .filter(model.Member.table_name == 'user') \
                    .filter(model.Member.state == 'active')

                for member in members:
                    user = model.User.get(member.table_id)
                    if user and user.email:
                        admins.append({
                            'name': user.fullname or user.name,
                            'email': user.email
                        })

        return admins
    except Exception as e:
        print("Error getting organization admins: {}".format(str(e)))
        return []


def _send_report_email(report_data, subject, email, recipient_name, org_name=None):
    """
    Send the report email to a specific recipient.
    """
    from ckan.common import config
    
    # Create email body using simple string formatting
    email_body = _create_text_email_body(report_data, org_name)
    
    # Create HTML email body
    email_body_html = _create_html_email_body(report_data, org_name)
    
    # Get site settings
    site_title = config.get('ckan.site_title', 'CKAN')
    site_url = config.get('ckan.site_url', '')
    
    # Send email
    _mail_recipient(
        recipient_name,
        email,
        site_title,
        site_url,
        subject,
        email_body,
        body_html=email_body_html
    )


def _create_html_email_body(report_data, org_name=None):
    """
    Create HTML email body using template.
    """
    from jinja2 import Environment, FileSystemLoader
    import os
    from ckan.common import config
    
    site_url = config.get('ckan.site_url', '')
    report_url = '{}/report/stale-datasets'.format(site_url.rstrip('/'))
    
    # Prepare report data in the format expected by the template
    stale_datasets = [row for row in report_data['table'] if row['status'] == 'STALE']
    up_to_date_datasets = report_data['num_packages'] - report_data['num_stale']
    
    template_data = {
        'report': {
            'summary': {
                'total_datasets': report_data['num_packages'],
                'stale_datasets': report_data['num_stale'],
                'stale_percentage': report_data['stale_percentage'],
                'up_to_date_datasets': up_to_date_datasets
            },
            'stale_datasets': [
                {
                    'title': row['title'] or 'No title',
                    'last_updated': row['last_modified'],
                    'expected_frequency': row['frequency']
                }
                for row in stale_datasets
            ]
        },
        'org_name': org_name,
        'site_url': site_url,
        'site_title': config.get('ckan.site_title', 'CKAN'),
        'report_url': report_url
    }
    
    # Set up Jinja2 environment
    template_dir = os.path.join(os.path.dirname(__file__), 'templates')
    env = Environment(loader=FileSystemLoader(template_dir))
    
    # Render HTML template
    template = env.get_template('report/stale_datasets_report_email.html')
    email_body_html = template.render(**template_data)
    
    return email_body_html


def _create_text_email_body(report_data, org_name=None):
    """
    Create text email body using template.
    """
    from jinja2 import Environment, FileSystemLoader
    import os
    from ckan.common import config
    
    site_url = config.get('ckan.site_url', '')
    report_url = '{}/report/stale-datasets'.format(site_url.rstrip('/'))
    
    # Prepare report data in the format expected by the template
    stale_datasets = [row for row in report_data['table'] if row['status'] == 'STALE']
    up_to_date_datasets = report_data['num_packages'] - report_data['num_stale']
    
    template_data = {
        'report': {
            'summary': {
                'total_datasets': report_data['num_packages'],
                'stale_datasets': report_data['num_stale'],
                'stale_percentage': report_data['stale_percentage'],
                'up_to_date_datasets': up_to_date_datasets
            },
            'stale_datasets': [
                {
                    'title': row['title'] or 'No title',
                    'last_updated': row['last_modified'],
                    'expected_frequency': row['frequency']
                }
                for row in stale_datasets
            ]
        },
        'org_name': org_name,
        'site_url': site_url,
        'site_title': config.get('ckan.site_title', 'CKAN'),
        'report_url': report_url
    }
    
    # Set up Jinja2 environment
    template_dir = os.path.join(os.path.dirname(__file__), 'templates')
    env = Environment(loader=FileSystemLoader(template_dir))
    
    # Render text template
    template = env.get_template('report/stale_datasets_report_email.txt')
    email_body = template.render(**template_data)
    
    return email_body


def _display_report_summary(report_data):
    """
    Display a summary of the report to the console.
    """
    click.echo('Stale Datasets Report Summary')
    click.echo('=' * 30)
    click.echo('Total datasets checked: {}'.format(report_data['num_packages']))
    click.echo('Stale datasets: {} ({:.1f}%)'.format(
        report_data['num_stale'], 
        report_data['stale_percentage']
    ))
    click.echo('Up-to-date datasets: {}'.format(
        report_data['num_packages'] - report_data['num_stale']
    ))
    click.echo('Total datasets in scope: {}'.format(report_data['total_datasets']))