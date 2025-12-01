# encoding: utf-8

import click

# Define our own click_config_option to avoid dependency on paste
import click
import os

# Define our own load_config function
def load_config(config_path=None):
    if config_path is None:
        # Try to get the config path from the environment variable
        config_path = os.environ.get('CKAN_INI')

    if config_path is None:
        # Default config path
        config_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', '..', 'ckan.ini')

    # Load the config
    from ckan.config.environment import load_environment
    from ckan.config.middleware import make_app

    conf = make_app(config_path)
    load_environment(conf)
    return conf

# Define our own click_config_option
click_config_option = click.option(
    '-c', '--config',
    default=None,
    metavar='CONFIG',
    help='Config file to use (default: development.ini)')

from ckan.config.middleware import make_app
import ckan.plugins.toolkit as toolkit
from ckan import model
from ckan.lib.jobs import DEFAULT_QUEUE_NAME

from . import tasks


class CkanCommand(object):

    def __init__(self, conf=None):
        self.config = load_config(conf)
        self.app = make_app(self.config.global_conf, **self.config.local_conf)


@click.group()
@click.help_option(u'-h', u'--help')
@click_config_option
@click.pass_context
def cli(ctx, config, *args, **kwargs):
    ctx.obj = CkanCommand(config)


@cli.command(u'update-zip', short_help=u'Update zip file for a dataset')
@click.argument('dataset_ref')
@click.option(u'--synchronous', u'-s',
              help=u'Do it in the same process (not the worker)',
              is_flag=True)
def update_zip(dataset_ref, synchronous):
    u''' update-zip <package-name>

    Generates zip file for a dataset, downloading its resources.'''
    if synchronous:
        # Pass None as the user parameter since there's no user context in CLI commands
        tasks.update_zip(dataset_ref, None)
    else:
        toolkit.enqueue_job(
            tasks.update_zip, [dataset_ref, None, True],
            title=u'DownloadAll {operation} "{name}" {id}'.format(
                operation='cli-requested', name=dataset_ref,
                id=dataset_ref),
            queue=DEFAULT_QUEUE_NAME)
    click.secho(u'update-zip: SUCCESS', fg=u'green', bold=True)


@cli.command(u'update-all-zips',
             short_help=u'Update zip files for all datasets')
@click.option(u'--synchronous', u'-s',
              help=u'Do it in the same process (not the worker)',
              is_flag=True)
def update_all_zips(synchronous):
    u''' update-all-zips <package-name>

    Generates zip file for all datasets. It is done synchronously.'''
    context = {'model': model, 'session': model.Session}
    datasets = toolkit.get_action('package_list')(context, {})
    for i, dataset_name in enumerate(datasets):
        if synchronous:
            print('Processing dataset {}/{}'.format(i + 1, len(datasets)))
            # Pass None as the user parameter since there's no user context in CLI commands
            tasks.update_zip(dataset_name, None)
        else:
            print('Queuing dataset {}/{}'.format(i + 1, len(datasets)))
            toolkit.enqueue_job(
                tasks.update_zip, [dataset_name, None, True],
                title=u'DownloadAll {operation} "{name}" {id}'.format(
                    operation='cli-requested', name=dataset_name,
                    id=dataset_name),
                queue=DEFAULT_QUEUE_NAME)

    click.secho(u'update-all-zips: SUCCESS', fg=u'green', bold=True)
