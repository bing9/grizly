#!/usr/bin/env python3

import click

import grizly

from .config import config as _config
from .workflow import workflow as _workflow


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    """
    \b
    Commands:
        config      Manage configuration
        workflow    Run, schedule, and monitor workflows
    """
    pass


cli.add_command(_config)
cli.add_command(_workflow)

@cli.command(hidden=True)
def version():
    """Get the current grizly version"""
    click.echo(grizly.__version__)

