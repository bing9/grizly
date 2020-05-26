import click
import json
import os


def set_email(email_address, email_password, config_file):
    with open(config_file, "r") as f:
        config = json.load(f)
        if not email_address:
            email_address = config["config"]["standard"]["email"]["email_address"]
        email_address = click.prompt("Please enter your email address", default=email_address)
        config["config"]["standard"]["email"]["email_address"] = email_address
        email_password = click.prompt("Please enter your email password", default=email_password)
    with open(config_file, "w") as f:
        json.dump(config, f, indent=4)



@click.command(hidden=True)
@click.option("--global", "_global", help="Global settings", is_flag=True)
@click.option("--email_address", "-e", help="Email address from which notifications will be sent", type=str)
# @click.password_option("--email_password", "-p", hide_input=True, confirmation_prompt=True, help="Email password", type=str)
@click.option("--email_password", "-p", hide_input=True, confirmation_prompt=True, help="Email password", type=str)
def config(_global, email_address, email_password):
    """Set up or modify grizly config"""
    print("Welcome to grizly configuration")

    if _global:
        config_file = os.path.expanduser("~/.grizly/config.json")
    else:
        config_file = "config.json"

    if os.path.exists(config_file):
        set_email(email_address, email_password, config_file)
    else:
        raise NotImplementedError("Creating a new config file is not yet supported")

    print("Successfully configured grizly")

