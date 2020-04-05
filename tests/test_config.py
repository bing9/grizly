from ..grizly.config import Config


data = {
  "config": {
    "standard": {
      "email": {
        "email_address": "my_email@example.com",
        "email_password": "my_password",
        "send_as": "Team"
      },
      "github": {
        "username": "my_login",
        "username_password": "my_password",
        "pages": 100,
        "proxies":  {
          "http": "http://restrictedproxy.tycoelectronics.com:80",
          "https": "https://restrictedproxy.tycoelectronics.com:80",
        }
      },
      "sfdc": {
        "stage": {
          "username": "my_login",
          "instance_url": "https://na1.salesforce.com",
          "password": "my_password",
          "organizationId": "OrgId"
        },
        "prod": {
          "username": "my_login",
          "password": "my_password",
          "organizationId": "OrgId"
        }
      }
    }
  }
}

def test_config_from_dict():
    Config().from_dict(data)
    assert Config.data == data['config']
