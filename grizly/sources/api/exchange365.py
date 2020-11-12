from __future__ import annotations

import exchangelib
import datetime

import pytz

from ...tools.email import EmailAccount
from ..base import BaseReadSource
from ...exceptions import MessageNotFound


class Exchange365(BaseReadSource):
    def __init__(
        self,
        address: str = None,
        password: str = None,
        auth_address: str = None,
        proxy: str = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.address = address
        self.password = password
        self.auth_address = auth_address or address
        self.proxy = proxy
        self._con = None

    @property
    def con(self):
        if self._con is None:
            self._con = EmailAccount(
                address=self.auth_address,
                password=self.password,
                alias=self.address,
                proxy=self.proxy,
            ).account
        return self._con

    def copy_object(self):
        pass

    def delete_object(self):
        pass

    def create_object(self):
        pass

    @property
    def objects(self):
        pass

    def object(self, name, *args, **kwargs):
        return self.message(subject=name, *args, **kwargs)

    def message(self, subject: str = None, folder: str = None) -> exchangelib.Message:
        """Retrieve a message by subject"""
        try:
            if folder:
                query_set = (
                    self.con.inbox.glob("**/" + folder)
                    .filter(subject=subject)
                    .order_by("-datetime_received")
                )
            else:
                query_set = self.con.inbox.filter(subject=subject).order_by("-datetime_received")
        except IndexError as e:
            msg = f"Message with the title '{subject}' was not found in {folder}"
            raise MessageNotFound(msg) from e

        messages = list(query_set)

        if len(messages) > 1:
            msg = f"Multiple messages with the title '{subject}' found. Returning the latest one."
            self.logger.warning(msg)

        # convert from EWSDate(?) to regular one
        message = messages[0]
        dt = message.datetime_received
        dt_standard = datetime.datetime(
            dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, tzinfo=pytz.UTC
        )
        message.datetime_received = dt_standard

        return message

    def _validate_folder(self, folder_name):
        self.con.inbox / folder_name
