from exchangelib.protocol import BaseProtocol, NoVerifyHTTPAdapter
from exchangelib import (
    Credentials,
    Account,
    Message,
    Configuration,
    DELEGATE,
    FaultTolerance,
    HTMLBody,
    FileAttachment,
)
from ..config import config as grizly_config
import os
import logging
from typing import Union, List
from time import sleep
from ..utils.deprecation import deprecated_params
from functools import partial

deprecated_params = partial(deprecated_params, deprecated_in="0.4.1", removed_in="0.4.3")


class EmailAccount:
    @deprecated_params(params_mapping={"address": "auth_address"})
    def __init__(
        self, auth_address=None, password=None, alias=None, proxy=None, logger=None, **kwargs,
    ):
        self.logger = logger or logging.getLogger(__name__)
        config = grizly_config.get_service("email")
        self.address = (
            auth_address
            or config.get("auth_address")
            or config.get("address")
            or os.getenv("GRIZLY_EMAIL_ADDRESS")
        )
        self.password = password or os.getenv("GRIZLY_EMAIL_PASSWORD") or config.get("password")
        if None in (self.address, self.password):
            raise ValueError("Email address or password not found")
        self.alias = alias
        self.credentials = Credentials(self.address, self.password)
        self.config = Configuration(
            server="smtp.office365.com",
            credentials=self.credentials,
            retry_policy=FaultTolerance(max_wait=60),
        )
        self.proxy = (
            proxy
            or os.getenv("GRIZLY_PROXY")
            or os.getenv("HTTPS_PROXY")
            or grizly_config.get_service("proxies").get("https")
        )
        if self.proxy:
            os.environ["HTTPS_PROXY"] = self.proxy
        try:
            smtp_address = self.address
            if self.alias:
                smtp_address = self.alias
            self.account = Account(
                primary_smtp_address=smtp_address,
                credentials=self.credentials,
                config=self.config,
                autodiscover=False,
                access_type=DELEGATE,
            )
        except:
            self.logger.exception(
                f"Email account {self.address}, proxy: {self.proxy if self.proxy else ''} could not be accessed."
            )
            raise ConnectionError(
                "Connection to Exchange server failed. Please check your credentials and/or proxy settings"
            )

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}("{self.address}")'


class Email:
    """Class used to build and send email using Exchange Web Services (EWS) API.

    Parameters
    ----------
    subject : str
        Email subject
    body : str
        Email body
    attachment_path : str, optional
        Path to local file to be attached in the email , by default None
    logger : [type], optional
        [description], by default None
    is_html : bool, optional
        [description], by default False
    auth_address : str, optional
        Email address used to send an email, by default None
    password : str, optional
        Password to the email specified in address, by default None
    config_key : str, optional
        Config key, by default 'standard'"""

    @deprecated_params(params_mapping={"address": "auth_address"})
    def __init__(
        self,
        subject: str,
        body: str,
        attachment_paths: str = None,
        logger=None,
        is_html: bool = False,
        auth_address: str = None,
        password: str = None,
        proxy: str = None,
        **kwargs,
    ):
        self.subject = subject
        self.body = body if not is_html else HTMLBody(body)
        self.logger = logger or logging.getLogger(__name__)
        config = grizly_config.get_service("email")
        self.address = (
            auth_address
            or config.get("auth_address")
            or config.get("address")
            or os.getenv("GRIZLY_EMAIL_ADDRESS")
        )
        self.password = password or config.get("password") or os.getenv("GRIZLY_EMAIL_PASSWORD")
        self.attachment_paths = self.to_list(attachment_paths)
        self.attachments = self.get_attachments(self.attachment_paths)
        try:
            self.proxy = (
                proxy
                or os.getenv("HTTPS_PROXY")
                or grizly_config.get_service("proxies").get("https")
            )
        except:
            self.proxy = None

    def to_list(self, maybe_list: Union[List[str], str]):
        if isinstance(maybe_list, str):
            maybe_list = [maybe_list]
        return maybe_list

    def get_attachments(self, attachment_paths):

        if not attachment_paths:
            return None

        names = [os.path.basename(attachment_path) for attachment_path in attachment_paths]
        contents = [
            self.get_attachment_content(attachment_path, name)
            for attachment_path, name in zip(attachment_paths, names)
        ]
        attachments = [self.get_attachment(name, content) for name, content in zip(names, contents)]

        return attachments

    def get_attachment_content(self, attachment_path, attachment_name):
        """ Get the content of a file in binary format """

        image_formats = ["png", "jpeg", "jpg", "gif", "psd", "tiff"]
        doc_formats = ["pdf", "ppt", "pptx", "xls", "xlsx", "xlsm", "doc", "docx"]
        archive_formats = ["zip", "7z", "tar", "rar", "iso"]
        compression_formats = ["pkl", "gzip", "bz", "bz2"]
        binary_formats = image_formats + doc_formats + archive_formats + compression_formats
        text_formats = [
            "txt",
            "log",
            "html",
            "xml",
            "json",
            "py",
            "md",
            "ini",
            "yaml",
            "yml",
            "toml",
            "cfg",
            "csv",
            "tsv",
        ]

        attachment_format = attachment_name.split(".")[-1]
        if attachment_format in binary_formats:
            with open(attachment_path, "rb") as f:
                binary_content = f.read()
        elif attachment_format in text_formats:
            with open(attachment_path) as f:
                text_content = f.read()
                binary_content = text_content.encode("utf-8")
        else:
            raise NotImplementedError(
                f"Attaching files with {attachment_format} type is not yet supported.\n"
                f"Try putting the file inside an archive."
            )

        return binary_content

    def get_attachment(self, attachment_name, attachment_content):
        """ Returns FileAttachment object """
        return FileAttachment(name=attachment_name, content=attachment_content)

    def send(self, to: list, cc: list = None, send_as: str = None):
        """Sends an email

        Parameters
        ----------
        to : str or list
            Email recipients
        cc : str or list, optional
            Cc recipients, by default None
        send_as : str, optional
            Author of the email, by default None

        Examples
        --------
        >>> from grizly import get_path, Email
        >>> attachment_path = get_path("grizly_dev", "tests", "output.txt")
        >>> email = Email(subject="Test", body="Testing body.", attachment_paths=attachment_path, config_key="standard")
        >>> to = "test@example.com"
        >>> cc = ["test2@example.com", "test3@example.com"]
        >>> team_email_address = "shared_mailbox@example.com"
        >>> #email.send(to=to, cc=cc, send_as=team_email_address) #uncomment this line to send an email

        Returns
        -------
        None
        """

        BaseProtocol.HTTP_ADAPTER_CLS = (
            NoVerifyHTTPAdapter  # change this in the future to avoid warnings
        )

        if self.proxy:
            os.environ["HTTPS_PROXY"] = self.proxy

        to = to if isinstance(to, list) else [to]
        cc = cc if cc is None or isinstance(cc, list) else [cc]

        if not send_as:
            try:
                config = grizly_config.get_service("email")
                send_as = config.get("send_as") or config.get("address")
            except KeyError:
                pass
            send_as = self.address

        address = self.address
        password = self.password
        account = EmailAccount(address, password, logger=self.logger).account

        m = Message(
            account=account,
            subject=self.subject,
            body=self.body,
            to_recipients=to,
            cc_recipients=cc,
            author=send_as,
            folder=account.sent,
        )

        if self.attachments:
            for attachment in self.attachments:
                m.attach(attachment)

        try:
            m.send_and_save()
        except Exception:
            # retry once
            sleep(1)
            try:
                m.send_and_save()
            except:
                self.logger.exception(f"Email not sent.")
                raise

        self.logger.info("Email sent.")

        return None
