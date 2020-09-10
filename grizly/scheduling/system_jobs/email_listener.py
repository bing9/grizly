from grizly import Job
from grizly.tools.email import EmailAccount
from exchangelib.errors import ErrorFolderNotFound
from datetime import date


def get_email_account(address=None, password=None, search_address=None, proxy=None):
    account = EmailAccount(address, password, alias=search_address, proxy=proxy,).account
    return account


def _validate_folder(account, folder_name):
    if not folder_name:
        return True
    try:
        folder = account.inbox / folder_name
    except ErrorFolderNotFound:
        raise


def get_last_email_date(
    account, notification_title, folder=None, search_address=None,
):
    _validate_folder(account, folder)
    last_message = None

    if folder:
        try:
            last_message = (
                account.inbox.glob("**/" + folder)
                .filter(subject=notification_title)
                .order_by("-datetime_received")
                .only("datetime_received")[0]
            )
        except IndexError:
            last_message = None
    else:
        try:
            last_message = (
                account.inbox.filter(subject=notification_title)
                .filter(subject=notification_title)
                .order_by("-datetime_received")
                .only("datetime_received")[0]
            )
        except IndexError:
            last_message = None

    if not last_message:
        return None

    d = last_message.datetime_received.date()
    last_received_date = date(d.year, d.month, d.day)

    return last_received_date


def listen(notification_title):
    # last_run = Job(job_name).last_run
    email_account = get_email_account(search_address="acoe_team@te.com")
    # previous_value = last_run.result[1] if last_run else None
    previous_value = 100500100900
    current_value = get_last_email_date(email_account, notification_title, folder="refresh_info")
    different = current_value == previous_value
    return different, current_value


email_title = "acoe_sales_daily_news"
sales_daily_news_job_name = "Sales Daily News Listener"
sales_daily_news_emea_job_name = "Sales Daily News EMEA Listener"
listen(notification_title)

sals_daily_news_emea_job = Job(sales_daily_news_emea_job_name).register(
    tasks=listener.listen, kwargs={"notification_title": "acoe_sales_daily_news_emea"}
)


class EmailListenerJob(Job):
    def __init__(self, address, title, folder: str = None, *args, **kwargs):
        self.address = address
        self.title = title
        self.folder = folder
        super().__init__(*args, **kwargs)

    @staticmethod
    def _validate_folder(account, folder_name):
        if not folder_name:
            return True
        try:
            account.inbox / folder_name
        except ErrorFolderNotFound:
            raise

    def get_last_email_date(self, account):
        self._validate_folder(account, self.folder)
        last_message = None

        if self.folder:
            try:
                last_message = (
                    account.inbox.glob("**/" + self.folder)
                    .filter(subject=self.title)
                    .order_by("-datetime_received")
                    .only("datetime_received")[0]
                )
            except IndexError:
                last_message = None
        else:
            try:
                last_message = (
                    account.inbox.filter(subject=self.title)
                    .filter(subject=self.title)
                    .order_by("-datetime_received")
                    .only("datetime_received")[0]
                )
            except IndexError:
                last_message = None

        if not last_message:
            return None

        d = last_message.datetime_received.date()
        last_received_date = date(d.year, d.month, d.day)

        return last_received_date

    def listen(self):
        last_run = self.last_run
        email_account = EmailAccount(alias=self.address).account
        previous_value = last_run.result[1] if last_run else None
        current_value = self.get_last_email_date(email_account)
        different = current_value == previous_value
        return different, current_value

    def register(self, *args, **kwargs):
        super().register(tasks=[self.listen], *args, **kwargs)


from grizly.scheduling.system_jobs import EmailListenerJob

sals_daily_news_listener = EmailListenerJob(
    "Sales Daily News Listener",
    address="acoeteam@te.com",
    folder="refresh_info",
    title="acoe_sales_daily_news",
)
sals_daily_news_listener.register()


# ------------------------------------------


class EmailListener:
    def __init__(self):
        self.store = self.con.hget("grizly:system:listener_store")

    def get_prev_value(self):
        key = _concat(args)
        prev_value = self.store.get(key)[-1]


from grizly.scheduling.system_jobs import EmailListener

sals_daily_news_listener = EmailListener(
    email_title="acoe_sales_daily_news", search_address="acoeteam@te.com"
)
assets = {RedisAsset("grizly:system:listener_store")}
sals_daily_news_listener_job = Job("Sales Daily News Listener").register(
    tasks=[sals_daily_news_listener.listen]
)
