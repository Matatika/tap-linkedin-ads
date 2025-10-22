"""Stream type classes for tap-linkedin-ads."""

from __future__ import annotations

import typing as t
from datetime import timezone
from importlib import resources

import pendulum
from singer_sdk.streams.core import REPLICATION_INCREMENTAL
from singer_sdk.typing import (
    IntegerType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

from tap_linkedin_ads.streams.ad_analytics.ad_analytics_base import AdAnalyticsBase
from tap_linkedin_ads.streams.streams import AccountsStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context

SCHEMAS_DIR = resources.files(__package__) / "schemas"
UTC = timezone.utc


class _AdAnalyticsByCampaignInit(AdAnalyticsBase):
    """https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting#analytics-finder."""

    name = "AdAnalyticsByCampaignInit"
    parent_stream_type = AccountsStream
    primary_keys: t.ClassVar[list[str]] = ["campaign_id", "day"]
    replication_method = REPLICATION_INCREMENTAL
    replication_key = "day"

    schema = PropertiesList(
        Property("campaign_id", IntegerType),
        Property("clicks", IntegerType),
        Property("costInLocalCurrency", StringType),
        Property("comments", IntegerType),
        Property(
            "dateRange",
            ObjectType(
                Property(
                    "end",
                    ObjectType(
                        Property("day", IntegerType),
                        Property("month", IntegerType),
                        Property("year", IntegerType),
                        additional_properties=False,
                    ),
                ),
                Property(
                    "start",
                    ObjectType(
                        Property("day", IntegerType),
                        Property("month", IntegerType),
                        Property("year", IntegerType),
                        additional_properties=False,
                    ),
                ),
            ),
        ),
        Property("day", StringType),
        Property("impressions", IntegerType),
        Property("landingPageClicks", IntegerType),
        Property("likes", IntegerType),
        Property("oneClickLeadFormOpens", IntegerType),
        Property("oneClickLeads", IntegerType),
        Property("otherEngagements", IntegerType),
        Property("shares", IntegerType),
        Property("totalEngagements", IntegerType),
    ).to_dict()

    @property
    def adanalyticscolumns(self) -> list[str]:
        return [
            "dateRange,clicks,shares,landingPageClicks,comments,costInLocalCurrency,impressions,likes,oneClickLeadFormOpens,oneClickLeads,otherEngagements,totalEngagements,pivotValues",
        ]

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        return {
            "q": "analytics",
            **super().get_url_params(context, next_page_token),
        }
    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config["user_agent"]
        headers["LinkedIn-Version"] = "202501" #convert to config
        headers["Content-Type"] = "application/json"
        headers["X-Restli-Protocol-Version"] = "2.0.0"

        return headers

    def get_unencoded_params(self, context: Context) -> dict:
        """Return a dictionary of unencoded params.

        Args:
            context: The stream context.

        Returns:
            A dictionary of URL query parameters.
        """
        start_date = pendulum.parse(self.config["start_date"])
        end_date = pendulum.parse(self.config["end_date"])
        return {
            "pivot": "(value:CAMPAIGN)",
            "timeGranularity": "(value:DAILY)",
            "accounts": (
                f"List(urn%3Ali%3AsponsoredAccount%3A{context['account_id']})"
            ),
            "dateRange": (
                f"(start:(year:{start_date.year},month:{start_date.month},day:{start_date.day}),"
                f"end:(year:{end_date.year},month:{end_date.month},day:{end_date.day}))"
            ),
            "fields": self.adanalyticscolumns[0],
        }
    def post_process(self, row: dict, context: dict | None = None) -> dict | None:

        pivot_value = row.pop("pivotValues", None)
        if isinstance(pivot_value, list):
            pivot_value = pivot_value[0] if pivot_value else None

        if pivot_value:
            row["campaign_id"] = int(pivot_value.split(":")[-1])

        return super().post_process(row, context)


class AdAnalyticsByCampaignStream(_AdAnalyticsByCampaignInit):
    """https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting#analytics-finder."""

    name = "ad_analytics_by_campaign"

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a dictionary of records from adAnalytics classes.

        Combines request columns from multiple calls to the api, which are limited to 20
        columns each.

        Uses `merge_dicts` to combine responses from each class
        super().get_records calls only the records from the adAnalyticsByCampaign class
        zip() Iterates over the records of adAnalytics classes and merges them with
        merge_dicts() function list() converts each stream context into lists

        Args:
            context: The stream context.

        Returns:
            A dictionary of records given from adAnalytics streams
        """
        adanalyticsinit_stream = _AdAnalyticsByCampaignInit(
            self._tap,
            schema={"properties": {}},
        )
        return [
            self.merge_dicts(x, y)
            for x, y in zip(
                list(adanalyticsinit_stream.get_records(context)),
                list(super().get_records(context)),
            )
        ]
