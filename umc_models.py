from __future__ import annotations
import json
import math
import datetime
import pydantic
import pandas as pd
from dateutil import tz
from typing import Union, Literal
from enum import Enum, unique


def get_now_mountain_time(offset_days=0):
    from_zone = tz.tzutc()
    to_zone = tz.gettz('America/Denver')

    utc = datetime.datetime.utcnow()
    utc = utc.replace(tzinfo=from_zone)
    mountain_time = utc.astimezone(to_zone)

    if offset_days != 0:
        mountain_time = mountain_time + datetime.timedelta(days=offset_days)

    return mountain_time


@unique
class ClassName(Enum):
    RateScrape = 'RateScrape'
    ApiResponse = 'ApiResponse'
    HomeScrape = 'HomeScrape'
    AgentBrokerScrape = 'AgentBrokerScrape'
    DailyStats = 'DailyStats'
    HomePriceChange = 'HomePriceChange'
    RealtorLead = 'RealtorLead'


def nan_to_none(obj):
    """
    convert nan into None
    """
    if isinstance(obj, dict):
        return {k: nan_to_none(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [nan_to_none(v) for v in obj]
    if isinstance(obj, float) and math.isnan(obj):
        return None
    return obj


def empty_string_to_none(obj):
    """
    Cool recursion to deep replace all empty strings to NoneType in dicts or lists.
    Could probably add some logic to do it for classes, but probably more correct to do that
    inside the class definition instead.
    """

    if isinstance(obj, dict):
        return {k: empty_string_to_none(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [empty_string_to_none(v) for v in obj]
    if isinstance(obj, str) and obj == "":
        return None
    return obj


class ResponseEncoder(json.JSONEncoder):
    """
    custom encoder to fix issues in data
    """

    def encode(self, obj, *args, **kwargs):
        obj = nan_to_none(obj)
        return super().encode(obj, *args, **kwargs)

    def iterencode(self, obj, *args, **kwargs):
        obj = nan_to_none(obj)
        return super().iterencode(obj, *args, **kwargs)


class RateScrape:
    def __init__(
            self,
            thirty_year_rate: float = None,
            twenty_year_rate: float = None,
            fifteen_year_rate: float = None,
            id: int = None,
    ):
        self.thirty_year_rate = thirty_year_rate
        self.twenty_year_rate = twenty_year_rate
        self.fifteen_year_rate = fifteen_year_rate
        self.id = id

    def to_dict(self) -> dict:
        return self.__dict__


class ApiResponse:
    def __init__(self, status_code: int, status_message: str, body: Union[dict, list]):
        self.status_code = status_code
        self.status_message = status_message
        self.body = body

    def to_dict(self) -> dict:
        return self.__dict__


class GoodApiResponse:

    def __init__(self, status_code: int, body: Union[dict, list]):
        self.status_code = status_code
        self.body = body

    def get_response(self) -> dict:
        return {
            'statusCode': self.status_code,
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
                "content-type": "application/json",
            },
            'body': json.dumps(self.body, cls=ResponseEncoder)
        }

    def __repr__(self):
        return f'UmcApiResponse'



class RealtorLead:

    def __init__(
            self,
            name: str = None,
            email: str = None,
            phone: str = None,
            city: str = None,
            income: str = None,
            selected_realtor_id: str = None,
            selected_realtor_name: str = None,
            selected_realtor_email: str = None,
            selected_realtor_phone: str = None,
    ):
        self.name = name
        self.email = email
        self.phone = phone
        self.city = city
        self.income = income
        self.date_submitted = str(get_now_mountain_time())
        self.selected_realtor_id = selected_realtor_id
        self.selected_realtor_name = selected_realtor_name
        self.selected_realtor_email = selected_realtor_email
        self.selected_realtor_phone = selected_realtor_phone

    def to_dict(self) -> dict:
        return self.__dict__

    @classmethod
    def from_dict(cls, d):
        return cls(**d)

    def __repr__(self):
        return f'RealtorLead - {self.name}'


class ListingEvent(pydantic.BaseModel):
    mls_number: str
    price: float
    sq_ft: int
    price_per_sq_ft: float = None
    days_on_market: int
    status: str
    beds: int
    baths: int
    year_built: int
    event_date: str # %Y-%m-%d %H:%M:%S

    def __repr__(self):
        return f'<ListingEvent> - {self.mls_number}'

    def to_dict(self):
        return self.__dict__

class ListingMeta(pydantic.BaseModel):
    mls_number: str
    url: str
    street_address: str = None
    city: str = None
    state: str = None
    zip_code: str = None
    images: list = None
    property_type: str = None
    property_style: str = None
    description: str = None
    features: str = None #json.dumps
    date_listed: str = None # %Y-%m-%d
    num_kitchens: int = None


    def __repr__(self):
        return f'<ListingMeta> - {self.mls_number}'

    def to_dict(self):
        return self.__dict__


class ReportRecipient(pydantic.BaseModel):
    owner_id: str
    owner_email: str
    recipient_email: str
    recipient_first_name: str
    recipient_last_name: str
    cadence: str
    cities: str = None #json.dumps list[str]
    zip_codes: str = None #json.dumps list[str]
    entire_state: bool
    active: bool
    min_price: float = None
    max_price: float = None
    min_days_on_market: int = None
    max_days_on_market: int = None
    min_beds: int = None
    max_beds: int = None
    min_baths: int = None
    max_baths: int = None
    min_sq_ft: int = None
    max_sq_ft: int = None
    min_price_per_sq_ft: float = None
    max_price_per_sq_ft: float = None
    min_year_built: int = None
    max_year_built: int = None
    price_reduction: float = None
    keywords: str = None #comma separated string
    enhance_keywords: bool = None
    exclude_keywords: str = None #comma separated string
    property_types: list = None
    counties: list = None
    num_kitchens: int = None
    nickname: str = None


    def to_dict(self):
        return self.__dict__


class RentalListingEvent(pydantic.BaseModel):
    listing_id: str
    property_id: str
    event_date: str
    price: float
    sq_ft: int = None
    price_per_sq_ft: float = None
    year_built: int = None
    beds: int = None
    baths: int = None

    def to_dict(self):
        return self.__dict__

class RentalListingMeta(pydantic.BaseModel):
    listing_id: str
    property_id: str
    image_urls: list = None
    primary_image: str = None
    property_type: str = None
    street_address: str = None
    city: str = None
    state: str = None
    zip_code: str = None
    available_date: str = None
    available_now: bool = None
    listing_url: str = None
    contact_name: str = None
    contact_email: str = None
    contact_phone: str = None
    contact_sms: str = None
    pets_allowed: bool = None
    min_lease_length: int = None
    max_lease_length: int = None
    deposit_total: float = None
    deposit_refundable: float = None
    utilities: list = None
    property_amenities: list = None
    community_amenities: list = None

    def to_dict(self):
        return self.__dict__


SellerMotivationScore = Literal["High", "Moderate", "Undetected"]
class AlertFilters(pydantic.BaseModel):
    min_price: float = None
    max_price: float = None
    min_sq_ft: int = None
    max_sq_ft: int = None
    min_beds: int = None
    max_beds: int = None
    min_baths: int = None
    max_baths: int = None
    min_year_built: int = None
    max_year_built: int = None
    min_days_on_market: int = None
    max_days_on_market: int = None
    min_price_per_sq_ft: float = None
    max_price_per_sq_ft: float = None
    price_reduction: float = None
    num_kitchens: int = None
    cities: list = None
    zip_codes: list = None
    counties: list = None
    entire_state: bool = None
    property_types: list = None
    seller_motivation_score: SellerMotivationScore = None
    keywords: str = None
    enhance_keywords: bool = None
    exclude_keywords: list = None