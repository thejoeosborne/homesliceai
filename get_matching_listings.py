from __future__ import annotations
import datetime
from postgres_utils import query_postgres_sql
from umc_models import AlertFilters, GoodApiResponse, SellerMotivationScore
from shared_sql_utils import (
    base_listings_cte,
    price_lead_cte,
    final_agg_cte
)

TODAY = datetime.datetime.now().date().strftime('%Y-%m-%d')

def handler(event: dict, context=None) -> list[dict] | dict:

    """
    /alerts/{alert_id}?user_id={}&email={}&page={}
    GetMatchingListings

    Gets listings that match given alert filters. Assign
    seller motivation scores, and mark new listings. Organize all
    price change events under each listing.
    """

    # Wrap whole function in try catch for debugging
    try:

        query_params = event.get('queryStringParameters')
        path_params = event.get('pathParameters')

        # Grab params from event. Page size hard coded to 500 for now
        alert_id = path_params.get('alert_id')
        user_id = query_params.get('user_id')
        email = query_params.get('email')
        page = int(query_params.get('page', 1))
        page_size = 500

        # Get alert filters for given alert id. Use email if needed
        # when the user is coming from an email referral
        sql = f"""select * from report_recipients where id = {alert_id}
              and (owner_id = '{user_id}' or recipient_email = '{email}')
              """
        filters = query_postgres_sql(sql, return_dataframe=False)

        # Account for missing filters, possible edge case.
        if not filters:
            res = GoodApiResponse(
                status_code=404,
                body={'err': 'No alert found with given id'}
            )
            return res.get_response()

        # Create AlertFilter object for data validation and type checking
        base_filters = filters[0]
        filters = AlertFilters(
            min_price=base_filters.get('min_price'),
            max_price=base_filters.get('max_price'),
            min_sq_ft=base_filters.get('min_sq_ft'),
            max_sq_ft=base_filters.get('max_sq_ft'),
            min_beds=base_filters.get('min_beds'),
            max_beds=base_filters.get('max_beds'),
            min_baths=base_filters.get('min_baths'),
            max_baths=base_filters.get('max_baths'),
            min_year_built=base_filters.get('min_year_built'),
            max_year_built=base_filters.get('max_year_built'),
            min_days_on_market=base_filters.get('min_days_on_market'),
            max_days_on_market=base_filters.get('max_days_on_market'),
            min_price_per_sq_ft=base_filters.get('min_price_per_sq_ft'),
            max_price_per_sq_ft=base_filters.get('max_price_per_sq_ft'),
            price_reduction=base_filters.get('price_reduction'),
            cities=base_filters.get('cities'),
            zip_codes=base_filters.get('zip_codes'),
            counties=base_filters.get('counties'),
            entire_state=base_filters.get('entire_state'),
            property_types=base_filters.get('property_types'),
            seller_motivation_scores=base_filters.get('seller_motivation_scores'),
            keywords=base_filters.get('keywords'),
            enhance_keywords=base_filters.get('enhance_keywords'),
            exlucde_keywords=base_filters.get('exclude_keywords'),
            num_kitchens=base_filters.get('num_kitchens'),
        )

        # Build metadata to add into response later
        filter_meta = {
            'filter_id': base_filters.get('id'),
            'owner_id': base_filters.get('owner_id'),
            'recipient_email': base_filters.get('recipient_email'),
            'owner_email': base_filters.get('owner_email'),
            'nickname': base_filters.get('nickname'),
            **filters.__dict__
        }

        # Build sql query with shared sql functions
        sql = f"""
        {base_listings_cte(filters)},
        {price_lead_cte()},
        {final_agg_cte()}
        SELECT * FROM final_fields
        WHERE active IS TRUE
        {f"AND biggest_price_drop >= {filters.price_reduction}" if filters.price_reduction else ""}
        {f"OFFSET {(page - 1) * page_size} LIMIT {page_size}"};
        """

        # Grab the listings
        data = query_postgres_sql(sql, return_dataframe=False)

        # Print out sql to use for debugging
        print(sql)

        # Nest price change events within each listing
        data = nest_events(data, min_days_on_market=filters.min_days_on_market)

        # Put the 'new' items in data at the start, otherwise keep the same order
        new_items = [x for x in data if x['new'] is True]
        old_items = [x for x in data if x['new'] is False]
        data = new_items + old_items

        # Combine metadata with results for the final object
        final_obj = {
            **filter_meta,
            'num_results': len(data),
            'results': data,
        }

        res = GoodApiResponse(
            status_code=200,
            body=final_obj,
        )

    except Exception as e:

        res = GoodApiResponse(
            status_code=500,
            body={'err': str(e)}
        )


    return res.get_response()


def nest_events(data: list[dict], min_days_on_market: int | None) -> list[dict]:

    """
    Nests price change events within each listing
    Applies a 'new' flag to each listing if there are new events
    Adds seller motivation fields to each listing
    """

    base_meta = [x for x in data if x['rn'] == 1]
    for listing in base_meta:

        mls_num = listing['mls_number']

        listing['events'] = []
        extra_events = [
            x for x in data
            if x['mls_number'] == mls_num
            and x['price_diff'] is not None
            and x['price_diff'] != 0.0
        ]

        # Check for events that are new today
        listing['new'] = False
        all_dates = [y['event_date'][:10] for y in extra_events]
        if TODAY in all_dates:
            listing['new'] = True

        # Check for brand-new listings or listings that just matched the days filters
        if (
            listing['current_days_on_market'] == 0
            or listing['current_days_on_market'] == min_days_on_market
        ):
            listing['new'] = True

        # Nest events within each listing
        for event in extra_events:
            event_obj = {
                'mls_number': event['mls_number'],
                'event_date': event['event_date'],
                'new_price': event['new_price'],
                'old_price': event['price'],
                'price_diff': event['price_diff'],
            }
            listing['events'].append(event_obj)

        # Get the seller motivation score
        listing['seller_motivation_score'] = seller_motivation_score(listing)

        # Order events by date within each date
        listing['events'] = sorted(listing['events'], key=lambda k: k['event_date'], reverse=True)

    return base_meta

def seller_motivation_score(listing: dict) -> SellerMotivationScore:

    score = 0

    # Get the gpt3.5 rated score based on description
    if listing.get('seller_motivation') is True:
        score += 4

    # Account for days on market
    if 90 < listing.get('current_days_on_market') < 180:
        score += 3
    elif listing.get('current_days_on_market') > 60:
        score += 2
    elif listing.get('current_days_on_market') > 30:
        score += 1

    # Account for the number of price drops
    events = listing.get('events')
    num_price_drops = [x for x in events if x.get('price_diff') < 0]

    if len(num_price_drops) == 1:
        score += 1
    elif len(num_price_drops) == 2:
        score += 2
    elif len(num_price_drops) > 2:
        score += 3

    # Assign score
    if score >= 6:
        return "High"
    elif score >= 3:
        return "Moderate"
    else:
        return "Undetected"



if __name__ == '__main__':

    event = {
        "queryStringParameters": {
            "user_id": "ccfde85a-1a1f-40cb-89c3-ec53cdb48c5b",
            "email": None
        },
        "pathParameters": {
            "alert_id": "22"
        }
    }

    run = handler(event, None)
    import json
    body = json.loads(run['body'])

