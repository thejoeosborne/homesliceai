import datetime

from postgres_utils import query_postgres_sql
from umc_models import AlertConfig
from openai_utils import get_motivation_detection

def get_cities_zips_string(cities: list[str], zips: list[str]) -> str:
    if cities and zips:
        return f"AND lm.city in {tuple(cities)} OR lm.zip in {tuple(zips)}"
    elif cities:
        return f"AND lm.city in {tuple(cities)}"
    elif zips:
        return f"AND lm.zip_code in {tuple(zips)}"
    else:
        return ""

def handler(event, context=None):

    fourty_five_minutes_ago = (datetime.datetime.utcnow() - datetime.timedelta(minutes=45)).strftime('%Y-%m-%d %H:%M:%S')
    kickoff_datetime = event.get('kickoff_datetime', fourty_five_minutes_ago)
    kickoff_date = str(kickoff_datetime)[:10]

    AlertConfig.update_forward_refs()
    config = AlertConfig(
        user_id=event.get('user_id'),
        email=event.get('email'),
        is_agent=event.get('is_agent'),
        filters=event.get('filters'),
    )

    filters = config.filters

    #TODO: build county config

    sql = f"""
        WITH lag_data AS (
        SELECT mls_number, event_date, price, days_on_market, status, sq_ft, beds, baths, year_built, price_per_sq_ft,
             LAG(price) OVER (PARTITION BY mls_number ORDER BY event_date) AS previous_price
        FROM listing_events
        WHERE event_date >= '{kickoff_date}'
        )
        
        SELECT 
        event_date, lm.description, lm.url, lm.property_type, mls_number,
        days_on_market, previous_price, price, lm.city, (previous_price - price) as price_diff
        FROM lag_data
        JOIN listing_meta lm USING (mls_number)
        WHERE
        event_date >= '{kickoff_datetime}'
        {f"AND price >= {filters.min_price}" if filters.min_price else ""}
        {f"AND price <= {filters.max_price}" if filters.max_price else ""}
        {f"AND sq_ft >= {filters.min_sq_ft}" if filters.min_sq_ft else ""}
        {f"AND sq_ft <= {filters.max_sq_ft}" if filters.max_sq_ft else ""}
        {f"AND beds >= {filters.min_beds}" if filters.min_beds else ""}
        {f"AND beds <= {filters.max_beds}" if filters.max_beds else ""}
        {f"AND baths >= {filters.min_baths}" if filters.min_baths else ""}
        {f"AND baths <= {filters.max_baths}" if filters.max_baths else ""}
        {f"AND year_built >= {filters.min_year_built}" if filters.min_year_built else ""}
        {f"AND year_built <= {filters.max_year_built}" if filters.max_year_built else ""}
        {f"AND days_on_market >= {filters.min_days_on_market}" if filters.min_days_on_market else ""}
        {f"AND days_on_market <= {filters.max_days_on_market}" if filters.max_days_on_market else ""}
        {f"AND price_per_sq_ft >= {filters.min_price_per_sq_ft}" if filters.min_price_per_sq_ft else ""}
        {f"AND price_per_sq_ft <= {filters.max_price_per_sq_ft}" if filters.max_price_per_sq_ft else ""}
        {f"AND (previous_price - price) >= {filters.price_reduction}" if filters.price_reduction else ""}
        {get_cities_zips_string(filters.cities, filters.zip_codes)}
        {f"AND lm.property_type in {tuple(filters.property_types)}" if filters.property_types else ""}
        AND status ilike '%active%';
    """

    listings = query_postgres_sql(sql)

    detection_obj = [{"mls_number": x["mls_number"], "description": x["description"]} for x in listings]
    motivation_detection = get_motivation_detection(detection_obj)

    for listing in listings:
        motivation_obj = [x for x in motivation_detection if x["mls_number"] == listing["mls_number"]][0]
        listing['seller_motivation_detected'] = motivation_obj['seller_motivation']

    return listings


if __name__ == '__main__':

    event = {
        'kickoff_datetime': '2023-07-19 22:00:29',
        'user_id': 1,
        'email': 'joerosborne@gmail.com',
        'is_agent': True,
        'filters': {
            'min_price': 400000,
            'max_price': 600000,
            'min_sq_ft': 1800,
            'max_sq_ft': None,
            'min_beds': 4,
            'max_beds': None,
            'min_baths': 2,
            'max_baths': None,
            'min_year_built': 1970,
            'max_year_built': None,
            'min_days_on_market': 30,
            'max_days_on_market': None,
            'min_price_per_sq_ft': None,
            'max_price_per_sq_ft': 300,
            'price_reduction': 10000,
            'cities': None,
            'zip_codes': None,
            'counties': None,
            'entire_state': False,
            'property_types': None,
            'seller_motivation_score': "MODERATE",
            'keywords': None,
            'enhance_keywords': False,
            'exclude_keywords': None,
        }
    }