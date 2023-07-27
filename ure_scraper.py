import re
import json
import time
import pandas as pd
import requests
import traceback
import datetime
from bs4 import BeautifulSoup
from openai_utils import get_motivation_detection
from umc_logging import listing_logger, CustomErrors, ChunkLog
from postgres_utils import query_postgres_sql, get_engine
from umc_utils import get_now_mountain_time, thread_items, write_json_to_s3
from umc_models import ListingEvent, ListingMeta

LOG_BUCKET = 'ure-collection'
TODAY = datetime.datetime.utcnow().strftime('%Y-%m-%d')

def regex_find(pattern, text, strip=False, mega_strip=False):
    try:
        value = re.findall(pattern, text)[0]
        if strip:
            value = value.strip()
        if mega_strip:
            value = ''.join(value.split())
    except:
        value = None
    return value


@listing_logger
def scrape_property(mls_number) -> dict:

    url = f'https://utahrealestate.com/{mls_number}'
    headers = {'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'}
    res = requests.get(url, headers=headers)
    soup = BeautifulSoup(res.text, 'html.parser')

    overview = soup.find('div', attrs={'class': 'prop___overview'})
    street_address = overview.findChild('h2').text if overview is not None else None

    if not street_address:
        address_check = regex_find('See Directions For Address Info', str(soup))
        if address_check:
            street_address = address_check

    # unlisted/inactive properties
    if not overview and not street_address:
        raise CustomErrors.EmptyListing

    city = regex_find('\n(.*),\s[UTID]', overview.text) if overview is not None else None
    state = regex_find('\,\s([A-Z]{2})', overview.text) if overview is not None else None

    if state == 'ID':
        raise CustomErrors.IdahoListing

    zip_code = regex_find(',\s[UTID]{2}\s(\d*)', overview.text) if overview is not None else None
    description = soup.find('meta', attrs={'property': 'og:description'})['content']

    main_image = soup.find('meta', attrs={'property': 'og:image'})['content']
    images = [x.get('src') for x in soup.select('div.image___gallery__photo___wrap img')]

    if main_image in images:
        images.remove(main_image)

    images.insert(0, main_image)

    details = soup.find('ul', attrs={'class': 'prop-details-overview'})
    price = regex_find('\$([\d\,]*)', details.text).replace(',', '')
    bed_count = regex_find('(\d*)\sBeds', details.text)
    bath_count = regex_find('(\d*)\sBath', details.text)
    square_feet = regex_find('(\d*)\sSq\.', details.text)

    # HomeDetails
    facts = soup.find('ul', attrs={'class': 'facts___list___items'})
    fact_details = facts.findChildren('div', attrs={'class': 'facts___item'})
    details = {}
    for attribute in fact_details:
        header = attribute.find('span').text
        value = ''.join(attribute.find('div').text.replace(header, '').split())
        if value == 'JustListed':
            value = '0'
        details[header.lower().replace(' ', '_').replace('#', '_num').replace(':', '')] = value

    # HomeFeatures
    features = soup.find('div', attrs={'class': 'features-wrap'})
    headers = features.find_all('h4')
    feature_obj = {}
    for feature_header in headers:
        feature_soup = feature_header.find_next_sibling()
        if '<li' in str(feature_soup):
            feature_data = feature_soup.text[1:-1].replace('\n', ',').replace(',,', ',')
        elif '<p' in str(feature_soup):
            feature_data = feature_soup.text
        else:
            print(f'couldnt find feature data check html tags {str(feature_soup)}')
        feature_obj[feature_header.text.lower().replace(' ', '_').replace('#', '_num').replace(':', '').replace("'","")] = feature_data

    days_on_market = int(details['days_on_ure'])
    status = details['status'].lower()
    property_type = details['type']
    property_style = details['style']
    year_built = int(details['year_built'])

    now_mountain_time = get_now_mountain_time()
    date_listed = now_mountain_time - datetime.timedelta(days=days_on_market)
    date_listed = date_listed.strftime('%Y-%m-%d')

    event_date = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    price_per_sq_ft = None
    if square_feet:
        square_feet = int(square_feet)
        if square_feet != 0:
            price_per_sq_ft = round(int(price) / int(square_feet), 2)

    listing_event = ListingEvent(
        mls_number=mls_number,
        price=price,
        sq_ft=square_feet,
        price_per_sq_ft=price_per_sq_ft,
        days_on_market=days_on_market,
        status=status,
        beds=bed_count,
        baths=bath_count,
        year_built=year_built,
        event_date=event_date,
    )

    listing_meta = ListingMeta(
        mls_number=mls_number,
        url=url,
        street_address=street_address,
        city=city,
        state=state,
        zip_code=zip_code,
        images=json.dumps(images),
        property_type=property_type,
        property_style=property_style,
        description=description,
        features=json.dumps(feature_obj),
        date_listed=date_listed,
    )

    return {'listing_event': listing_event, 'listing_meta': listing_meta}


def get_existing_listing_meta(mls_numbers: list[str]) -> pd.DataFrame:

    formatted_mls_numbers = ', '.join([f"'{x}'" for x in mls_numbers])
    sql = f"""
    SELECT DISTINCT
        mls_number
    FROM
        "listing_meta"
    WHERE
        mls_number IN ({formatted_mls_numbers})
    """
    mls_nums = query_postgres_sql(sql, return_dataframe=False)
    mls_nums = [x.get('mls_number') for x in mls_nums]

    return mls_nums

def apply_seller_motivation(df_meta: pd.DataFrame) -> pd.DataFrame:

    try:

        df_description = df_meta[["mls_number", "description"]]
        records = df_description.to_dict('records')

        motivation_records = []

        # loop through records in chunks of 5
        for i in range(0, len(records), 5):
            chunk = records[i:i + 5]
            motivation_obj = get_motivation_detection(chunk)
            motivation_records.extend(motivation_obj)

        df_motivation = pd.DataFrame(motivation_records)
        # merge df_meta and df_motivation
        df_meta = df_meta.merge(df_motivation, on='mls_number', how='left')

    except Exception as e:
        print(e)

    return df_meta


def handler(event, context):

    start_time = time.time()
    today = datetime.datetime.utcnow().strftime('%Y-%m-%d')
    mls_numbers = event.get('mls_numbers', [])

    try:
        scraped_data_objects = thread_items(mls_numbers, scrape_property, max_workers=20)
        scraped_data = [x for x in scraped_data_objects if 'failed_scrape' not in x]

        # log failed scrapes
        failed_logs = [x.get('failed_scrape') for x in scraped_data_objects if 'failed_scrape' in x]
        now = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
        write_json_to_s3(
            key=f"listing_logs/{TODAY}/fail/{now}.json",
            bucket=LOG_BUCKET,
            data=failed_logs
        )

        num_meta_written = 0
        num_events_written = 0
        if scraped_data:
            # write listing meta first since there is a foreign key constraint
            existing_listing_meta = get_existing_listing_meta(mls_numbers)
            df_meta = pd.DataFrame([x.get('listing_meta').to_dict() for x in scraped_data])
            df_meta = df_meta[~df_meta['mls_number'].isin(existing_listing_meta)]

            # Run through gpt to get a bool for seller motivation in chunks of 5
            df_meta = apply_seller_motivation(df_meta)

            engine = get_engine()
            num_meta_written = df_meta.to_sql('listing_meta', engine, if_exists='append', index=False, method='multi')

            # write all ListingEvents to listing_events table
            df_events = pd.DataFrame([x.get('listing_event').to_dict() for x in scraped_data])
            num_events_written = df_events.to_sql('listing_events', engine, if_exists='append', index=False, method='multi')

        log = ChunkLog(
            mls_numbers=mls_numbers,
            success=True,
            chunk_duration=time.time() - start_time,
            num_mls_numbers_received=len(mls_numbers),
            num_listing_events_written=num_events_written,
            num_listing_meta_written=num_meta_written,
        )
        log.log()

        # Write log to s3
        log_object = log.__dict__
        now = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
        key = f"chunk_logs/{today}/success/{now}.json"
        res = write_json_to_s3(key, LOG_BUCKET, log_object)


    except Exception as err:

        log = ChunkLog(
            mls_numbers=mls_numbers,
            success=False,
            chunk_duration=round(time.time() - start_time, 3),
            num_mls_numbers_received=len(mls_numbers),
            error_type=type(err).__name__,
            error_msg=str(err),
            stack_trace=traceback.format_exc()
        )
        log.log()

        # Write log to s3
        log_object = log.__dict__
        now = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
        key = f"chunk_logs/{today}/fail/{now}.json"
        res = write_json_to_s3(key, LOG_BUCKET, log_object)

if __name__ == '__main__':

    mls_numbers = ['1889816']
    event = {'mls_numbers': mls_numbers}
    #handler(event, None)