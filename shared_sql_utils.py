from __future__ import annotations
from umc_models import AlertFilters

def format_cities_zips(cities: list[str], zips: list[str]) -> str:

    """
    Handles filtering by cities and zips in the case of each being
    present or each being null.
    """

    if cities and zips:
        return f"AND lm.city in {tuple(cities)} OR lm.zip in {tuple(zips)}"
    elif cities:
        return f"AND lm.city in {tuple(cities)}"
    elif zips:
        return f"AND lm.zip_code in {tuple(zips)}"
    else:
        return ""


def format_keywords(keywords: str) -> str:

    """
    Formats keywords for filtering in listing descriptions.
    """

    if keywords:
        keywords = keywords.split(',')
        keywords = [x.strip().lower() for x in keywords]
        keywords = [f"'%{x}%'" for x in keywords]
        sql = f"""
        AND description ILIKE ANY (array[{", ".join(keywords)}]) 
        """
        return sql
    else:
        return ""


def base_listings_cte(filters: AlertFilters) -> str:

    """
    Pass in AlertFilters to get the base listing events given the filters.
    Joins the listing_meta as well to get all rows needed for the further
    aggregations and calculations.
    """

    sql = f"""
    WITH base_listings AS (
            SELECT DISTINCT ON (mls_number, price) mls_number,
            date_listed::text, price, event_date::text, beds, baths, street_address, city, sq_ft, year_built, price_per_sq_ft,
            images, property_type, seller_motivation, num_kitchens, status, days_on_market, description, url, active,
            (CURRENT_DATE - date_listed::date) AS current_days_on_market
            FROM listing_events
            JOIN listing_meta lm USING (mls_number)
            WHERE lm.active IS TRUE
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
            {f"AND price_per_sq_ft >= {filters.min_price_per_sq_ft}" if filters.min_price_per_sq_ft else ""}
            {f"AND price_per_sq_ft <= {filters.max_price_per_sq_ft}" if filters.max_price_per_sq_ft else ""}
            {format_cities_zips(filters.cities, filters.zip_codes)}
            {f"AND lm.property_type in {tuple(filters.property_types)}" if filters.property_types else ""}
            {format_keywords(filters.keywords)}
            {f"AND num_kitchens >= {filters.num_kitchens}" if filters.num_kitchens else ""}
            {f"AND (CURRENT_DATE - date_listed::date) >= {filters.min_days_on_market}" if filters.min_days_on_market else ""}
            {f"AND (CURRENT_DATE - date_listed::date) <= {filters.max_days_on_market}" if filters.max_days_on_market else ""}
            ORDER BY mls_number, price, event_date DESC
            )
    """

    return sql

def price_lead_cte(cte_name: str = "price_lead") -> str:

    """
    Adds the price lead column using LEAD window function in order to track
    price changes.
    """

    sql = f"""
    {cte_name} AS (
            SELECT *,
            ROW_NUMBER() OVER (PARTITION BY mls_number ORDER BY event_date DESC) AS rn,
            LEAD(price) OVER (PARTITION BY mls_number ORDER BY event_date) AS new_price
            FROM base_listings
        )
    """
    return sql

def final_agg_cte(cte_name: str = "final_fields") -> str:

    """
    Gets a few final fields to include in the response rather than doing it on the FE.
    """

    sql = f"""
    {cte_name} AS (
            SELECT *,
            (new_price - price) as price_diff,
            MAX(ABS(new_price - price)) OVER (PARTITION BY mls_number) AS biggest_price_drop
            FROM price_lead
        )
    """
    return sql