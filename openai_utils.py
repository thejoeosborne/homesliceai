from __future__ import annotations
import json
import openai
from typing import TypedDict
from postgres_utils import query_postgres_sql

API_KEY = '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'
openai.api_key = API_KEY


class Listing(TypedDict):
    mls_number: str
    description: str

def get_motivation_detection(listings: list[Listing], model="gpt-3.5-turbo"):
    terms = [
        "motivated seller",
        "must sell",
        "price reduced",
        "quick sale",
        "priced to sell",
        "below market value",
        "must be sold",
        "moving out",
        "must relocate",
        "must downsize",
        "must liquidate",
        "must settle estate",
        "seller financing available",
        "flexible terms",
        "bring offers",
        "seller eager to close",
        "willing to negotiate",
        "time sensitive sale",
        "desperate to sell",
        "must move quickly",
        "relocating soon",
        "vacant property",
        "estate sale",
        "foreclosure",
        "divorce sale",
        "job transfer",
        "financial hardship",
        "investor opportunity",
        "fixer upper",
        "cash offers preferred",
        "closing cost assistance",
        "price dropped",
        "distressed property",
        "reduced for a fast sale",
        "highly motivated seller",
        "urgent sale",
        "serious inquiries only",
        "drastic price reduction",
        "moving must sell",
        "need to sell fast",
        "seller wants quick closing",
        "must sell by [specific date]",
        "selling below appraised value",
        "seller willing to pay closing costs"
    ]

    seller_terms = ", ".join(terms)

    prompt = f"""Pretend you are an expert in real estate and you are analyzing listing descriptions to detect seller motivation.
    You will be given a JSON array of listings in this format:
    [
    {{"mls_number": "1234567", "description": "This is the first description."}},
    {{"mls_number": "2345678", "description": "This is the second description."}},
    ]
    
    Your task as an expert is to read the description of each listing and determine if it indicates a motivated seller.
    To help with this task, here are a list of phrases you might see in a description that shows seller motivation: {seller_terms}
    Don't rely completely on the given terms, but use them as a guide.
    
    You will return TRUE if you find an indication of seller motivation, and FALSE if not. You will then return a JSON
    array of each listing, matched to the correct mls_number, with your given result as such:
    [
    {{"mls_number": "1234567", "seller_motivation": TRUE}},
    {{"mls_number": "2345678", "seller_motivation": FALSE}},
    ]
    Do not include any other text or language in your response besides the JSON array as shown above. If there is any other text in your response besides the JSON array, terrible things will happen.
    
    Here are the listings: {listings}
    
    Your response: 

    """

    messages = [{"role": "user", "content": prompt}]

    completion = openai.ChatCompletion.create(
        model=model,
        messages=messages,
    )

    result = completion.choices[0].message.content

    start = result.rfind("[")
    end = result.rfind("]")
    result = result[start:end + 1]
    array = json.loads(result)

    return array


if __name__ == '__main__':
    sql = """
    select mls_number, description
    from listing_meta
    WHERE date_listed >= '2023-07-01'
    and description like '%downsize%'
    order by random() limit 5"""

    listings = query_postgres_sql(sql, return_dataframe=False)

    completion = get_motivation_detection(listings)

    result = completion.choices[0].message.content

    # In the result, find where the array starts and ends, and extract it.

    start = result.find("[")
    end = result.find("]")
    result = result[start:end + 1]
    array = json.loads(result)