import asyncio
import aiohttp
import logging
import json
import pandas as pd
import streamlit as st

logging.basicConfig(level=logging.DEBUG)
_LOGGER = logging.getLogger(__name__)

BASE_URL = "https://www.gasbuddy.com/graphql"
DEFAULT_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Your details)"
}

LOCATION_QUERY = """
query LocationBySearchTerm($search: String) {
    locationBySearchTerm(search: $search) {
        stations {
            results {
                id
                name
                address {
                    line1
                }
                prices {
                    fuelProduct
                    credit {
                        price
                        postedTime
                    }
                }
            }
        }
    }
}
"""

async def fetch_stations(zipcode: str):
    query = {
        "operationName": "LocationBySearchTerm",
        "variables": {"search": zipcode},
        "query": LOCATION_QUERY,
    }

    async with aiohttp.ClientSession(headers=DEFAULT_HEADERS) as session:
        response = await session.post(BASE_URL, json=query)
        if response.status == 200:
            data = await response.json()
            _LOGGER.debug(json.dumps(data, indent=2))
            return data
        else:
            _LOGGER.error("Failed to fetch data: %s", await response.text())
            return None

def extract_stations_data(data, zipcode):
    stations_list = []
    for station in data['data']['locationBySearchTerm']['stations']['results']:
        for price in station['prices']:
            if price['fuelProduct'] == 'regular_gas' and price['credit']['price'] > 0:
                stations_list.append({
                    'station_id': station['id'],
                    'name': station['name'],
                    'address': station['address']['line1'],
                    'price': price['credit']['price'],
                    'postedTime': price['credit']['postedTime'],
                    'zipcode': zipcode
                })
    return stations_list

async def main():
    # List of zip codes for Jersey City
    zipcodes = ["07302", "07304", "07305", "07306", "07307", "07310"]  # Add all relevant zip codes

    if st.button("Fetch Gas Stations Data"):
        all_stations = {}

        for zipcode in zipcodes:
            data = await fetch_stations(zipcode)
            if data:
                stations_list = extract_stations_data(data, zipcode)
                if stations_list:
                    all_stations[zipcode] = stations_list
                    cheapest_station = min(stations_list, key=lambda x: x['price'])
                    st.write(f"The cheapest gas station in {zipcode} is {cheapest_station['name']} located at {cheapest_station['address']} with a price of ${cheapest_station['price']}.")

        for zipcode, stations in all_stations.items():
            st.write(f"All stations in zipcode {zipcode}:")
            df = pd.DataFrame(stations)
            df = df.rename(columns={"name": "Name", "address": "Address", "price": "Price", "postedTime": "Posted Time"})
            st.table(df[['Name', 'Address', 'Price', 'Posted Time']])


if __name__ == "__main__":
    asyncio.run(main())