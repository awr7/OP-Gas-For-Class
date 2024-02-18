import asyncio
import aiohttp
import logging
import json
import pandas as pd
import streamlit as st
import pydeck as pdk

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

zipcode_coords = {
    "07302": (40.719389, -74.046469),
    "07304": (40.716495, -74.072593),
    "07305": (40.697302, -74.082273),
    "07306": (40.734924, -74.071875),
    "07307": (40.750877, -74.056865),
    "07310": (40.730133, -74.036816),
}

target_zip_codes = {"07302", "07304", "07305", "07306", "07307", "07310"}

def filter_geojson(geojson_data, target_zip_codes):
    filtered_features = [
        feature for feature in geojson_data['features']
        if feature['properties'].get('ZCTA5CE10') in target_zip_codes
    ]
    return {
        "type": "FeatureCollection",
        "features": filtered_features
    }

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
        lat, lon = zipcode_coords[zipcode]
        for price in station['prices']:
            if price['fuelProduct'] == 'regular_gas' and price['credit']['price'] > 0:
                stations_list.append({
                    'station_id': station['id'],
                    'name': station['name'],
                    'address': station['address']['line1'],
                    'price': price['credit']['price'],
                    'postedTime': price['credit']['postedTime'],
                    'zipcode': zipcode,
                    'lat': lat,
                    'lon': lon,
                })
    return stations_list

async def main():
    if 'button_clicked' not in st.session_state:
        st.session_state.button_clicked = False

    all_stations = {}  # Initialize all_stations dictionary

    with open('new-jersey-zip-codes-_1601.geojson', 'r') as file:
        geojson_data = json.load(file)
    filtered_geojson = filter_geojson(geojson_data, target_zip_codes)

    layers = []

    if st.button("Fetch Gas Stations Data"):
        st.session_state.button_clicked = True
        for zipcode in target_zip_codes:
            data = await fetch_stations(zipcode)
            if data:
                stations_list = extract_stations_data(data, zipcode)
                if stations_list:
                    all_stations[zipcode] = stations_list
                    cheapest_station = min(stations_list, key=lambda x: x['price'])
                    st.write(f"The cheapest gas station in {zipcode} is {cheapest_station['name']} located at {cheapest_station['address']} with a price of ${cheapest_station['price']}.")

    if st.session_state.button_clicked:
        text_data = [{
            "position": [lon, lat],
            "text": zipcode,
            "color": [255, 255, 255, 128]
        } for zipcode, (lat, lon) in zipcode_coords.items()]

        text_layer = pdk.Layer(
            "TextLayer",
            data=text_data,
            get_position="position",
            get_text="text",
            get_color="color",
            get_size=20,
            get_alignment_baseline="'bottom'",
        )

        geojson_layer = pdk.Layer(
            'GeoJsonLayer',
            filtered_geojson,
            opacity=0.8,
            stroked=True,
            filled=True,
            extruded=False,
            get_fill_color=[180, 0, 200, 140],
            get_line_color=[255, 255, 255],
            get_line_width=100
        )
        layers.extend([geojson_layer, text_layer])

    view_state = pdk.ViewState(latitude=40.7178, longitude=-74.0431, zoom=11, pitch=0)
    st.pydeck_chart(pdk.Deck(layers=layers, initial_view_state=view_state))

    if all_stations:
        for zipcode, stations in all_stations.items():
            st.write(f"All stations in zipcode {zipcode}:")
            df = pd.DataFrame(stations)
            df = df.rename(columns={"name": "Name", "address": "Address", "price": "Price", "postedTime": "Posted Time"})
            st.table(df[['Name', 'Address', 'Price', 'Posted Time']])

if __name__ == "__main__":
    asyncio.run(main())
