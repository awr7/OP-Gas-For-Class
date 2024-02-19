import asyncio
import aiohttp
import logging
import json
import pandas as pd
import streamlit as st
import pydeck as pdk

# Configure logging
logging.basicConfig(level=logging.DEBUG)
_LOGGER = logging.getLogger(__name__)

# Constants for the API request
BASE_URL = "https://www.gasbuddy.com/graphql"
DEFAULT_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Your details)"
}

# GraphQL query to fetch gas station data based on a search term (zipcode)
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
                latitude
                longitude
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

# Dictionary mapping zipcodes to their geographic coordinates (latitude, longitude)
zipcode_coords = {
    "07302": (40.719389, -74.046469),
    "07304": (40.716495, -74.072593),
    "07305": (40.697302, -74.082273),
    "07306": (40.734924, -74.071875),
    "07307": (40.750877, -74.056865),
    "07310": (40.730133, -74.036816),
}

# Set of target zipcodes to fetch gas station data for
target_zip_codes = {"07302", "07304", "07305", "07306", "07307", "07310"}

def filter_geojson(geojson_data, target_zip_codes):
    """Filters GeoJSON data to include only features within target zip codes."""
    filtered_features = [
        feature for feature in geojson_data['features']
        if feature['properties'].get('ZCTA5CE10') in target_zip_codes
    ]
    return {
        "type": "FeatureCollection",
        "features": filtered_features
    }

async def fetch_stations(zipcode: str):
    """Asynchronously fetches gas station data for a given zipcode."""
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
    """Extracts and returns a list of stations and their details from the API response."""
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
                    'zipcode': zipcode,
                    'latitude': station['latitude'],
                    'longitude': station['longitude'],
                })
    return stations_list

def assign_colors_to_zip_codes(zipcode_prices):
    """Assigns a color to each zipcode based on gas price, creating a gradient effect."""
    sorted_zipcodes = sorted(zipcode_prices.items(), key=lambda x: x[1], reverse=False)
    
    start_color = [255, 0, 0, 255]  # Red for the cheapest
    end_color = [255, 200, 200, 255]  # Light red for the most expensive
    
    num_zipcodes = len(sorted_zipcodes)
    zipcode_colors = {}
    
    for i, (zipcode, _) in enumerate(sorted_zipcodes):
        ratio = i / (num_zipcodes - 1) if num_zipcodes > 1 else 0
        interpolated_color = [int(start_color[j] * (1 - ratio) + end_color[j] * ratio) for j in range(4)]
        zipcode_colors[zipcode] = interpolated_color
    
    return zipcode_colors

def modify_geojson_with_colors(filtered_geojson, zipcode_colors):
    """Modifies the filtered GeoJSON with colors corresponding to each zipcode."""
    for feature in filtered_geojson['features']:
        zipcode = feature['properties']['ZCTA5CE10']
        feature['properties']['fillColor'] = zipcode_colors.get(zipcode, [255, 255, 255, 0])
    return filtered_geojson

async def main():
    """Main function to run the Streamlit app."""
    st.title("Gas Station Prices in Jersey City, NJ by Zip Code")
    
    all_stations = {}
    cheapest_stations = []
    zipcode_prices = {}

    # Load and filter GeoJSON data based on target zip codes
    with open('new-jersey-zip-codes-_1601.geojson', 'r') as file:
        geojson_data = json.load(file)
    filtered_geojson = filter_geojson(geojson_data, target_zip_codes)

    # Fetch and display gas station data upon button click
    if st.button("Fetch Gas Stations Data", key="fetch_gas_stations"):
        for zipcode in target_zip_codes:
            data = await fetch_stations(zipcode)
            if data:
                stations_list = extract_stations_data(data, zipcode)
                if stations_list:
                    all_stations[zipcode] = stations_list
                    cheapest_price = min([station['price'] for station in stations_list])
                    zipcode_prices[zipcode] = cheapest_price
                    cheapest_station = min(stations_list, key=lambda x: x['price'])
                    cheapest_stations.append({
                        "station_id": cheapest_station['station_id'],
                        "name": cheapest_station['name'],
                        "address": cheapest_station['address'],
                        "price": cheapest_station['price'],
                        "postedTime": cheapest_station['postedTime'],
                        "zipcode": zipcode,
                        "latitude": cheapest_station['latitude'],
                        "longitude": cheapest_station['longitude'],
                        "icon_data": {"url": "https://cdn-icons-png.flaticon.com/512/3448/3448636.png", "width": 128, "height": 128, "anchorY": 128}
                    })

        cheapest_stations = sorted(cheapest_stations, key=lambda x: x['price'])

        for station in cheapest_stations:
            # Construct Google Maps URL
            maps_url = f"https://www.google.com/maps/search/?api=1&query={station['latitude']},{station['longitude']}"
            # Display station with clickable link
            st.markdown(f"Cheapest in {station['zipcode']}: [{station['name']}]({maps_url}) at ${station['price']}")

        if zipcode_prices:
            zipcode_colors = assign_colors_to_zip_codes(zipcode_prices)
            modified_geojson = modify_geojson_with_colors(filtered_geojson, zipcode_colors)

            layers = []

            geojson_layer = pdk.Layer(
                'GeoJsonLayer',
                modified_geojson,
                opacity=0.8,
                stroked=True,
                filled=True,
                extruded=False,
                get_fill_color='properties.fillColor',
                get_line_color=[255, 255, 255],
                get_line_width=25,
            )
            layers.append(geojson_layer)

            icon_layer = pdk.Layer(
                "IconLayer",
                data=cheapest_stations,
                get_icon="icon_data",
                get_size=4,
                size_scale=15,
                get_position=["longitude", "latitude"],
                pickable=True,
            )
            layers.append(icon_layer)

            text_data = [{
                "position": [zipcode_coords[zipcode][1], zipcode_coords[zipcode][0]],
                "text": zipcode,
                "color": [255, 255, 255, 255],
            } for zipcode in target_zip_codes]

            text_layer = pdk.Layer(
                "TextLayer",
                data=text_data,
                get_position="position",
                get_text="text",
                get_color="color",
                get_size=16,
                get_alignment_baseline="'bottom'",
            )
            layers.append(text_layer)

            view_state = pdk.ViewState(latitude=40.7178, longitude=-74.0431, zoom=11, pitch=0)
            st.pydeck_chart(pdk.Deck(layers=layers, initial_view_state=view_state))

    # Display all stations in a table format for each zipcode
    for zipcode, stations in all_stations.items():
        st.write(f"All stations in zipcode {zipcode}:")
        df = pd.DataFrame(stations)
        df = df.rename(columns={"name": "Name", "address": "Address", "price": "Price", "postedTime": "Posted Time"})
        st.table(df[['Name', 'Address', 'Price', 'Posted Time']])

if __name__ == "__main__":
    asyncio.run(main())
