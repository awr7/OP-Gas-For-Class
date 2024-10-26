import asyncio
import aiohttp
import logging
import json
import pandas as pd
import streamlit as st
import pydeck as pdk
from geopy.distance import geodesic

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

def load_and_merge_geojson(nj_file, pa_file):
    """Loads and merges New Jersey and Pennsylvania GeoJSON files."""
    with open(nj_file, 'r') as nj_file, open(pa_file, 'r') as pa_file:
        nj_geojson = json.load(nj_file)
        pa_geojson = json.load(pa_file)
    
    merged_features = nj_geojson['features'] + pa_geojson['features']
    return {
        "type": "FeatureCollection",
        "features": merged_features
    }

def extract_zip_coords(geojson_data):
    """Extracts zip code coordinates from GeoJSON data."""
    zip_coords = {}
    
    for feature in geojson_data['features']:
        zip_code = feature['properties'].get('ZCTA5CE10')
        coords = feature['geometry']['coordinates']
        geometry_type = feature['geometry']['type']

        # Check if the coordinates are valid and not empty
        if not coords:
            continue
        
        # Handle different geometry types
        if geometry_type == 'Polygon' or geometry_type == 'MultiPolygon':
            # For Polygon, get the outer boundary; for MultiPolygon, get the first polygon
            coords = coords[0][0] if geometry_type == 'MultiPolygon' else coords[0]
        
        # Ensure that coords is a list of lists (lat, lon pairs)
        if isinstance(coords[0], list):
            try:
                # Calculate the centroid for a list of coordinates
                centroid_lat = sum([point[1] for point in coords]) / len(coords)
                centroid_lon = sum([point[0] for point in coords]) / len(coords)
            except IndexError:
                continue  # Skip if the structure is invalid
        else:
            # Handle single coordinate pair
            try:
                centroid_lat, centroid_lon = coords[1], coords[0]
            except IndexError:
                continue  # Skip if the structure is invalid
        
        if zip_code:
            zip_coords[zip_code] = (centroid_lat, centroid_lon)
            
    return zip_coords



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

def find_neighboring_zipcodes(input_zip, zip_coords, max_distance=5):
    """Finds neighboring zipcodes within a 5 miles from the input zipcode."""
    input_coords = zip_coords.get(input_zip)
    if not input_coords:
        return []

    neighboring_zipcodes = []
    for zipcode, coords in zip_coords.items():
        distance = geodesic(input_coords, coords).miles
        if distance <= max_distance:
            neighboring_zipcodes.append(zipcode)

    return neighboring_zipcodes

async def main():
    """Main function to run the Streamlit app."""
    st.title("Gas Station Prices by Zip Code")
    
    # Load and merge GeoJSON data from NJ and PA
    geojson_data = load_and_merge_geojson(
        'new-jersey-zip-codes-_1601.geojson',
        'pennsylvania-zip-codes-_1608.geojson'
    )
    zip_coords = extract_zip_coords(geojson_data)

    # User input for the zipcode
    input_zip = st.text_input("Enter a Zip Code:", "19405")
    if input_zip not in zip_coords:
        st.warning("Zip code not found in the dataset.")
        return

    # Get the center coordinates of the input zip code
    center_lat, center_lon = zip_coords[input_zip]

    # Calculate neighboring zipcodes within 5 miles
    target_zip_codes = find_neighboring_zipcodes(input_zip, zip_coords)

    all_stations = {}
    cheapest_stations = []
    zipcode_prices = {}

    # Filter GeoJSON data based on target zip codes
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

            # Prepare text data for displaying zip codes on the map
            text_data = [{
                "position": [zip_coords[zipcode][1], zip_coords[zipcode][0]],
                "text": zipcode,
                "color": [255, 255, 255, 255],
            } for zipcode in target_zip_codes]

            # Add a TextLayer to display zip codes
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

            # Set the map's view state to center on the input zip code
            view_state = pdk.ViewState(
                latitude=center_lat,
                longitude=center_lon,
                zoom=11,
                pitch=0
            )

            st.pydeck_chart(pdk.Deck(layers=layers, initial_view_state=view_state))

    # Display all stations in a table format for each zipcode
    for zipcode, stations in all_stations.items():
        st.write(f"All stations in zipcode {zipcode}:")
        df = pd.DataFrame(stations)
        df = df.rename(columns={"name": "Name", "address": "Address", "price": "Price", "postedTime": "Posted Time"})
        st.table(df[['Name', 'Address', 'Price', 'Posted Time']])

if __name__ == "__main__":
    asyncio.run(main())
