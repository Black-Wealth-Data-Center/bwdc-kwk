import argparse
import json
import os
import pandas
import requests
import urllib
import zipcodes


DESTINATION_SCHEMA = "yelp"
DESTINATION_TABLE = "business_search_results"
YELP_URL = "https://api.yelp.com/v3/businesses/search?"
HEADERS = {
  "accept": "application/json",
  "Authorization": f"Bearer {os.getenv('YELP_API_KEY')}"
}
JSON_COLUMNS = ["categories", "coordinates", "transactions", "location"]


class DummyConnection:
  """
  Placeholder to serve as a data store that pandas will read/write
  data from/to -- could be SQLAlchemy, SQLite, etc.
  Update the TODOs below depending on your implementation
  """
  def __init__(
    self,
    connection,
    creds,
  ):
    self.connection = connection
    self.creds = creds


def yelp_search_url(location: str, term: str = None) -> str:
  """
  Build a Yelp search URL from `location` and optional `term`
  """
  term_param = f"&term={urllib.parse.quote_plus(term)}" if term else ""
  search_url = f"{YELP_URL}{term_param}&location={urllib.parse.quote_plus(location)}&sort=distance"

  return search_url


def yelp_expected_result_count(location: str, term: str = None) -> int:
  """
  Get the total number of results for a Yelp search on `location` and optional `term`
  """
  search_url = yelp_search_url(location=location, term=term)
  response = requests.get(url=search_url, headers=HEADERS)

  if response.status_code == 200:
    results_dict = json.loads(response.text)
    total_count = results_dict['total']
  else:
    total_count = 0
  
  print(f"Yelp search should return {total_count} results")
  return total_count


def yelp_city_locations(city: str, term: str = None) -> tuple[list, int]:
  """
  Get the total number of results for a Yelp search on `city` and optional `term`;
  if this is greater than 1000, split the single citywide search into multiple
  searches on zipcode-specific locations
  """
  expected_results = yelp_expected_result_count(location=city, term=term)

  if expected_results > 1000:
    print(f"\tSearching by ZIP so that we can catch more than 1000 results")

    filtered_zipcodes = zipcodes.filter_by(city=city.split(', ')[0], state=city.split(', ')[1])
    city_zips = [zipcode['zip_code'] for zipcode in filtered_zipcodes]
    locations = [f"{city} {zip_code}" for zip_code in city_zips]

  else:
    locations = [city]

  return locations, expected_results


def yelp_location_search(location: str, term = None) -> pandas.DataFrame:
  """
  Get as many results as possible for a Yelp search on `location`
  and optional `term`, paginating up to the the 1000-result limit
  """
  MAX_LIMIT = 50
  MAX_RESULTS = 1000

  search_url = yelp_search_url(location=location, term=term)

  page_dfs = []
  running_count = 0
  limit = MAX_LIMIT
  total_count = running_count + limit

  while running_count + limit <= MAX_RESULTS and running_count + limit <= total_count and limit > 0:
    if running_count == 0:
      print(f"\tGetting first {limit} results for {location}...")
    else:
      print(f"\tGetting results {running_count} - {running_count + limit} (of {total_count}) for {location}...")

    page_url = f"{search_url}&limit={limit}&offset={running_count}"
    response = requests.get(url=page_url, headers=HEADERS)

    if response.status_code == 200:
      results_dict = json.loads(response.text)
      total_count = results_dict['total']
      page_results = results_dict['businesses']
      page_count = len(page_results)

      if page_count == 0:
        print(f"\t\t...WARNING! Last request for {location} got {page_count} results. Moving on")
        limit = 0 # exits while loop

      else:
        running_count = running_count + page_count

        print(f"\t\t...got {running_count} of {total_count} results for {location}")

        results_df = pandas.DataFrame.from_records(page_results)
        results_df["_page_url"] = page_url
        page_dfs.append(results_df)

        limit = MAX_LIMIT if running_count + MAX_LIMIT <= min(total_count, MAX_RESULTS) else min(total_count, MAX_RESULTS) - running_count

    else:
      print(f"\tERROR! Got response status {response.status_code}: {response.reason}")
      print(f"\t\t{json.loads(response.text)}")
      limit = 0 # exits while loop

  if running_count < total_count:
    print(f"\tWARNING! Stopped at {running_count} results for {location}{' due to API limit' if running_count == MAX_RESULTS else ''}")
    print(f"\t\tAn additional {total_count - running_count} results cannot be retrieved")
    is_complete = False

  else:
    is_complete = True
  
  if page_dfs:
    search_results = pandas.concat(page_dfs, ignore_index=True)
  else:
    search_results = pandas.DataFrame()

  # Coerce JSON-like pandas objects into JSON-formatted strings
  search_results[JSON_COLUMNS] = search_results[JSON_COLUMNS].map(json.dumps).astype('string')

  # Add metadata
  search_results['_loaded_at'] = pandas.Timestamp.utcnow()
  search_results['_location'] = location
  search_results['_term'] = term if term else ''
  search_results['_is_complete'] = str(is_complete)

  return search_results


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Load business search results from Yelp')
  parser.add_argument('--cities', help="Semicolon-separated list like this: city_name, state_code; city_name, state_code", nargs='+', required=True)
  parser.add_argument('--black_owned', help="Pass this to search for Black-owned businesses", action='store_true')
  parser.add_argument('--all', help="Pass this to search for all businesses", action='store_true')
  args = parser.parse_args()

  print("Parsing args")
  terms = []

  if args.black_owned:
    terms.append('Black owned')

  if args.all:
    terms.append(None)

  cities = " ".join(args.cities).split(";")

  # TODO: implement a real connection 
  dummy_con = DummyConnection(connection='foo', creds='bar')

  for city in cities:
    for term in terms:
      print(f"************")
      print(f"Getting data for {term if term else 'total'} businesses in {city}")
      locations, expected_results = yelp_city_locations(city=city, term=term)

      for location in locations:
        term_filter = term if term else ''

        # Check whether we've already loaded results
        # TODO: implement according to your connection 
        try:
          loaded_data = pandas.read_sql(
            sql=f"""
            SELECT DISTINCT 1
            FROM {DESTINATION_SCHEMA}.{DESTINATION_TABLE}
            WHERE _location = '{location}'
            AND _term = '{term_filter}'
            """,
            con=dummy_con
          )
        except Exception as e:
          loaded_data = pandas.DataFrame()

        if loaded_data.empty:
          yelp_data = yelp_location_search(location=location, term=term)

          # Write the results to the data store
          # TODO: implement according to your connection
          try:
            yelp_data.to_sql(
              name=DESTINATION_TABLE,
              con=dummy_con,
              schema=DESTINATION_SCHEMA,
            )
          except Exception as e:
            print(e)
