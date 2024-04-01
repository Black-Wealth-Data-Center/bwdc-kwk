This folder contains a Python script that can be used to search the Yelp API
for all Black-owned businesses, or all businesses, in a city (or list of cities).

Documentation for the Yelp API, including instructions for obtaining an API key,
[is available here](https://docs.developer.yelp.com/docs/fusion-intro)

To run the script, you will need:
- A relatively recent version of Python (the script was built and tested with v3.11)
- The dependencies in `requirements.txt` installed
- An environment variable named `YELP_API_KEY` that stores your API key
- To implement a connection to a data store of your choosing where search results can persist

Run `python yelp_business_results.py --help` for a brief description of how to use the script.

Given the API's rate limits, it is unlikely that complete search results can be
obtained by a single execution of the script.

The primary challenges are therefore:
- storing intermediate results
- exiting gracefully when the rate limit is hit
- resuming the search (after sufficient time has passed) from whatever point it left off
- deduplicating the results of multiple searches
