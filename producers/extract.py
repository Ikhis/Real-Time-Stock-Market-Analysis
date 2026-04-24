import requests
from config import logger, headers, url

def connect_to_api():
	"""
	Fetch intraday stock data from the Alpha Vantage API via RapidAPI.

	This function loops through a predefined list of stock symbols and sends
	HTTP GET requests to retrieve 5-minute interval time series data.

	The API responses are collected into a list for further processing.

	Returns:
		list: A list of JSON responses from the API, one per stock symbol.

	Raises:
		None: All request-related errors are caught and logged internally.
	
	Notes:
		- Stops execution on the first encountered request failure.
		- Uses logging instead of print statements for observability.
	"""
	stocks = ['TSLA', 'MSFT', 'GOOGL'] # List of stocks. Retrieving multiple stocks
	json_response = [] # This list is used to store the API response

	for stock in range (0, len(stocks)):
	
		querystring = {"function":"TIME_SERIES_INTRADAY", # mimics a real-time pipeline senerio
               "symbol":f"{stocks[stock]}",
               "outputsize":"compact",
			   "interval":"5min", # returns data every 5 minutes
               "datatype":"json"}
# we use the try block to catch any error encountered in this request
		try:
			response = requests.get(url, headers=headers, params=querystring)

			response.raise_for_status # raises any http request error and passes to except block

			data = response.json() # Any API response received is captured here
			logger.info(f"Stocks {stocks[stock]} loaded successfully")

			json_response.append(data) # appends the response to json response list instead of the terminal
		
		except requests.exceptions.RequestException as e:
			logger.error(f"Error on stock:{e}")
			break
	return json_response


def extract_json(response):
	"""
	Transform raw API responses into a flattened list of stock records.

	This function extracts relevant fields from the nested JSON structure
	returned by the Alpha Vantage API and converts them into a list of
	dictionaries suitable for downstream processing (e.g., Kafka, Spark, DB).

	Args:
		response (list): List of JSON responses returned from `connect_to_api()`.

	Returns:
		list: A list of dictionaries where each dictionary represents a single
		      time-series record with the following keys:
		          - symbol (str)
		          - date (str)
		          - open (str)
		          - close (str)
		          - high (str)
		          - low (str)

	Raises:
		None: Assumes the API response structure is valid. Will raise KeyError
		      if expected fields are missing.

	Notes:
		- Designed for Alpha Vantage TIME_SERIES_INTRADAY response format.
		- Output is not type-casted; all numeric values remain as strings.
	"""
	records = []

	for data in response:
		symbol = data ['Meta Data']['2. Symbol']

		for data_str, metrics in data['Time Series (5min)'].items():
			record = {
				"symbol": symbol,
				"date": data_str,
				"open": metrics ["1. open"],
				"close": metrics ["4. close"],
				"high": metrics ["2. high"],
				"low": metrics ["3. low"]
			}

			records.append(record)
	return records