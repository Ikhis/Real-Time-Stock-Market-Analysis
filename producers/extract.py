import requests
from config import logger, headers, url
# To connect and get the data from API
def connect_to_api():
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

# To extract only the required information from the API response
def extract_json(response):
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