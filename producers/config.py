import logging # used as a replacement for print fn
import os # enables retrieval of API key from .env file

from dotenv import load_dotenv # used with os module to retrieve API key

load_dotenv()

#configure logging module
logging.basicConfig(
    level=logging.INFO,
    format= "%(asctime)s - %(levelname)s - %(message)s" # describes how the message wil appear in the terminal
)

logger = logging.getLogger(__name__)

BASEURL = "alpha-vantage.p.rapidapi.com"

url = f"https://{BASEURL}/query"
api_key = os.getenv('API_KEY')
headers = {
	"x-rapidapi-key": api_key,
	"x-rapidapi-host": BASEURL
}