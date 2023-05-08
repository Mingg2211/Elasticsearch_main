import requests

url = 'http://192.168.1.58:9200'

response = requests.get(url)
print(response.text)