import json
import time
import hmac
import hashlib
import base64
import requests
import urllib.parse

url = f'https://marginalttdemowebapi.fxopen.net:8443'
local_url = f'https://localhost:8443'


api_id = '5a96d26f-c7e0-40ee-96f8-1621d8afcd7f'
api_key = '2qmHFttRswczhpCW'
api_secret = 'r9rZKcbHk37dXc3WrbgHcJeRn38EDFrntssMpm3Q2aabRs7eCJK2cjrR4HKSdFeP'.encode()

compare_auth = 'HMAC 5a96d26f-c7e0-40ee-96f8-1621d8afcd7f:2qmHFttRswczhpCW:1622517630635:px0ZwKw8X7GulC0/1zyTZ9ABW4Or55baTUQo10RWKH4='

def generateHeaders(req_method, req_uri, req_content):
	timestamp = int(time.time()*1000)
	# timestamp = 1622527660288
	signature = f'{timestamp}{api_id}{api_key}{req_method}{req_uri}{req_content}'.encode()
	hmac_str = hmac.new(
		api_secret,
		msg=signature,
		digestmod=hashlib.sha256
	).digest()
	base64_hmac_str = base64.b64encode(hmac_str).decode()
	return {
		'Content-type': 'application/json',
		'Accept': 'application/json',
		'Accept-encoding': 'gzip, deflate',
		'Authorization': f'HMAC {api_id}:{api_key}:{timestamp}:{base64_hmac_str}'
	}

ept = '/api/v2/trade'
uri = url + ept
payload = {
	'Type': 'STOP',
	'Side': 'Buy',
	'Symbol': 'EURUSD',
	'Amount': 1000,
	'StopPrice': 1.26
}
# payload = {
# 	'Id': 167988004,
# 	'StopLoss': 1.15
# }
# payload = {
# 	'type': 'Close',
# 	'id': 167988004,
# 	'amount': 1000.0
# }
# params = {
# 	'type': 'Close',
# 	'id': '167988005',
# 	'amount': '1000.00'
# }

# print(urllib.parse.urlencode(params, doseq=False))

print(f'URI: {uri}')
headers = generateHeaders('GET', uri, '')
print(f'HEADERS: {json.dumps(headers, indent=2)}')

res = requests.get(
	uri, 
	headers=headers,
	# data=json.dumps(payload)
	# params=params
)

print(res.url)
print(res.status_code)
print(res.text)
print(res.json())
