import numpy as np
import pandas as pd
import time
import requests
import hmac
import hashlib
import base64
import urllib.parse
import json
import traceback
import shortuuid
from datetime import datetime
from threading import Thread
from . import tradelib as tl
from .client import Client


class Subscription(object):

	def __init__(self, broker, msg_id, _type, *args):
		self.broker = broker
		self.msg_id = msg_id
		self.type = _type

		self.res = []
		self.args = args

		self.receive = False
		self.stream = None
		self.last_update = None

	def setStream(self, stream):
		self.receive = True
		self.stream = stream

	def onUpdate(self, *args):
		# self.broker._send_response(
		# 	self.msg_id,
		# 	{
		# 		'args': args,
		# 		'kwargs': {}
		# 	}
		# )

		if self.type == 1:
			self.broker.container.send_queue.append({
				"type": "account",
				"message": {
					"msg_id": self.msg_id,
					"result": {
						"args": args,
						"kwargs": {}
					}
				}
			})

			# self.broker.container.zmq_req_socket.recv()



class FXOpen(object):

	def __init__(self, container, user_id, strategy_id, broker_id, api_id, api_key, api_secret, is_demo):
		self.container = container
		self.userId = user_id
		self.strategyId = strategy_id
		self.brokerId = broker_id
		self.api_id = api_id
		self.api_key = api_key
		self.api_secret = api_secret.encode('ascii')
		self.isDemo = is_demo

		self.is_running = True
		self.is_connected = False
		self.last_update = time.time()

		if self.isDemo:
			self._url = f'https://marginalttdemowebapi.fxopen.net:8443'
			self._stream_url = "https://marginalttdemowebapi.fxopen.net:3001"
		else:
			self._url = f'https://ttlivewebapi.fxopen.net:8443'
			self._stream_url = "https://marginalttlivewebapi.fxopen.net:3001"
		self._session = requests.session()

		self.account_msg_queue = []
		self.price_msg_queue = []
		self._account_update_queue = []
		self.req_timer = time.time()
		self.req_count = 0

		self.account_subscriptions = []
		self.price_subscriptions = {}

		self._handle = {}

		self.account_client = None
		if broker_id != "PARENT":
			self.account_client = Client(self, is_demo=self.isDemo, client_type="account")
			self.account_client.connect()
			Thread(target=self._handle_account_updates).start()
		# else:
			# self.price_client = Client(self, is_demo=self.isDemo, client_type="price")
			# self.price_client.connect()

		self.send_login()

		print('[FXOpen.__init__] DONE.', flush=True)


	def generateReference(self):
		return shortuuid.uuid()


	def generateHeaders(self, req_method, req_uri, req_content):
		timestamp = int(time.time()*1000)
		signature = f'{timestamp}{self.api_id}{self.api_key}{req_method}{req_uri}{req_content}'.encode()
		hmac_str = hmac.new(
			self.api_secret,
			msg=signature,
			digestmod=hashlib.sha256
		).digest()
		base64_hmac_str = base64.b64encode(hmac_str).decode()
		return {
			'Content-type': 'application/json',
			'Accept': 'application/json',
			'Accept-encoding': 'gzip, deflate',
			'Authorization': f'HMAC {self.api_id}:{self.api_key}:{timestamp}:{base64_hmac_str}'
		}

	
	def generateStreamHmac(self):
		timestamp = int(time.time()*1000)
		signature = f'{timestamp}{self.api_id}{self.api_key}'.encode()
		hmac_str = hmac.new(
			self.api_secret,
			msg=signature,
			digestmod=hashlib.sha256
		).digest()
		base64_hmac_str = base64.b64encode(hmac_str).decode()
		return timestamp, base64_hmac_str


	# def _send_response(self, msg_id, res):
	# 	res = {
	# 		'msg_id': msg_id,
	# 		'result': res
	# 	}

	# 	self.container.sio.emit(
	# 		'broker_res', 
	# 		res, 
	# 		namespace='/broker'
	# 	)


	def on_account_message(self, msg):
		print(f"[FXOpen.on_account_message] {json.dumps(msg, indent=2)}")
		# for sub in self.account_subscriptions:
		# 	sub.onUpdate(msg)
		self._on_account_update(msg)
		
		msg_id = msg.get("Id")
		if msg_id is not None:
			self.check_handle(msg_id)

		error = msg.get("Error")
		if error is not None:
			if "login" in error:
				self.send_login()



	def on_price_message(self, msg):
		# print(f"[FXOpen.on_price_message] {json.dumps(msg, indent=2)}")

		try:
			if "Symbol" in msg["Result"]:
				instrument = self.convertFromFXOInstrument(msg["Result"]["Symbol"])
				timestamp = msg["Result"]["Timestamp"] / 1000
				ask = None
				bid = None
				volume = None
				if "BestAsk" in msg["Result"]:
					ask = msg["Result"]["BestAsk"]["Price"]
					volume = msg["Result"]["BestAsk"]["Volume"]
				if "BestBid" in msg["Result"]:
					bid = msg["Result"]["BestBid"]["Price"]
					volume = msg["Result"]["BestBid"]["Volume"]

				if instrument in self.price_subscriptions:
					self.price_subscriptions[instrument].onUpdate(timestamp, ask, bid, volume)
					
		except Exception:
			print(traceback.format_exc())


	def _download_historical_data_broker(self, 
		product, period, tz='Europe/London', 
		start=None, end=None, count=None,
		force_download=False
	):
		params = {}
		count_multi = self.getPeriodCountMultiplier(period)
		if count is not None:
			if start is not None:
				params["timestamp"] = int(start * 1000)
				params["count"] = int(count * count_multi)

			elif end is not None:
				params["timestamp"] = int(end * 1000)
				params["count"] = int(-count * count_multi)

			else:
				params["timestamp"] = int(datetime.utcnow().timestamp() * 1000)
				params["count"] = int(-count * count_multi)

		elif start is not None and end is not None:
			count = tl.getDateCount(
				period, tl.utils.convertTimestampToTime(start), 
				tl.utils.convertTimestampToTime(end)
			)
			params["timestamp"] = end * 1000
			params["count"] = int(-count * count_multi)

		else:
			return { "error": "Bad Request." }
			
		fxo_instrument = self.convertToFXOInstrument(product)
		fxo_period = self.convertToFXOPeriod(period)

		ask_uri = self._url + f"/api/v2/public/quotehistory/{fxo_instrument}/{fxo_period}/bars/ask"
		bid_uri = self._url + f"/api/v2/public/quotehistory/{fxo_instrument}/{fxo_period}/bars/bid"

		ask_headers = self.generateHeaders('GET', ask_uri, '?'+urllib.parse.urlencode(params, doseq=False))
		bid_headers = self.generateHeaders('GET', bid_uri, '?'+urllib.parse.urlencode(params, doseq=False))

		self._session.headers.update(ask_headers)
		ask_res = self._session.get(ask_uri, params=params)

		self._session.headers.update(bid_headers)
		bid_res = self._session.get(bid_uri, params=params)
		
		if ask_res.status_code == 200 and bid_res.status_code == 200:
			ask_data = ask_res.json()
			bid_data = bid_res.json()

			ask_df = pd.DataFrame(data=ask_data["Bars"])
			ask_df.columns = [
				"volume", "ask_close", "ask_low", "ask_high", "ask_open", "timestamp"
			]
			ask_df.set_index("timestamp", inplace=True)
			ask_df.index /= 1000

			bid_df = pd.DataFrame(
				data=bid_data["Bars"]
			)
			bid_df.columns = [
				"volume", "bid_close", "bid_low", "bid_high", "bid_open", "timestamp"
			]
			bid_df.set_index("timestamp", inplace=True)
			bid_df.index /= 1000

			# Fill missing timestamps
			missing_ask = bid_df.loc[bid_df.index.difference(ask_df.index)]
			missing_ask.columns = ask_df.columns
			ask_df = pd.concat((ask_df, missing_ask)).sort_index()

			missing_bid = ask_df.loc[ask_df.index.difference(bid_df.index)]
			missing_bid.columns = bid_df.columns
			bid_df = pd.concat((bid_df, missing_bid)).sort_index()

			mid_values = (
				ask_df[["ask_close", "ask_low", "ask_high", "ask_open"]].values 
				 + bid_df[["bid_close", "bid_low", "bid_high", "bid_open"]].values
			)/2
			mid_df = pd.DataFrame(
				data=mid_values, 
				columns=["mid_close", "mid_low", "mid_high", "mid_open"],
				index=ask_df.index.copy()
			).round(decimals=5)
			result = pd.concat((
				ask_df[["ask_open", "ask_high", "ask_low", "ask_close"]], 
				mid_df[["mid_open", "mid_high", "mid_low", "mid_close"]], 
				bid_df[["bid_open", "bid_high", "bid_low", "bid_close"]]
			), axis=1)
			# result["volume"] = ask_df.volume

			result = self.constructBarsByPeriod(result, period)
			result.index = result.index.astype(int)
			return result.to_dict()
		else:
			return { "error": "Failed to retrieve data." }



	def authCheck(self):
		uri = self._url + '/api/v2/account'
		headers = self.generateHeaders('GET', uri, '')
		self._session.headers.update(headers)

		res = self._session.get(uri)
		if res.status_code == 200:
			data = res.json()
			if (
				data.get("IsValid") and not data.get("IsReadonly") and
				not data.get("IsBlocked") and not data.get("IsArchived")
			):
				return {'result': True}
		
		return {'result': False}


	def _convert_fxo_position(self, account_id, pos):
		order_id = str(pos.get('Id'))
		instrument = self.convertFromFXOInstrument(pos.get('Symbol'))
		direction = tl.LONG if pos.get("Side") == "Buy" else tl.SHORT
		
		if pos.get('RemainingAmount'):
			lotsize = self.convertToLotsize(pos.get('RemainingAmount'))
		else:
			lotsize = self.convertToLotsize(pos.get('FilledAmount'))

		if pos.get('StopPrice'):
			entry_price = float(pos.get('StopPrice'))
		else:
			entry_price = float(pos.get('Price'))
			
		sl = None
		if pos.get('StopLoss'):
			sl = pos['StopLoss']
		tp = None
		if pos.get('TakeProfit'):
			tp = pos['TakeProfit']
		open_time = pos.get('Filled')/1000

		return tl.Position(
			self,
			order_id, account_id, instrument,
			tl.MARKET_ENTRY, direction, lotsize,
			entry_price, sl, tp, open_time
		)


	def _get_all_positions(self, account_id):
		uri = self._url + '/api/v2/trade'
		headers = self.generateHeaders('GET', uri, '')
		self._session.headers.update(headers)

		res = self._session.get(uri)

		if res.status_code == 200:
			result = {account_id: []}
			data = res.json()
			print(data, flush=True)
			for pos in data:
				if pos.get("Type") == "Position":
					new_pos = self._convert_fxo_position(account_id, pos)
					result[account_id].append(new_pos)

			print(f"[_get_all_positions] {json.dumps(result, indent=2)}", flush=True)
			return result
		else:
			return None

	
	def _handle_order_create(self, trade):

		account_id = str(trade["AccountId"])

		result = {}
		client_id = trade.get("ClientId")
		handled_id = None
		check_order = self.getOrderByID(str(trade["Id"]))
		if check_order is None:
			order = self._convert_fxo_order(account_id, trade)
			self.appendDbOrder(order)

			result[self.generateReference()] = {
				'timestamp': order["open_time"],
				'type': order["order_type"],
				'accepted': True,
				'item': order
			}
			print(f"[_handle_order_create] 4: {result}")

			if client_id is not None:
				handled_id = "ordercreate_" + client_id
			# 	self._handled["ordercreate_" + client_id] = result

		return result, handled_id


	def _handle_order_fill_close(self, trade):

		account_id = str(trade["AccountId"])

		# Delete any existing order reference
		from_order = self.getOrderByID(str(trade["Id"]))
		if from_order is not None:
			self.deleteDbOrder(from_order["order_id"])

			for sub in self.account_subscriptions:
				sub.onUpdate(
					{
						self.generateReference(): {
							'timestamp': from_order["close_time"],
							'type': tl.ORDER_CANCEL,
							'accepted': True,
							'item': from_order
						}
					}, 
					account_id, None
				)
			# self.handleOnTrade(account_id, {
			# 	self.generateReference(): {
			# 		'timestamp': from_order["close_time"],
			# 		'type': tl.ORDER_CANCEL,
			# 		'accepted': True,
			# 		'item': from_order
			# 	}
			# })

		result = {}
		client_id = trade.get("ClientId")
		handled_id = None
		# Closed Position
		pos = self.getPositionByID(str(trade["Id"]))
		if pos is not None:
			size = pos["lotsize"] - self.convertToLotsize(trade["RemainingAmount"])

			if size >= pos["lotsize"]:
				if trade.get("Price"):
					pos["close_price"] = trade["Price"]
				else:
					pos["close_price"] = trade["StopPrice"]

				pos["close_time"] = trade["Modified"] / 1000

				comment = trade.get("Comment")
				if comment is not None and "TP" in comment:
					order_type = tl.TAKE_PROFIT
				elif comment is not None and "SL" in comment:
					order_type = tl.STOP_LOSS
				else:
					order_type = tl.POSITION_CLOSE

				result[self.generateReference()] = {
					'timestamp': pos["close_price"],
					'type': order_type,
					'accepted': True,
					'item': pos
				}
				self.deleteDbPosition(pos["order_id"])
			
			else:
				cpy = tl.Position.fromDict(self, pos)
				cpy.lotsize = size

				if trade.get("Price"):
					cpy.close_price = trade["Price"]
				else:
					cpy.close_price = trade["StopPrice"]

				cpy.close_time = trade["Modified"] / 1000

				# Modify open position
				pos["lotsize"] = self.convertToLotsize(trade["RemainingAmount"])
				self.replaceDbPosition(pos)

				result[self.generateReference()] = {
					'timestamp': cpy.close_price,
					'type': tl.POSITION_CLOSE,
					'accepted': True,
					'item': cpy
				}
			
			if client_id is not None:
				handled_id = "fillclose_" + client_id
			# 	self._handled["fillclose_" + client_id] = result
		
		return result, handled_id


	def _handle_order_fill_open(self, trade):
		
		print(f"[_handle_order_fill_open] {trade}")

		account_id = str(trade["AccountId"])

		# Delete any existing order reference
		from_order = self.getOrderByID(str(trade["Id"]))
		if from_order is not None:
			self.deleteDbOrder(from_order["order_id"])

			for sub in self.account_subscriptions:
				sub.onUpdate(
					{
						self.generateReference(): {
							'timestamp': from_order["close_time"],
							'type': tl.ORDER_CANCEL,
							'accepted': True,
							'item': from_order
						}
					}, 
					account_id, None
				)
			# self.handleOnTrade(account_id, {
			# 	self.generateReference(): {
			# 		'timestamp': from_order["close_time"],
			# 		'type': tl.ORDER_CANCEL,
			# 		'accepted': True,
			# 		'item': from_order
			# 	}
			# })

		result = {}
		client_id = trade.get("ClientId")
		handled_id = None
		# Closed Position
		
		check_pos = self.getPositionByID(str(trade["Id"]))
		if check_pos is None:
			pos = self._convert_fxo_position(account_id, trade)
			self.appendDbPosition(pos)

			result[self.generateReference()] = {
				'timestamp': pos["open_time"],
				'type': pos["order_type"],
				'accepted': True,
				'item': pos
			}

			if client_id is not None:
				handled_id = "fillopen_" + client_id
			#	self._handled["fillopen_" + client_id] = result
	
		print(f"[_handle_order_fill_open] {result}")
		# print(f"[_handle_order_fill_open] {self._handled}")
	
		return result, handled_id


	def _handle_order_cancel(self, trade):

		result = {}
		client_id = trade.get("ClientId")
		handled_id = None
		order = self.getOrderByID(str(trade["Id"]))
		if order is not None:
			order["close_time"] = trade["Modified"] / 1000
			self.deleteDbOrder(order["order_id"])

			result[self.generateReference()] = {
				'timestamp': order["close_time"],
				'type': tl.ORDER_CANCEL,
				'accepted': True,
				'item': order
			}

			if client_id is not None:
				handled_id = "ordercancel_" + client_id
				# self._handled["ordercancel_" + client_id] = result

		return result, handled_id


	def _handle_modify(self, trade):
		
		result = {}
		client_id = trade.get("ClientId")
		handled_id = None
		
		if trade["Type"] == "Position":
			pos = self.getPositionByID(str(trade["Id"]))
			if pos is not None:
				pos["sl"] = trade.get("StopLoss")
				pos["tp"] = trade.get("TakeProfit")

				self.replaceDbPosition(pos)

				result[self.generateReference()] = {
					'timestamp': trade["Modified"] / 1000,
					'type': tl.MODIFY,
					'accepted': True,
					'item': pos
				}

				if client_id is not None:
					handled_id = "modify_" + client_id
					# self._handled["modify_" + client_id] = result

		else:
			order = self.getOrderByID(str(trade["Id"]))
			if order is not None:
				order["sl"] = trade.get("StopLoss")
				order["tp"] = trade.get("TakeProfit")
				order["lotsize"] = self.convertToLotsize(trade["RemainingAmount"])

				if "StopPrice" in trade:
					order["entry_price"] = trade["StopPrice"]
				else:
					order["entry_price"] = trade["Price"]

				self.replaceDbOrder(order)

				result[self.generateReference()] = {
					'timestamp': trade["Modified"] / 1000,
					'type': tl.MODIFY,
					'accepted': True,
					'item': order
				}

				if client_id is not None:
					handled_id = "modify_" + client_id
					# self._handled["modify_" + client_id] = result

		return result, handled_id

	
	def _handle_trades(self, trades):
		new_positions = []
		new_orders = []

		for i in trades:
			account_id = str(i["AccountId"])
			if i.get("Type") == "Position":
				new_pos = self._convert_fxo_position(account_id, i)
				new_positions.append(new_pos)
			elif i.get("Type") == "Limit" or i.get("Type") == "Stop":
				new_order = self._convert_fxo_order(account_id, i)
				new_orders.append(new_order)

		self.setDbPositions(new_positions)
		self.setDbOrders(new_orders)

		return {
			self.generateReference(): {
				'timestamp': time.time(),
				'type': tl.UPDATE,
				'accepted': True,
				'item': {
					"positions": new_positions,
					"orders": new_orders
				}
			}
		}


	def createPosition(self,
		product, lotsize, direction,
		account_id, entry_range, entry_price,
		sl_range, tp_range, sl_price, tp_price
	):
		if not self.account_client.is_connected:
			return {'status': 400, 'result': {}}

		uri = self._url + '/api/v2/trade'

		fxo_instrument = self.convertToFXOInstrument(product)
		size = self.convertToUnitSize(lotsize)
		direction = "Buy" if direction == tl.LONG else "Sell"

		sl = 0
		tp = 0
		if sl_price:
			sl = float(sl_price)
		if tp_price:
			tp = float(tp_price)

		payload = {
			"ClientId": self.generateReference(),
			'Type': "Market",
			'Side': direction,
			'Symbol': fxo_instrument,
			'Amount': size,
			'StopLoss': sl,
			'TakeProfit': tp
		}

		headers = self.generateHeaders('POST', uri, json.dumps(payload))
		self._session.headers.update(headers)

		res = self._session.post(uri, data=json.dumps(payload))
		status_code = res.status_code

		if status_code == 200:
			data = res.json()
			print(f'[FXOpen.createPosition] DONE: {status_code}\n{json.dumps(data, indent=2)}', flush=True)
			return {'status': status_code, 'result': data}
		else:
			return {'status': status_code, 'result': {}}


	def modifyPosition(self, pos, sl_price, tp_price):		
		uri = self._url + '/api/v2/trade'
		payload = {
			'Id': int(pos['order_id']),
			'StopLoss': float(sl_price),
			'TakeProfit': float(tp_price)
		}

		headers = self.generateHeaders('PUT', uri, json.dumps(payload))
		self._session.headers.update(headers)

		res = self._session.put(uri, data=json.dumps(payload))
		status_code = res.status_code

		if status_code == 200:
			data = res.json()
			print(f'[FXOpen.modifyPosition] DONE: {status_code}\n{json.dumps(data, indent=2)}', flush=True)
			return {'status': status_code, 'result': data}
		else:
			return {'status': status_code, 'result': {}}


	def deletePosition(self, pos, lotsize):		
		size = self.convertToUnitSize(lotsize)

		uri = self._url + '/api/v2/trade'
		params = {
			'type': 'Close',
			'id': int(pos['order_id']),
			'amount': size
		}

		headers = self.generateHeaders('DELETE', uri, '?'+urllib.parse.urlencode(params, doseq=False))
		self._session.headers.update(headers)

		res = self._session.delete(uri, params=params)
		status_code = res.status_code

		if status_code == 200:
			data = res.json()
			print(f'[FXOpen.deletePosition] DONE: {status_code}\n{json.dumps(data, indent=2)}', flush=True)
			return {'status': status_code, 'result': data}
		else:
			return {'status': status_code, 'result': {}}


	def _convert_fxo_order(self, account_id, order):
		if order.get("Type") == "Limit":
			order_type = tl.LIMIT_ORDER
		elif order.get("Type") == "Stop":
			order_type = tl.STOP_ORDER
		else:
			order_type = tl.MARKET_ORDER

		order_id = str(order.get('Id'))
		instrument = self.convertFromFXOInstrument(order.get('Symbol'))
		direction = tl.LONG if order.get("Side") == "Buy" else tl.SHORT
		lotsize = self.convertToLotsize(order.get('RemainingAmount'))

		if order.get('Price') is not None:
			entry_price = float(order.get('Price'))
		else:
			entry_price = float(order.get('StopPrice'))

		sl = None
		if order.get('StopLoss'):
			sl = order['StopLoss']
		tp = None
		if order.get('TakeProfit'):
			tp = order['TakeProfit']
		open_time = order.get('Modified')/1000

		return tl.Order(
			self,
			order_id, account_id, instrument,
			order_type, direction, lotsize,
			entry_price, sl, tp, open_time
		)


	def _get_all_orders(self, account_id):
		uri = self._url + '/api/v2/trade'
		headers = self.generateHeaders('GET', uri, '')
		self._session.headers.update(headers)

		res = self._session.get(uri)

		if res.status_code == 200:
			result = {account_id: []}
			data = res.json()
			print(data, flush=True)
			for order in data:
				if order.get("Type") == "Limit" or order.get("Type") == "Stop":
					new_order = self._convert_fxo_order(account_id, order)
					result[account_id].append(new_order)

			print(f"[_get_all_orders] {json.dumps(result, indent=2)}", flush=True)
			return result
		else:
			return None


	def getAllAccounts(self):
		uri = self._url + '/api/v2/account'
		headers = self.generateHeaders('GET', uri, '')
		self._session.headers.update(headers)

		res = self._session.get(uri)

		result = []
		print(f"[FXOpen.getAllAccounts] {res.status_code}", flush=True)
		if res.status_code == 200:
			data = res.json()
			print(f"[FXOpen.getAllAccounts] {data}", flush=True)

			if (
				data.get("Id") is not None and 
				data.get("AccountingType") == "Gross" and
				data.get("IsValid") and not data.get("IsReadonly") and
				not data.get("IsBlocked") and not data.get("IsArchived")
			):
				result.append(data.get("Id"))
				return result
			else:
				return None

		else:
			return None


	def getAccountInfo(self, account_id):
		uri = self._url + '/api/v2/account'
		headers = self.generateHeaders('GET', uri, '')
		self._session.headers.update(headers)

		res = self._session.get(uri)

		result = {}
		print(f"[FXOpen.getAccountInfo] {res.status_code}", flush=True)
		if res.status_code == 200:
			data = res.json()
			print(f"[FXOpen.getAccountInfo] {data}", flush=True)
			
			result[account_id] = {
				'currency': data.get("BalanceCurrency"),
				'balance': data.get("Balance"),
				'pl': data.get("Profit"),
				'margin': data.get("Margin"),
				'available': data.get("Balance") + data.get("Profit")
			}
			return result

		else:
			return None


	def createOrder(self, 
		product, lotsize, direction,
		account_id, order_type, entry_range, entry_price,
		sl_range, tp_range, sl_price, tp_price
	):
		if not self.account_client.is_connected:
			print(f"[createOrder] 1: {self.brokerId} {self.account_client.is_connected}", flush=True)
			return {'status': 400, 'result': {}}
	
		uri = self._url + '/api/v2/trade'

		fxo_instrument = self.convertToFXOInstrument(product)
		size = self.convertToUnitSize(lotsize)

		sl = 0
		tp = 0
		if sl_price:
			sl = sl_price
		elif sl_range:
			sl_range = tl.convertToPrice(sl_range)
			if direction == tl.LONG:
				sl = round(entry_price + sl_range, 5)
			else:
				sl = round(entry_price - sl_range, 5)

		if tp_price:
			tp = tp_price

		elif tp_price:
			tp_range = tl.convertToPrice(tp_range)
			if direction == tl.LONG:
				tp = round(entry_price + tp_range, 5)
			else:
				tp = round(entry_price - tp_range, 5)

		direction = "Buy" if direction == tl.LONG else "Sell"
		
		payload = {
			"ClientId": self.generateReference(),
			'Side': direction,
			'Symbol': fxo_instrument,
			'Amount': size,
			'StopLoss': float(sl),
			'TakeProfit': float(tp)
		}

		if order_type == tl.LIMIT_ORDER:
			payload['Type'] = "Limit"
			payload['Price'] = float(entry_price)
		elif order_type == tl.STOP_ORDER:
			payload['Type'] = "Stop"
			payload['StopPrice'] = float(entry_price)
		else:
			print(f"[createOrder] 2: {order_type}", flush=True)
			return {'status': 400, 'result': {}}

		print(json.dumps(payload), flush=True)
		headers = self.generateHeaders('POST', uri, json.dumps(payload))
		self._session.headers.update(headers)

		res = self._session.post(uri, data=json.dumps(payload))
		status_code = res.status_code

		if status_code == 200:
			data = res.json()
			print(f'[FXOpen.createOrder] DONE: {status_code}\n{json.dumps(data, indent=2)}', flush=True)
			return {'status': status_code, 'result': data}
		else:
			try:
				print(f"[createOrder] 3: {status_code}", flush=True)
				data = res.text
				print(f"[createOrder] 3: {data}", flush=True)
			except:
				print(traceback.format_exc(), flush=True)
			return {'status': status_code, 'result': {}}


	def modifyOrder(self, order, lotsize, entry_price, sl_price, tp_price):
		uri = self._url + '/api/v2/trade'

		size = self.convertToUnitSize(lotsize)
		
		payload = None
		if order['order_type'] == tl.LIMIT_ORDER:
			payload = {
				'Id': int(order['order_id']),
				'Price': float(entry_price),
				'Amount': size,
				'StopLoss': float(sl_price),
				'TakeProfit': float(tp_price)
			}
		elif order['order_type'] == tl.STOP_ORDER:
			payload = {
				'Id': int(order['order_id']),
				'StopPrice': float(entry_price),
				'Amount': size,
				'StopLoss': float(sl_price),
				'TakeProfit': float(tp_price)
			}
		else:
			print(f"[modifyOrder] 1: {order['order_type']}", flush=True)
			return {'status': 400, 'result': {}}

		headers = self.generateHeaders('PUT', uri, json.dumps(payload))
		self._session.headers.update(headers)

		print(json.dumps(payload), flush=True)
		res = self._session.put(uri, data=json.dumps(payload))
		status_code = res.status_code

		if status_code == 200:
			data = res.json()
			print(f'[FXOpen.modifyOrder] DONE: {status_code}\n{json.dumps(data, indent=2)}', flush=True)
			return {'status': status_code, 'result': data}
		else:
			try:
				print(f"[modifyOrder] 2: {status_code}", flush=True)
				data = res.text
				print(f"[modifyOrder] 2: {data}", flush=True)
			except:
				print(traceback.format_exc(), flush=True)
			return {'status': status_code, 'result': {}}


	def deleteOrder(self, order):		
		uri = self._url + '/api/v2/trade'
		params = {
			'type': 'Cancel',
			'id': int(order['order_id'])
		}

		headers = self.generateHeaders('DELETE', uri, '?'+urllib.parse.urlencode(params, doseq=False))
		self._session.headers.update(headers)

		print(f"[FXOpen.createOrder] {uri}", flush=True)
		print(f"[FXOpen.createOrder] {params}", flush=True)

		res = self._session.delete(uri, params=params)
		status_code = res.status_code
		print(f"[FXOpen.createOrder] {status_code}", flush=True)
		print(f"[FXOpen.createOrder] {res.json()}", flush=True)

		if status_code == 200:
			data = res.json()
			print(f'[FXOpen.deleteOrder] DONE: {status_code}\n{json.dumps(data, indent=2)}', flush=True)
			return {'status': status_code, 'result': data}
		else:
			return {'status': status_code, 'result': {}}


	def convertFromFXOInstrument(self, instrument):
		if instrument == "EURUSD":
			return "EUR_USD"
		else:
			return instrument


	def convertToFXOInstrument(self, instrument):
		if instrument == "EUR_USD":
			return "EURUSD"
		else:
			return instrument

	
	def convertToFXOPeriod(self, period):
		if period == tl.period.TWO_MINUTES:
			return "M1"
		elif period == tl.period.TEN_MINUTES:
			return "M5"
		elif period == tl.period.TWO_HOURS:
			return "H1"
		elif period == tl.period.THREE_HOURS:
			return "H1"
		elif period == tl.period.DAILY:
			return "D1"
		elif period == tl.period.WEEKLY:
			return "W1"
		elif period == tl.period.MONTHLY:
			return "W1"
		else:
			return period


	def getPeriodCountMultiplier(self, period):
		if period == tl.period.TWO_MINUTES:
			return (
				tl.period.getPeriodOffsetSeconds(period)
				/ tl.period.getPeriodOffsetSeconds(tl.period.ONE_MINUTE)
			)
		elif period == tl.period.TEN_MINUTES:
			return (
				tl.period.getPeriodOffsetSeconds(period)
				/ tl.period.getPeriodOffsetSeconds(tl.period.FIVE_MINUTES)
			)
		elif period == tl.period.TWO_HOURS:
			return (
				tl.period.getPeriodOffsetSeconds(period)
				/ tl.period.getPeriodOffsetSeconds(tl.period.ONE_HOUR)
			)
		elif period == tl.period.THREE_HOURS:
			return (
				tl.period.getPeriodOffsetSeconds(period)
				/ tl.period.getPeriodOffsetSeconds(tl.period.ONE_HOUR)
			)
		elif period == tl.period.MONTHLY:
			return (
				tl.period.getPeriodOffsetSeconds(period)
				/ tl.period.getPeriodOffsetSeconds(tl.period.WEEKLY)
			)
		else:
			return 1.0


	def constructBarsByPeriod(self, df, period):
		if period == tl.period.TWO_MINUTES:
			period_off = (
				tl.period.getPeriodOffsetSeconds(period) 
				/ tl.period.getPeriodOffsetSeconds(tl.period.ONE_MINUTE)
			) * tl.period.getPeriodOffsetSeconds(tl.period.ONE_MINUTE)
		elif period == tl.period.TEN_MINUTES:
			period_off = (
				tl.period.getPeriodOffsetSeconds(period) 
				/ tl.period.getPeriodOffsetSeconds(tl.period.FIVE_MINUTES)
			) * tl.period.getPeriodOffsetSeconds(tl.period.FIVE_MINUTES)
		elif period == tl.period.TWO_HOURS:
			period_off = (
				tl.period.getPeriodOffsetSeconds(period) 
				/ tl.period.getPeriodOffsetSeconds(tl.period.ONE_HOUR)
			) * tl.period.getPeriodOffsetSeconds(tl.period.ONE_HOUR)
		elif period == tl.period.THREE_HOURS:
			period_off = (
				tl.period.getPeriodOffsetSeconds(period) 
				/ tl.period.getPeriodOffsetSeconds(tl.period.ONE_HOUR)
			) * tl.period.getPeriodOffsetSeconds(tl.period.ONE_HOUR)
		elif period == tl.period.MONTHLY:
			period_off = (
				tl.period.getPeriodOffsetSeconds(period) 
				/ tl.period.getPeriodOffsetSeconds(tl.period.WEEKLY)
			) * tl.period.getPeriodOffsetSeconds(tl.period.WEEKLY)
		else:
			return df

		first_data_ts = tl.convertTimeToTimestamp(datetime.utcfromtimestamp(df.index.values[0]).replace(
			hour=0, minute=0, second=0, microsecond=0
		))
		c_ts = df.index.values[0] - ((df.index.values[0] - first_data_ts) % tl.period.getPeriodOffsetSeconds(period))

		result = pd.DataFrame(columns=df.columns)
		c_ohlc = np.zeros((12,), dtype=np.float64)
		for i in range(df.shape[0]):
			if df.index[i] < c_ts + period_off:
				df_ohlc = df.iloc[i].values
				if (c_ohlc == 0).any():
					c_ohlc = df_ohlc
				else:
					# High
					c_ohlc[1] = max(c_ohlc[1], df_ohlc[1])
					c_ohlc[5] = max(c_ohlc[5], df_ohlc[5])
					c_ohlc[9] = max(c_ohlc[9], df_ohlc[9])
					# Low
					c_ohlc[2] = min(c_ohlc[2], df_ohlc[2])
					c_ohlc[6] = min(c_ohlc[6], df_ohlc[6])
					c_ohlc[10] = min(c_ohlc[10], df_ohlc[10])
					# Close
					c_ohlc[3] = df_ohlc[3]
					c_ohlc[7] = df_ohlc[7]
					c_ohlc[11] = df_ohlc[11]
				
			else:
				result.loc[c_ts] = c_ohlc
				c_ts = tl.utils.getNextTimestamp(period, c_ts, now=c_ts)
				c_ohlc = df.iloc[i].values

			if i == df.shape[0]-1:
				result.loc[c_ts] = c_ohlc

		return result


	def convertToLotsize(self, size):
		return size / 100000


	def convertToUnitSize(self, size):
		return int(round(size * 100) * 1000)


	def send_login(self):
		timestamp, signature = self.generateStreamHmac()

		payload = {
			"Id": self.brokerId,
			"Request": "Login",
			"Params": {
				"AuthType": "HMAC",
				"WebApiId": self.api_id,
				"WebApiKey": self.api_key,
				"Timestamp": timestamp,
				"Signature": signature,
				"DeviceId": "algowolf-server",
				"AppSessionId": "algowolf",
			}
		}

		if self.brokerId != "PARENT":
			try:
				self.account_client.send(payload)
			except Exception:
				pass
		# else:
		# 	self.price_client.send(payload)


	def reset_last_update(self):
		self.last_update = time.time()


	def check_handle(self, msg_id):
		if msg_id in self._handle:
			self._handle[msg_id][0](*self._handle[msg_id][1], **self._handle[msg_id][2])
			del self._handle[msg_id]


	def clean_handle(self):
		for k in list(self._handle.keys()):
			if time.time() - self._handle[k][3] > 30:
				del self._handle[k]


	def update_trades(self):
		if self.account_client is not None:
			msg_id = self.generateReference()

			self._handle[msg_id] = (self.reset_last_update, (), {}, time.time())
			self.account_client.send({
				"Id": msg_id,
				"Request": "Trades"
			})


	def _get_account_subscription_msg_id(self):
		for sub in self.account_subscriptions:
			return sub.msg_id
		return None


	def subscribe_account_updates(self, msg_id):
		existing_msg_id = self._get_account_subscription_msg_id()

		if existing_msg_id is None:
			self.account_subscriptions.append(Subscription(self, msg_id, 1))
		else:
			msg_id = existing_msg_id
		
		return msg_id


	def subscribe_price_updates(self, msg_id, instrument):
		return
		self.price_subscriptions[instrument] = Subscription(self, msg_id, 0)

		fxo_instrument = self.convertToFXOInstrument(instrument)
		self.price_client.send({
			"Id": msg_id,
			"Request": "FeedSubscribe",
			"Params": {
				"Subscribe": [{
					"Symbol": fxo_instrument,
					"BookDepth": 1
				}]
			}
		})


	def convertFromFXOInstrument(self, instrument):
		if instrument == "EURUSD":
			return "EUR_USD"
		else:
			return instrument


	def convertToFXOInstrument(self, instrument):
		if instrument == "EUR_USD":
			return "EURUSD"
		else:
			return instrument


	def convertToLotsize(self, size):
		return size / 100000


	def convertToUnitSize(self, size):
		return int(round(size * 100) * 1000)


	def getBrokerKey(self):
		return self.strategyId + '.' + self.brokerId

	def getDbPositions(self):
		positions = self.container.redis_client.hget(self.getBrokerKey(), "positions")
		if positions is None:
			positions = []
		else:
			positions = json.loads(positions)
		return positions

	def setDbPositions(self, positions):
		self.container.redis_client.hset(self.getBrokerKey(), "positions", json.dumps(positions))

	def appendDbPosition(self, new_position):
		positions = self.getDbPositions()
		positions.append(new_position)
		self.setDbPositions(positions)

	def deleteDbPosition(self, order_id):
		positions = self.getDbPositions()
		for i in range(len(positions)):
			if positions[i]["order_id"] == order_id:
				del positions[i]
				break
		self.setDbPositions(positions)

	def replaceDbPosition(self, position):
		positions = self.getDbPositions()
		for i in range(len(positions)):
			if positions[i]["order_id"] == position["order_id"]:
				positions[i] = position
				break
		self.setDbPositions(positions)

	def getPositionByID(self, order_id):
		for pos in self.getDbPositions():
			if pos["order_id"] == order_id:
				return pos
		return None
	
	def getDbOrders(self):
		orders = self.container.redis_client.hget(self.getBrokerKey(), "orders")
		if orders is None:
			orders = []
		else:
			orders = json.loads(orders)
		return orders

	def setDbOrders(self, orders):
		self.container.redis_client.hset(self.getBrokerKey(), "orders", json.dumps(orders))

	def appendDbOrder(self, new_order):
		orders = self.getDbOrders()
		orders.append(new_order)
		self.setDbOrders(orders)

	def deleteDbOrder(self, order_id):
		orders = self.getDbOrders()
		for i in range(len(orders)):
			if orders[i]["order_id"] == order_id:
				del orders[i]
				break
		self.setDbOrders(orders)

	def replaceDbOrder(self, order):
		orders = self.getDbOrders()
		for i in range(len(orders)):
			if orders[i]["order_id"] == order["order_id"]:
				orders[i] = order
				break
		self.setDbOrders(orders)

	def getOrderByID(self, order_id):
		for order in self.getDbOrders():
			if order["order_id"] == order_id:
				return order
		return None
	
	def _on_account_update(self, update):
		self._account_update_queue.append(update)


	def _handle_account_updates(self):
		
		while True:
			if len(self._account_update_queue):
				update = self._account_update_queue[0]
				del self._account_update_queue[0]

				try:
					item = update.get("Result")
					result = {}
					account_id = None
					handled_id = None

					if item is not None:
						event = item.get("Event")
						# On Filled Event
						if event == "Filled":
							# Position Updates
							account_id = str(item["Trade"]["AccountId"])
							if "Profit" in item:
								item["Trade"]["Price"] = item["Fill"]["Price"]
								result, handled_id = self._handle_order_fill_close(item["Trade"])
							else:
								result, handled_id = self._handle_order_fill_open(item["Trade"])
					
						# On Allocated Event
						elif event == "Allocated":
							if item["Trade"]["Type"] in ("Stop","Limit"):
								account_id = str(item["Trade"]["AccountId"])
								result, handled_id = self._handle_order_create(item["Trade"])

						# On Canceled Event
						elif event == "Canceled":
							account_id = str(item["Trade"]["AccountId"])
							result, handled_id = self._handle_order_cancel(item["Trade"])

						# On Modified Event
						elif event == "Modified":
							account_id = str(item["Trade"]["AccountId"])
							result, handled_id = self._handle_modify(item["Trade"])

						elif "Trades" in item:
							result = self._handle_trades(item["Trades"])

						if event is not None:
							print(f"[FXOpen._handle_account_updates] {update}")

					if len(result):
						print(f"HANDLED ID: {handled_id}")
						for sub in self.account_subscriptions:
							sub.onUpdate(result, account_id, handled_id)

				except Exception:
					print(traceback.format_exc())


	def heartbeat(self):
		return { "result": True }


	# TESTING
	def disconnectBroker(self):
		if self.account_client is not None:
			self.account_client.shutdown()

		return {
			'result': 'success'
		}
