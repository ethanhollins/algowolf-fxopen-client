import datetime
import json
import math
import time
from app import tradelib as tl
# from app.error import BrokerException

class Order(dict):

	def __init__(self, broker, order_id, account_id, product, order_type, direction, lotsize, entry_price=None, sl=None, tp=None, open_time=None):
		
		# Hidden Variable
		super().__setattr__('_broker', broker)

		# Dictionary Variables
		self.order_id = order_id
		self.account_id = account_id
		self.product = product
		self.order_type = order_type
		self.direction = direction
		self.lotsize = lotsize
		self.entry_price = entry_price
		self.close_price = None
		self.sl = sl
		self.tp = tp

		if open_time:
			self.open_time = int(open_time)
		else:
			self.open_time = math.floor(time.time())
		self.close_time = None

	@classmethod
	def fromDict(cls, broker, order):
		# Hidden Variable
		res = cls(
			broker,
			order['order_id'],
			order['account_id'],
			order['product'],
			order['order_type'],
			order['direction'],
			order['lotsize']
		)
		# Dictionary Variables
		for k, v in order.items():
			res.__setattr__(k, v)

		return res

	def __getattr__(self, key):
		if key != '_broker':
			try:
				return self[key]
			except Exception:
				pass
				
		super().__getattr__(key)

	def __setattr__(self, key, value):
		if key != '_broker':
			self[key] = value
		else:
			raise Exception('`_broker` is a protected variable.')

	def __str__(self):
		cpy = self.copy()
		cpy['open_time'] = int(cpy['open_time'])
		return json.dumps(cpy, indent=2)


	def responseFriendly(self):
		res_friendly = dict(self)
		del res_friendly['broker']
		return res_friendly


	def close(self, override=False):
		return self.cancel(override=override)


	def cancel(self, override=False):
		# Call to broker
		return self._broker.deleteOrder(self, override=override)


	def modify(self, 
		lotsize=None,
		entry_range=None, entry_price=None,
		sl_range=None, tp_range=None,
		sl_price=None, tp_price=None,
		override=False
	):
		if not lotsize:
			lotsize = self.lotsize

		# Convert to price
		if entry_range:
			if direction == tl.LONG:
				entry = round(self.entry_price + tl.utils.convertToPrice(entry_range), 5)
			else:
				entry = round(self.entry_price - tl.utils.convertToPrice(entry_range), 5)
		elif entry_price:
			entry = entry_price
		else:
			entry = self.entry_price

		if sl_range:
			if direction == tl.LONG:
				sl = round(self.entry_price - tl.utils.convertToPrice(sl_range), 5)
			else:
				sl = round(self.entry_price + tl.utils.convertToPrice(sl_range), 5)
		elif sl_price:
			sl = sl_price
		else:
			sl = self.sl

		if tp_range:
			if direction == tl.LONG:
				tp = round(self.entry_price + tl.utils.convertToPrice(tp_range), 5)
			else:
				tp = round(self.entry_price - tl.utils.convertToPrice(tp_range), 5)
		elif tp_price:
			tp = tp_price
		else:
			tp = self.tp

		# Check for min requirements

		# Call to broker
		return self._broker.modifyOrder(self, lotsize, entry, sl, tp, override=override)
		

	def modifyEntry(self, entry_range=None, entry_price=None):
		return self.modify(entry_range=entry_range, entry_price=entry_price)

	def modifySL(self, sl_range=None, sl_price=None):
		return self.modify(sl_range=sl_range, sl_price=sl_price)

	def modifyTP(self, tp_range=None, tp_price=None):
		return self.modify(tp_range=tp_range, tp_price=tp_price)

	def update(self, order):
		self.lotsize = order.lotsize
		self.entry_price = order.entry_price
		self.sl = order.sl
		self.tp = order.tp
