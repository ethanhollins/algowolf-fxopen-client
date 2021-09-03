import datetime
import json
from app import tradelib as tl
# from app.error import BrokerException


class Position(dict):

	def __init__(self, 
		broker, order_id, account_id, product, order_type, direction, lotsize, 
		entry_price=None, sl=None, tp=None, open_time=None, sl_id=None, tp_id=None
	):
		
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
		self.sl_id = sl_id
		self.tp = tp
		self.tp_id = tp_id

		if open_time:
			self.open_time = int(open_time)
		else:
			self.open_time = int(tl.utils.convertTimeToTimestamp(datetime.datetime.utcnow()))

		self.close_time = None

	@classmethod
	def fromDict(cls, broker, pos):
		# Hidden Variable
		res = cls(
			broker,
			pos['order_id'],
			pos['account_id'],
			pos['product'],
			pos['order_type'],
			pos['direction'],
			pos['lotsize']
		)
		# Dictionary Variables
		for k, v in pos.items():
			res.__setattr__(k, v)

		return res

	@classmethod
	def fromOrder(cls, broker, order):
		if order.order_type == tl.LIMIT_ORDER:
			order_type = tl.LIMIT_ENTRY
		elif order.order_type == tl.STOP_ORDER:
			order_type = tl.STOP_ENTRY

		res = cls(
			broker,
			order['broker'],
			order['order_id'],
			order['account_id'],
			order['product'],
			order_type,
			order['direction'],
			order['lotsize']
		)
		# Dictionary Variables
		for k, v in order.items():
			if k != 'order_type':
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
		return json.dumps(self, indent=2)

	def close(self, lotsize=None, override=False):
		if not lotsize: lotsize = self.lotsize

		# Lotsize validation
		lotsize = min(self.lotsize, lotsize)
		# Call to broker
		return self._broker.deletePosition(self, lotsize, override=override)


	def modify(self,
		sl_range=None, tp_range=None,
		sl_price=None, tp_price=None,
		override=False
	):
		# Convert to price
		if sl_range is not None:
			if self.direction == tl.LONG:
				sl = round(self.entry_price - tl.utils.convertToPrice(sl_range), 5)
			else:
				sl = round(self.entry_price + tl.utils.convertToPrice(sl_range), 5)
		elif sl_price is not None:
			sl = sl_price
		else:
			sl = self.sl

		if tp_range is not None:
			if self.direction == tl.LONG:
				tp = round(self.entry_price + tl.utils.convertToPrice(tp_range), 5)
			else:
				tp = round(self.entry_price - tl.utils.convertToPrice(tp_range), 5)
		elif tp_price is not None:
			tp = tp_price
		else:
			tp = self.tp

		# Check for min requirements

		# Call to broker
		return self._broker.modifyPosition(self, sl, tp, override=override)


	def getProfit(self):
		ask = self._broker.getAsk(self.product)
		bid = self._broker.getBid(self.product)

		if direction == tl.LONG:
			if self.close:
				return round(tl.utils.convertToPips(self.close - self.entry_price), 2)
			else:
				return round(tl.utils.convertToPips(bid - self.entry_price), 2)
		else:
			if self.close:
				return round(tl.utils.convertToPips(self.entry_price - self.close), 2)
			else:
				return round(tl.utils.convertToPips(self.entry_price - ask), 2)

