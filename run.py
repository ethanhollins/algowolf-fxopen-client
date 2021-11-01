import sys
import socketio
import os
import json
import time
import traceback
import shortuuid
import zmq
from redis import Redis
from app.fxopen import FXOpen
from threading import Thread

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

'''
Utilities
'''
class UserContainer(object):

	def __init__(self):
		self.parent = None
		self.users = {}
		self.add_user_queue = []
		self.redis_client = Redis(host='redis', port=6379, password="dev")
		self._setup_zmq_connections()


	def _setup_zmq_connections(self):
		self.zmq_context = zmq.Context()

		self.zmq_req_socket = self.zmq_context.socket(zmq.DEALER)
		self.zmq_req_socket.connect("tcp://zmq_broker:5557")

		self.zmq_pull_socket = self.zmq_context.socket(zmq.PULL)
		self.zmq_pull_socket.connect("tcp://zmq_broker:5559")

		self.zmq_poller = zmq.Poller()
		self.zmq_poller.register(self.zmq_pull_socket, zmq.POLLIN)
		self.zmq_poller.register(self.zmq_req_socket, zmq.POLLIN)


	def setParent(self, parent):
		self.parent = parent


	def getParent(self):
		return self.parent


	def addUser(self, user_id, strategy_id, broker_id, api_id, api_key, api_secret, is_demo, is_parent):
		if broker_id not in self.users:
			self.users[broker_id] = FXOpen(self, user_id, strategy_id, broker_id, api_id, api_key, api_secret, is_demo)
			if is_parent:
				self.parent = self.users[broker_id]

		return self.users[broker_id]


	def deleteUser(self, port):
		if port in self.users:
			self.users[port].stop()
			del self.users[port]


	def getUser(self, port):
		return self.users.get(port)

	
	def addToUserQueue(self):
		_id = shortuuid.uuid()
		self.add_user_queue.append(_id)
		while self.add_user_queue[0] != _id:
			time.sleep(0.1)


	def popUserQueue(self):
		del self.add_user_queue[0]


def getConfig():
	path = os.path.join(ROOT_DIR, 'instance/config.json')
	if os.path.exists(path):
		with open(path, 'r') as f:
			return json.load(f)
	else:
		raise Exception('Config file does not exist.')


'''
Initialize
'''

config = getConfig()
# sio = socketio.Client()
user_container = UserContainer()

'''
Socket IO functions
'''

def sendResponse(msg_id, res):
	res = {
		"type": "broker_reply",
		"message": {
			'msg_id': msg_id,
			'result': res
		}
	}

	user_container.zmq_req_socket.send_json(res)


# def sendResponse(msg_id, res):
# 	res = {
# 		'msg_id': msg_id,
# 		'result': res
# 	}

# 	sio.emit(
# 		'broker_res', 
# 		res, 
# 		namespace='/broker'
# 	)


def onAddUser(user_id, strategy_id, broker_id, api_key, api_id, api_secret, is_demo, accounts, is_parent, is_dummy):
	user_container.addToUserQueue()
	try:
		user = user_container.addUser(user_id, strategy_id, broker_id, api_id, api_key, api_secret, is_demo, is_parent)
	except Exception:
		print(traceback.format_exc())
	finally:
		user_container.popUserQueue()
		
	return {
		'completed': True
	}


def onDeleteUser(port):
	user_container.deleteUser(port)

	return {
		'completed': True
	}


def getUser(port):
	return user_container.getUser(port)


def getParent():
	return user_container.getParent()


def reconnect_user(user):
	user.account_client.connect()
	user.send_login()
	user.update_trades()


def onFXODisconnect():
	ping_timer = time.time()
	while True:
		if time.time() - ping_timer > 30:
			ping_timer = time.time()
			try:
				for broker_id in list(user_container.users.keys()):
					user = user_container.users[broker_id]
					if user.account_client is not None:
						print(f"[FXOpen.onFXODisconnect] ({broker_id}) Connected: {user.account_client.is_connected}", flush=True)
						if not user.account_client.is_connected:
							reconnect_user(user)
							
						else:
							user.update_trades()
							user.clean_handle()

							# if time.time() - user.last_update > 90:
							# 	reconnect_user(user)
							pass

					time.sleep(0.1)

			except Exception:
				print(f"[FXOpen.onFXODisconnect] {traceback.format_exc()}", flush=True)
				
		# try:
		# 	if not user.price_client.is_connected:
		# 		user.price_client.connect()
		# except Exception:
		# 	print(traceback.format_exc(), flush=True)

		time.sleep(1)


# @sio.on('connect', namespace='/broker')
# def onConnect():
# 	print('CONNECTED!', flush=True)


# @sio.on('disconnect', namespace='/broker')
# def onDisconnect():
# 	print('DISCONNECTED', flush=True)


# @sio.on('broker_cmd', namespace='/broker')
def onCommand(data):
	print(f'COMMAND: {data}', flush=True)

	try:
		cmd = data.get('cmd')
		broker = data.get('broker')
		broker_id = data.get('broker_id')

		if broker_id is None:
			user = getParent()
		else:
			user = getUser(broker_id)

		if broker == 'fxopen':
			res = {}
			if cmd == 'add_user':
				res = onAddUser(*data.get('args'), **data.get('kwargs'))

			elif cmd == 'delete_user':
				res = onDeleteUser(*data.get('args'), **data.get('kwargs'))

			elif cmd == '_download_historical_data_broker':
				res = user._download_historical_data_broker(*data.get('args'), **data.get('kwargs'))

			elif cmd == 'subscribe_account_updates':
				res = user.subscribe_account_updates(*data.get('args'), **data.get('kwargs'))

			elif cmd == 'subscribe_price_updates':
				res = user.subscribe_price_updates(*data.get('args'), **data.get('kwargs'))

			elif cmd == '_get_all_positions':
				res = user._get_all_positions(*data.get('args'), **data.get('kwargs'))

			elif cmd == '_get_all_orders':
				res = user._get_all_orders(*data.get('args'), **data.get('kwargs'))

			elif cmd == 'createPosition':
				res = user.createPosition(*data.get('args'), **data.get('kwargs'))

			elif cmd == 'modifyPosition':
				res = user.modifyPosition(*data.get('args'), **data.get('kwargs'))

			elif cmd == 'deletePosition':
				res = user.deletePosition(*data.get('args'), **data.get('kwargs'))

			elif cmd == 'getAllAccounts':
				res = user.getAllAccounts(*data.get('args'), **data.get('kwargs'))

			elif cmd == 'getAccountInfo':
				res = user.getAccountInfo(*data.get('args'), **data.get('kwargs'))

			elif cmd == 'createOrder':
				res = user.createOrder(*data.get('args'), **data.get('kwargs'))

			elif cmd == 'modifyOrder':
				res = user.modifyOrder(*data.get('args'), **data.get('kwargs'))

			elif cmd == 'deleteOrder':
				res = user.deleteOrder(*data.get('args'), **data.get('kwargs'))
			
			elif cmd == 'authCheck':
				res = user.authCheck(*data.get('args'), **data.get('kwargs'))

			elif cmd == 'disconnectBroker':
				res = user.disconnectBroker(*data.get('args'), **data.get('kwargs'))

			elif cmd == 'heartbeat':
				if user is not None:
					res = user.heartbeat(*data.get('args'), **data.get('kwargs'))
				else:
					res = { "result": False }

			sendResponse(data.get('msg_id'), res)

	except Exception as e:
		print(traceback.format_exc(), flush=True)
		sendResponse(data.get('msg_id'), {
			'error': str(e)
		})


# def createApp():
# 	print('CREATING APP')
# 	while True:
# 		try:
# 			sio.connect(
# 				config['STREAM_URL'], 
# 				headers={
# 					'Broker': 'fxopen'
# 				}, 
# 				namespaces=['/broker']
# 			)
# 			break
# 		except Exception:
# 			pass

# 	return sio

def run():
	while True:
		socks = dict(user_container.zmq_poller.poll())

		if user_container.zmq_pull_socket in socks:
			message = user_container.zmq_pull_socket.recv_json()
			print(f"[ZMQ_PULL] {message}")
			onCommand(message)

		if user_container.zmq_req_socket in socks:
			message = user_container.zmq_req_socket.recv()
			print(f"[ZMQ_REQ] {message}")


if __name__ == '__main__':
	# sio = createApp()
	# print('DONE')

	Thread(target=run).start()
	onFXODisconnect()
