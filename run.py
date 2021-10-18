import sys
import socketio
import os
import json
import time
import traceback
from app.fxopen import FXOpen

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

'''
Utilities
'''
class UserContainer(object):

	def __init__(self, sio):
		self.sio = sio
		self.parent = None
		self.users = {}

	def setParent(self, parent):
		self.parent = parent


	def getParent(self):
		return self.parent


	def addUser(self, user_id, broker_id, api_id, api_key, api_secret, is_demo, is_parent):
		if broker_id not in self.users:
			self.users[broker_id] = FXOpen(self, user_id, broker_id, api_id, api_key, api_secret, is_demo)
			if is_parent:
				self.parent = self.users[broker_id]

		return self.users[broker_id]


	def deleteUser(self, port):
		if port in self.users:
			self.users[port].stop()
			del self.users[port]


	def getUser(self, port):
		return self.users.get(port)


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
sio = socketio.Client()
user_container = UserContainer(sio)

'''
Socket IO functions
'''

def sendResponse(msg_id, res):
	res = {
		'msg_id': msg_id,
		'result': res
	}

	sio.emit(
		'broker_res', 
		res, 
		namespace='/broker'
	)


def onAddUser(user_id, broker_id, api_key, api_id, api_secret, is_demo, accounts, is_parent, is_dummy):
	user = user_container.addUser(user_id, broker_id, api_id, api_key, api_secret, is_demo, is_parent)
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
							# user.update_trades()
							# user.clean_handle()

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


@sio.on('connect', namespace='/broker')
def onConnect():
	print('CONNECTED!', flush=True)


@sio.on('disconnect', namespace='/broker')
def onDisconnect():
	print('DISCONNECTED', flush=True)


@sio.on('broker_cmd', namespace='/broker')
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


def createApp():
	print('CREATING APP')
	while True:
		try:
			sio.connect(
				config['STREAM_URL'], 
				headers={
					'Broker': 'fxopen'
				}, 
				namespaces=['/broker']
			)
			break
		except Exception:
			pass

	return sio


if __name__ == '__main__':
	sio = createApp()
	print('DONE')

	onFXODisconnect()
