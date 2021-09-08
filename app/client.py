import traceback
import time
import json
import os
import ssl
import socket
import array
import six
import struct
import websocket
from struct import pack, unpack
from json.decoder import JSONDecodeError
from threading import Thread


CONNECT_EVENT = 'connect'
DISCONNECT_EVENT = 'disconnect'
MESSAGE_EVENT = 'message'

OPCODE_TEXT = 0x1

class Client(object):

	def __init__(self, broker, is_demo=False, client_type="account", timeout=None):
		print("[FXOpen.Client] Init", flush=True)
		
		self._events = {
			CONNECT_EVENT: [],
			DISCONNECT_EVENT: [],
			MESSAGE_EVENT: []
		}

		self.broker = broker
		self.is_demo = is_demo
		self.host = "marginalttdemowebapi.fxopen.net" if is_demo else "marginalttlivewebapi.fxopen.net"

		self.is_connected = False

		self.client_type = client_type
		if client_type == "account":
			self.msg_queue = self.broker.account_msg_queue
			self.on_message = self.broker.on_account_message
			self.port = 3001
		else:
			self.msg_queue = self.broker.price_msg_queue
			self.on_message = self.broker.on_price_message
			self.port = 3000

		# self.ssock = websocket.WebSocket()
		self.ssock = None

		Thread(target=self._perform_send).start()
	

	def _mask(self, _m, _d):
		for i in range(len(_d)):
			_d[i] ^= _m[i % 4]

		if six.PY3:
			return _d.tobytes()
		else:
			return _d.tostring()


	def get_masked(self, data):
		mask_key = os.urandom(4)
		if data is None:
			data = ""

		bin_mask_key = mask_key
		if isinstance(mask_key, six.text_type):
			bin_mask_key = six.b(mask_key)

		if isinstance(data, six.text_type):
			data = six.b(data)

		_m = array.array("B", bin_mask_key)
		_d = array.array("B", data)
		s = self._mask(_m, _d)

		if isinstance(mask_key, six.text_type):
			mask_key = mask_key.encode('utf-8')
		return mask_key + s


	def ws_encode(self, data="", opcode=OPCODE_TEXT, mask=1):
		if opcode == OPCODE_TEXT and isinstance(data, six.text_type):
			data = data.encode('utf-8')

		length = len(data)
		fin, rsv1, rsv2, rsv3, opcode = 1, 0, 0, 0, opcode

		frame_header = chr(fin << 7 | rsv1 << 6 | rsv2 << 5 | rsv3 << 4 | opcode)

		if length < 0x7e:
			frame_header += chr(mask << 7 | length)
			frame_header = six.b(frame_header)
		elif length < 1 << 16:
			frame_header += chr(mask << 7 | 0x7e)
			frame_header = six.b(frame_header)
			frame_header += struct.pack("!H", length)
		else:
			frame_header += chr(mask << 7 | 0x7f)
			frame_header = six.b(frame_header)
			frame_header += struct.pack("!Q", length)

		if not mask:
			return frame_header + data
		return frame_header + self.get_masked(data)


	def ws_decode(self, data):
		"""
		ws frame decode.
		:param data:
		:return:
		"""
		if len(data) > 1:
			try:
				data[:2].decode('utf-8')
				return array.array('B', data).tobytes()
				
			except UnicodeDecodeError:
				length = data[1] & 127
				index = 2
				if length < 126:
					index = 2
				if length == 126:
					index = 4
				elif length == 127:
					index = 10
				return array.array('B', data[index:]).tobytes()

		else:
			return array.array('B', data).tobytes()


	def connect(self, timeout=None):
		if self.ssock is not None:
			try:
				self.ssock.close()
			except Exception:
				pass

		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		if timeout:
			sock.settimeout(timeout)

		PEM_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "cert.pem")
		self.ssock = ssl.wrap_socket(sock, ssl_version=ssl.PROTOCOL_TLS, certfile=PEM_PATH, keyfile=PEM_PATH)
		self.ssock.connect((self.host, self.port))

		handshake = f'GET / HTTP/1.1\r\nHost: {self.host}\r\nUpgrade: websocket\r\nConnection: ' \
            f'Upgrade\r\nSec-WebSocket-Key: {self.broker.generateReference()}\r\nOrigin: https://api.algowolf.com\r\nSec-WebSocket-Protocol: ' \
            'echo\r\n' \
            'Sec-WebSocket-Version: 13\r\n\r\n'
		
		self.ssock.send(handshake.encode())
		print(self.ssock.recv(1024))
		# print(f"[Client.connect] connecting: {'wss://' + self.host + ':' + str(self.port)}")
		# self.ssock.connect("wss://" + self.host + ':' + str(self.port))

		self.is_connected = True

		print('[FXOpen] Connected', flush=True)

		Thread(target=self.receive).start()


	def reconnect(self):
		while True:
			print('[FXOpen] Attempting reconnect.', flush=True)
			try:
				self.connect()
				break
			except Exception:
				time.sleep(1)


	def _perform_send(self):
		print(f"[FXOpen] Perform Send. {self.client_type}", flush=True)

		while True:
			try:
				if len(self.msg_queue):
					payload, msgid = self.msg_queue[0]
					del self.msg_queue[0]

					if self.ssock is not None:
						# sock_msg = json.dumps(payload).encode()
						print(f"[FXOpen] send1 {json.dumps(payload)}", flush=True)
						sock_msg = self.ws_encode(data=json.dumps(payload), opcode=OPCODE_TEXT)

						print(f"[FXOpen] send2 {sock_msg}", flush=True)

						self.ssock.sendall(sock_msg)
						# self.ssock.send(sock_msg)

					else:
						print('[FXOpen] Not connected.')

					self.broker.req_count += 1

				if self.broker.req_count >= 50:
					time.sleep(1)
					self.broker.req_timer = time.time()
					self.broker.req_count = 0

				elif time.time() - self.broker.req_timer > 1:
					self.broker.req_timer = time.time()
					self.broker.req_count = 0

			except Exception:
				print(f'[SC] SEND ERROR:\n{traceback.format_exc()}', flush=True)

			time.sleep(0.001)


	def send(self, payload, msgid=''):
		self.msg_queue.append((payload, msgid))


	def read_msg(self, msg):
		try:
			data = json.loads(msg)
			return data
		except JSONDecodeError:
			return None


	def match_buffer(self, buffer, indicies=[], msg=''):
		print(f"[match_buffer] {len(buffer)}, {indicies}", flush=True)
		for i in range(len(buffer)):
			json_data = self.read_msg(msg + buffer[i])
			if json_data is not None:
				print(f"[match_buffer] FOUND", flush=True)
				return json_data, indicies
			else:
				new_buffer = buffer[:i] + buffer[i+1:]
				new_indicies = indicies + [i]
				result_data, result_indicies = self.match_buffer(new_buffer, new_indicies, msg + buffer[i])
				if result_data is not None and result_indicies is not None:
					print(f"[match_buffer] FOUND2", flush=True)
					return result_data, result_indicies

		print(f"[match_buffer] NOPE", flush=True)
		return None, None


	def receive(self):
		print("[FXOpen] receive", flush=True)
		
		# buffer=[]
		buffer=''
		try:
			while True:
				try:
					recv = self.ws_decode(self.ssock.recv()).decode()
					if len(recv) == 0:
						break
					
					msg = self.read_msg(recv)
					if msg is None:
						# buffer.append(recv)
						buffer += recv
					else:
						self.on_message(msg)


				except Exception:
					print(f'[FXOpen] ERROR:\n{traceback.format_exc()}', flush=True)
					break

				if len(buffer):
					# print(f"BUFFER: {len(buffer)}, {buffer}", flush=True)
					
					# msg, indicies = self.match_buffer(buffer)
					# end_idx = 0
					# messages = []
					# while end_idx != -1:
					# 	end_idx = buffer.find("}{")
					# 	if end_idx == -1:
					# 		msg = self.read_msg(buffer)
					# 		if msg is not None:
					# 			messages.append(msg)
					# 			buffer = ''
					# 	else:
					# 		msg = self.read_msg(buffer[:end_idx+1])
					# 		if msg is not None:
					# 			messages.append(msg)
					# 		buffer = buffer[end_idx+1:]
					
					# for msg in messages:
					# 	try:
					# 		self.on_message(msg)
					# 	except Exception:
					# 		print(traceback.format_exc())

					msg = self.read_msg(buffer)
					if msg is not None:
						print(f"BUFFER MSG COMPLETE: {len(buffer)}", flush=True)
						buffer = ''
						try:
							self.on_message(msg)
						except Exception:
							print(traceback.format_exc())
				
				time.sleep(0.001)

		except Exception:
			print(f'[FXOpen] (client.receive) Error:\n{traceback.format_exc()}', flush=True)

		print('[FXOpen] Disconnected...', flush=True)

		self.is_connected = False


	def stop(self):
		self.ssock.close()

	
	def shutdown(self):
		try:
			self.ssock.shutdown(1)
		except Exception:
			print(traceback.format_exc(), flush=True)


	def event(self, event_type, func):
		if callable(func):
			if event_type == CONNECT_EVENT:
				self._events[CONNECT_EVENT].append(func)

			elif event_type == DISCONNECT_EVENT:
				self._events[DISCONNECT_EVENT].append(func)

			elif event_type == MESSAGE_EVENT:
				self._events[MESSAGE_EVENT].append(func)

