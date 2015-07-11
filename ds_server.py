import threading
import socket
import ConfigParser
import json
import datetime
import time
import sqlite3
import os
from flask import Flask, session, redirect, url_for, escape, request, render_template
from multiprocessing import Process
from socket_helper import SocketHelper
from datetime import datetime
import ssl


class Server(SocketHelper):
	def __init__(self, name):
		super(Server, self).__init__(name)
		self.channel = 'server'
		self.users = []
		self.token = 'greo485jht9orjoghuh'
		self.servers = {} # uid, connect_time
				
	def get_users(self):
		conn = sqlite3.connect(self.db_path)
		conn.row_factory = sqlite3.Row
		cur = conn.cursor()
		
		fetch_all_users_query = "select * from users"
		cur.execute(fetch_all_users_query)
		rows = cur.fetchall()
		
		users_list = {}
		for row in rows:
			users_list[row['username']] = row['server_name']
		conn.close()
		
		return users_list
		
	def get_servers(self):
		conn = sqlite3.connect(self.db_path)
		conn.row_factory = sqlite3.Row
		cur = conn.cursor()
		
		fetch_all_users_query = "select * from servers"
		cur.execute(fetch_all_users_query)
		rows = cur.fetchall()
		
		#server_name : {'name', 'host', 'port', 'heartbeat', 'max_user', 'total_user', 'last_heartbeat', 'hold_token', 'connect_time'}
		servers_list = {}
		for row in rows:
			servers_list[row['name']] = {'name' : row['name'],
												'host' : row['host'],
												'port' : row['port'],
												'heartbeat' : row['heartbeat'],
												'max_user' : row['max_user'],
												'total_user' : row['total_user'],
												'last_heartbeat' : row['last_heartbeat'],
												'hold_token' : row['hold_token'],
												'connect_time' : row['connect_time']}
		conn.close()
		
		return servers_list
		
	def remove_user(self, username):
		conn = sqlite3.connect(self.db_path)
		conn.row_factory = sqlite3.Row
		cur = conn.cursor()
		
		remove_query = "delete from users where username=?";
		cur.execute(remove_query, (username))
		conn.commit()
		conn.close()
		
	def update_user(self, username, new_server):
		conn = sqlite3.connect(self.db_path)
		conn.row_factory = sqlite3.Row
		cur = conn.cursor()
		
		remove_query = "update users set server_name=? where username=?";
		cur.execute(remove_query, (username, new_server))
		conn.commit()
		conn.close()
		
	def clear_all_users(self):
		conn = sqlite3.connect(self.db_path)
		conn.row_factory = sqlite3.Row
		cur = conn.cursor()
		
		remove_query = "delete from users";
		cur.execute(remove_query)
		conn.commit()
		conn.close()
		
	def remove_server(self, server_name):
		conn = sqlite3.connect(self.db_path)
		conn.row_factory = sqlite3.Row
		cur = conn.cursor()
		
		remove_query = "delete from servers where name=?";
		cur.execute(remove_query, (server_name,))
		conn.commit()
		conn.close()
	
	def clear_all_servers(self):
		conn = sqlite3.connect(self.db_path)
		conn.row_factory = sqlite3.Row
		cur = conn.cursor()
		
		remove_query = "delete from servers";
		cur.execute(remove_query)
		conn.commit()
		conn.close()
	
	def update_server(self, server_name, new_values):
		conn = sqlite3.connect(self.db_path)
		conn.row_factory = sqlite3.Row
		cur = conn.cursor()
		
		update_values = ''
		for key in new_values:
			update_values = update_values + key + '=:' + key + ','
		update_values = update_values + 'name=:server_name_condition'
		
		new_values['server_name_condition'] = server_name
		new_values['server_name'] = server_name
		
		remove_query = "update servers set " + update_values + " where name=:server_name";
		cur.execute(remove_query, new_values)
		conn.commit()
		conn.close()

	#server_name : {'name', 'host', 'port', 'heartbeat', 'max_user', 'total_user', 'last_heartbeat', 'hold_token', 'connect_time'}
	def add_server(self, server_name, host, port, heartbeat, max_user):
		conn = sqlite3.connect(self.db_path)
		conn.row_factory = sqlite3.Row
		cur = conn.cursor()
		current_time = int(time.time())
		insert_query = "insert into servers (name, host, port, heartbeat, max_user, total_user, last_heartbeat, hold_token, connect_time) \
						values (:server_name, :host, :port, :heartbeat, :max_user, :total_user, :last_heartbeat, :hold_token, :connect_time)";
		cur.execute(insert_query, {	'server_name' : server_name, 'host':host, 'port':port, 
									'heartbeat':heartbeat, 'max_user':max_user, 'total_user':0, 
									'last_heartbeat':current_time, 'hold_token':'False', 'connect_time':current_time})
		conn.commit()
		conn.close()	
		
	def get_token(self):
		return self.token
		
	def get_token_holder(self):
		return self.get_variable('token_holder')
		
	def set_token_holder(self, token_holder):
		return self.set_variable('token_holder', token_holder)
			
	def add_user(self,username, old_server='', new_server=''):
		
		servers = self.get_servers()
		
		if new_server == '':
			new_server = self.get_server()
		
		conn = sqlite3.connect(self.db_path)
		conn.row_factory = sqlite3.Row
		cur = conn.cursor()
		
		insert_query = "insert into users (username, server_name) values (:username, :server_name)";
		cur.execute(insert_query, {'username' : username, 'server_name' : new_server})
		conn.commit()
		conn.close()
		
		self.update_server(new_server, {'total_user' : servers[new_server]['total_user'] + 1})
		
		if old_server != '':
			self.update_server(old_server, {'total_user' : servers[old_server]['total_user'] - 1})
			
		data = {'action' : 'RETURN_SIGNAL', 'data' : {'signal' : 'ADDED_TO_USERS_LIST'}}
		return data
	
	def process_heart_beat(self, data):
		'''
		Process a heartbeat signal from a server:
			If this is a new server, add the server to list
			If this is an old server, update last_heartbeat variable to current time
		'''
		servers = self.get_servers()
		current_time = int(time.time())
		token_holder = self.get_variable('token_holder')
		last_token_check = int(self.get_variable('last_token_check'))
		
		if data['name'] not in servers:
			self.add_server(data['name'], data['host'], data['port'], data['heartbeat'], data['max_user'])
			
			#check if the only server => send token
			print('New server connected: ' + data['name'] + ' at ' + data['host'] + ':' + str(data['port']))
		else:
			self.update_server(data['name'], {'last_heartbeat':current_time})
		
		if token_holder == '' or (current_time - last_token_check) > self.token_check_time:
			self.set_variable('last_token_check', str(current_time))
			print('send token to server ' + data['name'])
			self.set_variable('token_holder', data['name'])
			data = {'action' : 'TOKEN', 'data' : {'token' : self.token}}
		else:	
			data = {'action' : 'HEARTBEAT_OK', 'data' : {}}
			
		return json.dumps(data)
	
	def check_dead_servers(self):
		removed_items = []
		current_time = int(time.time())
		servers = self.get_servers()
		token_holder = self.get_variable('token_holder')
		last_token_check = int(self.get_variable('last_token_check'))
		
		for server_name, server in servers.iteritems():
			if (current_time - server['last_heartbeat']) > self.heartbeat_timeout:
				print('' + server_name + ' inactive for ' + str(self.heartbeat_timeout) + ' seconds, removed')
				removed_items.append(server_name)
				
		# TODO: move users from that server to other servers
		for item in removed_items:
			if item in servers:
				self.remove_server(item)
			if token_holder == item:
				self.set_variable('token_holder', '')
				
		if len(servers) == 0:
			self.set_variable('token_holder', '')
		
		if len(servers) > 0 and (token_holder == '' or current_time-last_token_check > self.token_check_time):
			sorted_servers = sorted(servers.values(), key=lambda server: server['connect_time'])
			self.set_variable('token_server', sorted_servers[0]['name'])
			data = {'action' : 'TOKEN', 'data' : {'token' : self.token}}
			token_server = sorted_servers[0]
			# send token to next server
			try:
				self.send_request(token_server['host'], token_server['port'], '/token', data, 'keys/' + str(token_server['name']))
			except:
				pass
			
		if not self.is_shutting_down:
			threading.Timer(1, self.check_dead_servers).start()
	
	def get_server(self, dead_servers=[]):
		servers = self.get_servers()
		
		conn = sqlite3.connect(self.db_path)
		conn.row_factory = sqlite3.Row
		cur = conn.cursor()
		
		fetch_all_users_query = "SELECT b.name \
								FROM servers b \
								left join users a on a.server_name = b.name \
								group by a.server_name \
								order by count(*)/b.total_user asc, count(*) asc, b.connect_time \
								limit 1"
		cur.execute(fetch_all_users_query)
		row = cur.fetchone()
		return row['name']
		conn.close()
	
	
	def next_token_server(self, data):
		''' sort self.servers based on connect time, remove server name in data['dead_servers']
		get the index of the requested server
		choose the next item, if the requested server is the highest, return 0
		'''
		current_time = int(time.time())
		servers = self.get_servers()
		self.set_variable('last_token_check', str(current_time))
		token_holder = self.get_variable('token_holder')
		sorted_servers = sorted(servers.values(), key=lambda server: server['connect_time'])
		requested_server_name = str(data['server_name'])
		print(requested_server_name)
		if token_holder != requested_server_name:
			self.update_server(str(requested_server_name), {'hold_token':'False'})
			return json.dumps({	'action' : 'REMOVE_TOKEN', 
								'data' : {	}})
		dead_servers = data['dead_servers']
		requested_server_found = False
		
		for server in sorted_servers:
			if requested_server_found and server['name'] not in dead_servers:
				token_holder = server['name']
				self.set_variable('token_holder', server['name'])
				self.update_server(str(token_holder), {'hold_token':'False'})
				self.update_server(str(server['name']), {'hold_token':'True'})
				return json.dumps({	'action' : 'SEND_NEXT_TOKEN_SERVER', 
									'data' : {	'server_name' : servers[server['name']]['name'],
												'host' : servers[server['name']]['host'],
												'port' : servers[server['name']]['port']}})
				
			if server['name'] == requested_server_name:
				requested_server_found = True
		for server in sorted_servers:
			if requested_server_found and server['name'] not in dead_servers:
				self.set_variable('token_holder', server['name'])
				self.update_server(str(token_holder), {'hold_token':'False'})
				self.update_server(str(server['name']), {'hold_token':'True'})
				return json.dumps({	'action' : 'SEND_NEXT_TOKEN_SERVER', 
									'data' : {	'server_name' : servers[server['name']]['name'],
												'host' : servers[server['name']]['host'],
												'port' : servers[server['name']]['port']}})
	
				
gateway = Gateway(__name__)		


@gateway.route('/', methods=['POST', 'GET'])
def index():
	
	if request.method == 'POST' and 'username' in request.form.keys() and request.form['username'].strip() != '':
		session['username'] = request.form['username'].strip()
		username = request.form['username'].strip()
	if 'username' in session:
		username = session['username']
		#check if the user is new, if new then assign the user to a server
		users = gateway.get_users()
		if username not in users:
			gateway.add_user(username)
		
		#go to chat page
		#chat_texts = gateway.get_list(session['username'])
		#users_list = gateway.user_lists.keys()
		#data = {'username' : username, 'chat_texts' : chat_texts, 'users_list' : users_list}
		data = {'username' : username}
		return render_template('chat.html', data=data)
	
	return render_template('login.html')

@gateway.route('/send_chat', methods=['POST'])
def send_chat():
	# check if the user is logged in, if not send redirect signal
	if 'username' not in session:
		return '{"action" : "REDIRECT", "data":{"href":"/"}}'
	users = gateway.get_users()
	username = session['username']
	if username not in users:
		gateway.add_user(username)
	
	#init variables
	chat_text = ''
	servers = gateway.get_servers()
	server_name = str(users[username])
	server = servers[str(users[username])]
	#validate chat text
	if request.form.get('chat_text') is not None and str(request.form.get('chat_text')).strip() != '':
		chat_text = str(request.form.get('chat_text')).strip()
		data = {'action' : 'ADD_TEXT', 'data' : {'username' : username, 'chat_text' : chat_text}}
		returned_data = gateway.send_request(server['host'], server['port'], '/add_text', data)
	else:
		returned_data = '{"action" : "ERROR", "data":{"error_message" : "Text cannot be blank!!!"}}' 
		
	#resp = Response(returned_data, status=200, mimetype='application/json')
	
	return returned_data

@gateway.route('/refresh_list', methods=['POST'])
def refresh_list():
	if 'username' not in session:
		return '{"action" : "REDIRECT", "data":{"href":"/"}}'
	users = gateway.get_users()
	user = session['username']
	if user not in users:
		gateway.add_user(user)
	
	servers = gateway.get_servers()
	server_name = str(users[user])
	server = servers[str(users[user])]
	
	json_data = {'action' : 'GET_CHAT_LIST', 'data' : {'username' : session['username']}}
	return gateway.send_request(server['host'], server['port'], '/get_chat_list', json_data)
	#returned_data = gateway.send_request(gateway.host, gateway.port, data, 1)
	#resp = Response(returned_data, status=200, mimetype='application/json')
	
	#return returned_data

@gateway.route('/users_list', methods=['POST'])
def get_users_list():
	if 'username' not in session:
		return '{"action" : "REDIRECT", "data":{"href":"/"}}'
	users = gateway.get_users()
	if session['username'] not in users:
		gateway.add_user(session['username'])
	
	users = gateway.get_users()
	users_list = []
	for user, server in users.iteritems():
		users_list.append({'username' : str(user), 'server_name' : str(server)})
	
	return json.dumps(users_list)

@gateway.route('/heartbeat', methods=['POST'])
def heartbeat():
	response_data = json.loads(request.form['text_data'])
	return gateway.process_heart_beat(response_data['data'])
	
@gateway.route('/next_token_server', methods=['POST'])
def next_token_server():
	response_data = json.loads(request.form['text_data'])
	return gateway.next_token_server(response_data['data'])

@gateway.route('/servers', methods=['GET'])
def display_servers():
	servers = gateway.get_servers()
	
	string = ''
	#"host": "localhost", "hold_token": "False", "name": "client_1", 
	#"max_user": 100, "heartbeat": 1000, "connect_time": 1431595843, 
	#"last_heartbeat": 1431595845, "port": 5001, "total_user": 0
	for server_name, server in servers.iteritems():
		string += '<div>' + str(server_name) + ':</div>'
		string += '<div style="margin-left:20px;">  host: ' + str(server['host']) + ':</div>'
		string += '<div style="margin-left:20px;">  port: ' + str(server['port']) + ':</div>'
		string += '<div style="margin-left:20px;">  max_user: ' + str(server['max_user']) + ':</div>'
		string += '<div style="margin-left:20px;">  total_user: ' + str(server['total_user']) + ':</div>'
		string += '<div style="margin-left:20px;">  connect_time: ' + str(datetime.fromtimestamp(server['connect_time'])) + ':</div>'
		string += '<div style="margin-left:20px;">  last_heartbeat: ' + str(datetime.fromtimestamp(server['last_heartbeat'])) + ':</div>'
		string += '<div style="margin-left:20px;">  hold_token: ' + str(server['hold_token']) + ':</div>'
		
	return string

@gateway.route('/users', methods=['GET'])
def display_users():
	users = gateway.get_users()
	
	string = ''
	for user, server in users.iteritems():
		string += '<div>' + str(user) + ' (' + str(server) + ')</div>'
	
	return string

@gateway.route('/info', methods=['GET'])
def display_info():
	
	
	string = ''
	
	string += "Token holder: " + gateway.get_variable("token_holder")

	servers = gateway.get_servers()
	string += '<h1>Servers</h1>'
	for server_name, server in servers.iteritems():
		string += '<div>' + str(server_name) + ':</div>'
		string += '<div style="margin-left:20px;">  host: ' + str(server['host']) + ':</div>'
		string += '<div style="margin-left:20px;">  port: ' + str(server['port']) + ':</div>'
		string += '<div style="margin-left:20px;">  max_user: ' + str(server['max_user']) + ':</div>'
		string += '<div style="margin-left:20px;">  total_user: ' + str(server['total_user']) + ':</div>'
		string += '<div style="margin-left:20px;">  connect_time: ' + str(datetime.fromtimestamp(server['connect_time'])) + ':</div>'
		string += '<div style="margin-left:20px;">  last_heartbeat: ' + str(datetime.fromtimestamp(server['last_heartbeat'])) + ':</div>'
		string += '<div style="margin-left:20px;">  hold_token: ' + str(server['hold_token']) + ':</div>'
	
	users = gateway.get_users()
	string += '<h1>Users</h1>'
	for user, server in users.iteritems():
		string += '<div style="margin-left:20px;">' + str(user) + ' (' + str(server) + ')</div>'
	
	return string
	
if __name__ == '__main__':
	
	ctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
	ctx.load_cert_chain('keys/server/ca.crt', 'keys/server/ca.key')
	
	threading.Timer(5, gateway.check_dead_servers).start()
	gateway.run(host=gateway.host, port=gateway.port,debug=True, use_reloader=False, ssl_context=ctx)
