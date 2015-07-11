import socket
import sys, getopt
import ConfigParser
import json
import threading
from socket_helper import SocketHelper
from flask import Flask, session, redirect, url_for, escape, request, render_template
import logging
import time
import os
import sqlite3
import ssl

class AppServer(SocketHelper):
	def __init__(self, name):
		super(AppServer, self).__init__(name)
		
		configFile = 'config.cfg'
		if len(sys.argv) > 1:
			configFile = sys.argv[1]
		
		config = ConfigParser.RawConfigParser()
		config.read(configFile)
		self.server = {	'host' : config.get('Server', 'host'),
						'port' : int(config.get('Server', 'port'))}
		self.resource_server = {	'host' : config.get('Resource', 'host'),
									'port' : int(config.get('Resource', 'port'))}
						
		self.host = config.get('AppServer', 'host')
		self.port = int(config.get('AppServer', 'port'))
		self.heartbeat = int(config.get('AppServer', 'heartbeat'))
		self.name = config.get('AppServer', 'name')
		self.max_user = int(config.get('AppServer', 'max_user'))
		self.token = ''
		self.token_hold_time = int(config.get('Resource', 'token_hold_time'))
		self.queue_texts = [] #[{'username', 'chat_text', 'timestamp'}]
		self.max_server = 5
		
		self.db_path = 'db/' + self.name + '.sqlite'
		conn = sqlite3.connect(self.db_path)
		with open ('db/appserver.sql') as f:
			sql = f.read()
			cur = conn.cursor()
			cur.executescript(sql)
		conn.commit()
		conn.close()
	
	def get_text_queue(self):
		conn = sqlite3.connect(self.db_path)
		conn.row_factory = sqlite3.Row
		cur = conn.cursor()
		
		fetch_all_texts_query = "select * from text_queue"
		cur.execute(fetch_all_texts_query)
		rows = cur.fetchall()
		
		texts_list = []
		for row in rows:
			texts_list.append({'username' : row['user_name'], 'timestamp' : row['timestamp'], 'chat_text' : row['chat_text']})
		conn.close()
		
		return texts_list
		
	def add_text_to_queue(self, username, text_content):
		conn = sqlite3.connect(self.db_path)
		conn.row_factory = sqlite3.Row
		cur = conn.cursor()
		
		fetch_all_texts_query = "insert into text_queue (user_name, timestamp, chat_text) values (?, ?, ?)"
		cur.execute(fetch_all_texts_query, (username, int(time.time()), text_content))
		conn.commit()
		conn.close()
		
	def clear_text_queue(self):
		conn = sqlite3.connect(self.db_path)
		conn.row_factory = sqlite3.Row
		cur = conn.cursor()
		
		fetch_all_texts_query = "delete from text_queue"
		cur.execute(fetch_all_texts_query)
		conn.commit()
		conn.close()
	
	def get_chat_list(self):
		data = {'action': 'GET_LIST', 'data' : {}}
		returned_data = json.loads(self.send_request(self.resource_server['host'], self.resource_server['port'], '/get_list', data, 'keys/resource'))
		return returned_data
	
	def add_chat_text(self, data):
		#define text_data dictionary
		text_data = {	'username' : data['username'],
						'chat_text' : data['chat_text'],
						'timestamp' : int(time.time())}
		#if the server doesn't hold token => add text to queue and return
		token = self.get_variable('token')
		if token == '':
			self.add_text_to_queue(data['username'], data['chat_text'])
			data = {'action' : 'RETURN_SIGNAL', 'data' : {'signal' : 'ADDED_TO_WAITING_LIST'}}
			return data
		
		#if the server holds token => send text to resource server
		data = {'action': 'ADD_DATA', 'token' : token, 'data' : [text_data]}
		returned_data = self.send_request(self.resource_server['host'], self.resource_server['port'], '/add_data', data, 'keys/resource')
		print(returned_data)
		return {'action' : 'RETURN_SIGNAL', 'data' : {'signal' : 'ADDED_TO_SERVER'}}
	
	def send_heartbeat(self):
		
		data = {'action' : 'HEARTBEAT',
				'data' : { 	'name' : self.name,
							'host' : self.host,
							'port' : self.port,
							'heartbeat' : self.heartbeat,
							'max_user' : self.max_user}}
		try:
			returned_data = self.send_request(self.server['host'], self.server['port'], '/heartbeat', data, 'keys/' + self.name)
			#check if the returned data contain token
			response = json.loads(returned_data)
			if response['action'] == 'TOKEN':
				token_data = response['data']
				self.begin_hold_token(token_data)
		finally:
			if not self.is_shutting_down:
				threading.Timer(self.heartbeat/1000, self.send_heartbeat).start()
	
	def begin_hold_token(self, data):
	
		#set token
		self.set_variable('token', data['token'])
		print('hold token')
		
		# send queuing texts to resource server
		waiting_texts = self.get_text_queue()
		if len(waiting_texts) > 0:
			data = {'action': 'ADD_DATA',
				'token' : self.get_variable('token'),
				'data' : waiting_texts}
			
			self.send_request(self.resource_server['host'], self.resource_server['port'], '/add_data', data, 'keys/resource')
			self.clear_text_queue()
			
		# set time to send token to next server
		threading.Timer(self.token_hold_time/1000, self.send_token).start()
		
		# return something
		data = {'action' : 'HOLD_TOKEN',
				'data' : {}}
		
		return data
		
	def send_token(self, dead_servers=[]):
		if self.is_shutting_down:
			return
			
		token = self.get_variable('token')
		if token == '':
			return
		#get next server from gateway
		data = {'action' : 'NEXT_TOKEN_SERVER',
				'data' : {	'server_name' : self.name,
							'dead_servers' : dead_servers}}
		
		returned_data = self.send_request(self.server['host'], self.server['port'], '/next_token_server', data, 'keys/server')
		returned_data = json.loads(returned_data)
		if returned_data['action'] == 'REMOVE_TOKEN':
			self.set_variable('token', '')
			print('token in another server, removed')
			return
		
		next_server = returned_data['data']
		if next_server['server_name'] == self.name:
			#no more sever, cannot send to self
			print('app only has this server, continue hold token')
			threading.Timer(self.token_hold_time/1000, self.send_token).start() 
		else:
			#send token to next server
			data = {'action' : 'TOKEN', 'data' : {'token' : token}}

			try:
				self.send_request(str(next_server['host']), next_server['port'], '/token', data)
				self.set_variable('token', '')
				print('send token')
			except:
				print('exception')
				dead_servers.append(str(next_server['server_name']))
				self.send_token(dead_servers)
			#print('send token to ' + next_server['server_name'])
		

server = AppServer(__name__)

@server.route('/token', methods=['POST'])
def begin_hold_token():
	response_data = json.loads(request.form['text_data'])
	return json.dumps(server.begin_hold_token(response_data['data']))
	
@server.route('/get_chat_list', methods=['POST'])
def get_chat_list():
	response_data = json.loads(request.form['text_data'])
	return json.dumps(server.get_chat_list())

@server.route('/add_text', methods=['POST'])
def add_chat_text():
	print(request.form['text_data'])
	response_data = json.loads(request.form['text_data'])
	return json.dumps(server.add_chat_text(response_data['data']))

if __name__ == '__main__':
	ctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
	
	ctx.load_cert_chain('keys/' + server.name + '/ca.crt', 'keys/' + server.name + '/ca.key')
	threading.Timer(server.heartbeat/1000, server.send_heartbeat).start() # send heart beat data
	server.run(host=server.host, port=server.port,debug=True, use_reloader=False, ssl_context=ctx)
	
