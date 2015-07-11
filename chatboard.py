import threading
import ConfigParser 
import json
import sqlite3
from socket_helper import SocketHelper
from flask import Flask, session, redirect, url_for, escape, request, render_template, Response
import ssl

class Chatboard(SocketHelper):
	def __init__(self, name):
		super(Chatboard, self).__init__(name)
		self.size = 100 #maximum text messages stored in database
		self.chatting_texts = [] #[{'username', 'chat_text', 'timestamp'}]
		configFile = 'config.cfg'
		self.max_server = 10
		
		config = ConfigParser.RawConfigParser()
		config.read(configFile)
		self.token = config.get('Resource', 'token_key')
		self.host = config.get('Resource', 'host')
		self.port = int(config.get('Resource', 'port'))
		self.db_path = 'db/chat_texts.sqlite'
	
	def add_data(self, token_key, posted_data):
		print(posted_data)
		if self.token != token_key:
			raise ValueError("Token not found")
		
		conn = sqlite3.connect(self.db_path)
		conn.row_factory = sqlite3.Row
		cur = conn.cursor()
		for item in posted_data:
			insert_query = "insert into texts(username, chat_content, time_stamp)\
						values (?, ?, ?)";
			cur.execute(insert_query, (item['username'], item['chat_text'], item['timestamp']))
		conn.commit()
		conn.close()
		
		returned_data = {'action' : 'RETURN_SIGNAL', 'data' : {'signal' : 'ADD_DATA_OK'}}
		return json.dumps(returned_data)
	
	def get_list(self):
	
		conn = sqlite3.connect(self.db_path)
		conn.row_factory = sqlite3.Row
		cur = conn.cursor()
		
		fetch_all_users_query = "select  * from texts order by time_stamp desc limit 100"
		cur.execute(fetch_all_users_query)
		rows = cur.fetchall()
		
		chatting_texts = []
		for row in rows:
			chatting_texts.append({'username' : row['username'], 'chat_text' : row['chat_content'], 'timestamp' : row['time_stamp']})
		
		data = {'action' : 'RETURN_LIST', 'data' : {'chatting_texts' : chatting_texts}}
		return json.dumps(data)

chatboard = Chatboard(__name__)

@chatboard.route('/get_list', methods=['POST'])
def get_list():
	return chatboard.get_list()
	
@chatboard.route('/add_data', methods=['POST'])
def add_data():
	response_data = json.loads(request.form['text_data'])
	return chatboard.add_data(response_data['token'], response_data['data'])

if __name__ == '__main__':
	
	print(ssl.OPENSSL_VERSION)
	ctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
	ctx.load_cert_chain('keys/resource/ca.crt', 'keys/resource/ca.key')
	
	
	chatboard.run(host=chatboard.host, port=chatboard.port,debug=True, use_reloader=False, ssl_context=ctx)
