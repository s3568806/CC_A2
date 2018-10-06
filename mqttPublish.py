import datetime
import ssl
import time
import json

import jwt
import paho.mqtt.client as mqtt

def create_jwt(project_id, private_key_file, algorithm):
	token = {
		'iat': datetime.datetime.utcnow(),
		'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=60),
		'aud': project_id}
	
	with open(private_key_file, 'r') as file:
		private_key = file.read()
	
	print('Creating JWT using {} from private key file {}'.format(algorithm, private_key_file))
	return jwt.encode(token, private_key, algorithm=algorithm)

def error_str(rc):
	return '{}: {}'.format(rc, mqtt.error_string(rc))

def on_connect(unused_client, unused_userdata, unused_flags, rc):
	print('Connection result:', error_str(rc))
	
def on_disconnect(unused_client, unused_userdata, rc):
	print('Diconnected:', error_str(rc))

def on_publish(unused_clientm, unused_userdata, mid):
	print('Published message acked')

def main():
	client = mqtt.Client(
		client_id='projects/{}/locations/{}/registries/{}/devices/{}'.format(
			'ccassiot2',
			'asia-east1',
			'cciot2018-registry',
			'raspi'))
		
	client.username_pw_set(
		username='unused',
		password=create_jwt('ccassiot2', 'rsa_private.pem', 'RS256'))
		
	client.tls_set(ca_certs='roots.pem', tls_version=ssl.PROTOCOL_TLSv1_2)
		
	client.on_connect = on_connect
	client.on_publish = on_publish
	client.on_disconnect = on_disconnect
	
	client.connect('mqtt.googleapis.com', 8883)
	
	client.loop_start()
		
	telemetry_topic = '/devices/{}/events'.format('raspi')
		
	#test message
	payload = json.dumps({'temperature': 24})
	print('Publishing payload', payload)
	client.publish(telemetry_topic, payload, qos=1)
		
	client.disconnect()
	client.loop_stop()

if __name__ == '__main__':
	main()