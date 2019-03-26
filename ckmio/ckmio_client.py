import socket
import json
import asyncio
import threading

HOST = 'ckmio.com'
PORT = 7023

COMMUNITY_PLAN_KEY = 'community-test-key'
COMMUNITY_PLAN_SECRET = 'community-test-secret'   

AUTH_SERVICE = "10"
CHAT_SERVICE = "22"
TOPIC_SERVICE = "21"
FUNNEL_SERVICE = "27"

SEND_CHAT_MESSAGE_ACTION = "send-chat-message"
AUTHENTICATE_ACTION = "authenticate"
SUBSCRIBE_ACTION = "subscribe"

def to_eight_bytes_array(packetLength):
    return [(packetLength & 0xff000000) >> 24,
            (packetLength & 0x00ff0000) >> 16,
            (packetLength & 0x0000ff00) >> 8,
            (packetLength & 0x000000ff) >> 0]


def send_chat_message(s, from_user, to, content):
    format_and_send(s, CHAT_SERVICE, SEND_CHAT_MESSAGE_ACTION, { 'from' : from_user, 'to': to, 'content' : content})


def authenticate(s, plan_key, plan_secret, user, password):
  format_and_send(s, AUTH_SERVICE, AUTHENTICATE_ACTION, { 'user' : user, 'password' : password, 'plan_key' : plan_key, 'plan_secret' : plan_secret})

def get_authenticate_message(plan_key, plan_secret, user, password):
    return get_message(get_formated_message(AUTH_SERVICE, AUTHENTICATE_ACTION, { 'user' : user, 'password' : password, 'plan_key' : plan_key, 'plan_secret' : plan_secret}))

def get_subscribe_to_chat_message():
    return get_message(get_formated_message(CHAT_SERVICE, SUBSCRIBE_ACTION, { }))

def get_chat_message(from_user, to, content):
    return get_message(get_formated_message(CHAT_SERVICE, SEND_CHAT_MESSAGE_ACTION, { 'from' : from_user, 'to': to, 'content' : content}))

def get_formated_message(service, action, payload): 
    return service + ":" + json.dumps(
        {
            'action' : action, 
            'clt_ref' : "xxxx", 
            'payload' : payload
        }
    )


def format_and_send(client, service, action, payload): 
    send_message(client, service + ":" + json.dumps(
        {
            'action' : action, 
            'clt_ref' : "xxxx", 
            'payload' : payload
        }
    ))

def send_message(socket, message):
    socket.sendall(bytearray(to_eight_bytes_array(len(message))))
    socket.sendall(message.encode("utf-8"))

def get_message(message):
    return bytearray(to_eight_bytes_array(len(message))), message.encode("utf-8")

def length_from_bytes(data):
    return data[2]*256 + data[3]



def handle_data(data, ckmioclt):
    message = json.loads(data)
    for h in ckmioclt[message['service']]:
        h(message, ckmioclt)


def default_message_handler(message, ckmioclt):
    print(message)

class CkmioClient(asyncio.Protocol):
    def __init__(self, user, password, 
        plan_key = COMMUNITY_PLAN_KEY, plan_secret=COMMUNITY_PLAN_SECRET,
        authentication_handlers = [default_message_handler], 
        chat_handlers = [default_message_handler], 
        topic_handlers = [default_message_handler], funnel_handlers= [default_message_handler]
    ):
        
        self.plan_key = plan_key
        self.plan_secret = plan_secret
        self.user = user
        self.buffer = bytes()
        self.bytesToRead = 4
        self.state_type = 0
        self.Read = 0
        self.password = password
        self.Handlers = { AUTH_SERVICE : authentication_handlers ,
                        CHAT_SERVICE : chat_handlers, 
                        TOPIC_SERVICE : topic_handlers,
                        FUNNEL_SERVICE : funnel_handlers
        }
    
    def connection_made(self, transport):
        print('connection made')
        self.loop = asyncio._get_running_loop()
        self.transport = transport
        header, message = get_authenticate_message(self.plan_key, self.plan_secret, self.user, self.password)
        transport.write(header)
        transport.write(message)
        header1, message1 = get_subscribe_to_chat_message()
        transport.write(header1)
        transport.write(message1)
        print('Data sent: {!r}'.format(self.plan_key)) 

    def stop_listening(self):
        self.loop.stop()
    
    def __getitem__(self, item):
         return self.Handlers[item]

    def data_received(self, data):
        print('[DEBUG] : Raw Data received : {!r}'.format(data))
        self.buffer += data 
        receivedLen = len(data)
        self.Read += receivedLen
        while (self.bytesToRead <= self.Read):              
            self.Read -= self.bytesToRead       
            workingdata = self.buffer[0:self.bytesToRead]
            self.buffer = self.buffer[self.bytesToRead:len(self.buffer)]
            if (self.state_type == 0):                
                self.bytesToRead = length_from_bytes(workingdata)
                self.state_type = 1
            else:  
                self.bytesToRead = 4
                self.state_type = 0
                handle_data(workingdata, self)

    def connection_lost(self, exc):
        print('The server closed the connection')
        print('Stop the event loop')
        self.loop.stop()



class CkmioRunner(threading.Thread):
    def __init__(self, clt):
        threading.Thread.__init__(self)
        self.ckmioClient = clt

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        con = loop.create_connection(lambda: ckmioclt,
                              HOST, PORT)
        loop.run_until_complete(con)
        loop.run_forever()
        loop.close()


def my_chat_handler(message, ckmioclt):
    if message['message']== 'new-chat-message':
        print("A new chat message from {} \n with content : {}\n".format(message['payload']['from'], message['payload']['content']))

if __name__ == '__main__':
    try :
        ckmioclt = CkmioClient('mamadou', 'mampwd', chat_handlers=[my_chat_handler])
        runner = CkmioRunner(ckmioclt)
        runner.start()
        print("Listening to CKMIO ...")
    except Exception as e :
        pass
    finally :
        pass
