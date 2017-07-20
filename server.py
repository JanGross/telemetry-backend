#!./venv/bin/python
from flask import Flask, request, jsonify, g
import json, logging, requests, sqlite3, datetime
import socketio

sio = socketio.Server(logger=True, async_mode=None)

app = Flask(__name__)
app.config.from_pyfile('config.cfg')
app.wsgi_app = socketio.Middleware(sio, app.wsgi_app)

debug_var = None

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect('logs.db')
    return db
    
@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

@app.route('/')
def index():
    return "Telemetry API v1"

@sio.on('my event')
def test_message(sid, message):
    print("SOCKET MESSAGE:" + message['data'])
    sio.emit('my response', {'data': message['data']})

def get_location(request):
    if request.headers.getlist("X-Forwarded-For"):
        ip = request.headers.getlist("X-Forwarded-For")[0]
    else:
        ip = request.remote_addr
    send_url = 'http://freegeoip.net/json/' + ip
    j = json.loads(requests.get(send_url).text)
    location = {
        'lat' :  j['latitude'],
        'lon' : j['longitude'],
        'country_code' : j['country_code'],
        'country_name' : j['country_name'],
        'region_code' : j['region_code'],
        'region_name': j['region_name'],
        'city' : j['city']
    }
    return location

@app.route( '/event/<string:event_type>', methods=['POST'] )
def trigger_event(event_type):
    global debug_var
    debug_var = request.args.get('uid')
    print(request)
    print("-------"+datetime.datetime.now().strftime('%H:%M:%S')+"--------")
    content = request.get_json()
    return_debug = int(content.get('debug', 0))
    user_id = content['uid']
    app_name = content['app']
    json_data = content['json']
    if not isinstance(json_data, dict):
        json_data = json.loads(json_data)
    print(type(json_data))
    location = get_location(request)
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    re = "Event(%s) from %s, running %s in %s/%s \n" % (event_type, user_id,app_name, location['country_code'], location['region_code'] )
    db = get_db()
    last_entry = db.cursor().execute('SELECT * FROM events WHERE app LIKE ? AND type LIKE ? AND user LIKE ? ORDER BY dt DESC LIMIT 1 ', \
        (app_name, event_type, user_id, )).fetchall()
    last_timestamp = datetime.datetime.now()
    if len(last_entry) >= 1 :
        last_timestamp = datetime.datetime.strptime(last_entry[0][5], '%Y-%m-%d %H:%M:%S')
    diff = ((datetime.datetime.now() - last_timestamp).total_seconds())/60
    if len(last_entry) <= 0 or (diff) > app.config['DB_DELAY'] : #30 min db entry diff
        db.cursor().execute('INSERT INTO events VALUES (?, ?, ?, ?, ?, ?)', (event_type,user_id, app_name, str(location), str(json_data), timestamp,) )
        result = db.commit()
        re +=  "DB Record created\n" #SCHEMA: events (type text, user text, app text, json text, dt datetime)
    else: re += "Event skipped, last db record added %.2f min ago\n" % (diff)
    print(re)
    #re = jsonify(re)
    io_response = {'event_type' : event_type, 
                                'user_id' : user_id,
                                'app_name' : app_name,
                                'json_data' : json_data,
                                'location': location,
                                'timestamp': timestamp}
                                
    sio.emit('event', {'data': io_response })
    return jsonify("true") if return_debug == 0 else re
    
@app.route('/get/types')
def get_type_list():
    db = get_db()
    data = db.cursor().execute('SELECT DISTINCT type FROM events ').fetchall()
    return jsonify(data)

@app.route('/get/events/<string:appfilter>', methods=['GET'])
def get_all_events(appfilter):
    db = get_db()
    appfilter = "%" if appfilter == "all" else appfilter
    fromfilter = request.args.get("from") or "%" #start date
    tofilter =  request.args.get("to") or datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') #end date
    typefilter = request.args.get("type") or "%"
    userfilter = request.args.get("userid") or "%"
    data = db.cursor().execute('SELECT * FROM events WHERE app LIKE ? AND dt BETWEEN ? and ? AND type LIKE ? AND user LIKE ? ', \
        (appfilter, fromfilter, tofilter, typefilter, userfilter, )).fetchall()
    result = {'events' : {}}
    for i, event in enumerate(data):
        result['events'][i] = { "type" : event[0], "user_id" : event[1], "app" : event[2], "location": event[3], "json" : event[4], "timestamp" : event[5] }
    return jsonify(result)

    
#if __name__ == "__main__" :
 #   app.run(host=app.config['HOST'], port=app.config['PORT'])
    
if __name__ == '__main__':
    if sio.async_mode == 'threading':
        print("threading(werkzeug)")
        # deploy with Werkzeug
        app.run(threaded=True, port=app.config['PORT'])
    elif sio.async_mode == 'eventlet':
        print("eventlet")
        # deploy with eventlet
        import eventlet
        import eventlet.wsgi
        eventlet.wsgi.server(eventlet.listen((app.config['HOST'], app.config['PORT'])), app)
    elif sio.async_mode == 'gevent':
        print("GEVENT")
        # deploy with gevent
        from gevent import pywsgi
        try:
            from geventwebsocket.handler import WebSocketHandler
            websocket = True
        except ImportError:
            websocket = False
        if websocket:
            pywsgi.WSGIServer(('', app.config['PORT']), app,
                              handler_class=WebSocketHandler).serve_forever()
        else:
            print("PYWSGI")
            pywsgi.WSGIServer(('', app.config['PORT']), app).serve_forever()
    elif sio.async_mode == 'gevent_uwsgi':
        print('Start the application through the uwsgi server. Example:')
        print('uwsgi --http :5000 --gevent 1000 --http-websockets --master '
              '--wsgi-file app.py --callable app')
    else:
        print('Unknown async_mode: ' + sio.async_mode)
