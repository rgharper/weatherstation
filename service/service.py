import configparser, mariadb, os, time, threading, statistics, croniter, datetime
from flask import Flask
import flask
from modules import dht20, as5600

flask_api = Flask(__name__)

cfg = configparser.ConfigParser()
cfg.read(os.path.join(os.path.dirname(__file__), "settings.ini"))

directions = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]

temp, humidity, wind_speed, wind_direction, rainfall, speed_rpm = None, None, None, None, None, None

list_wind_speed = []
list_wind_dir = []

running = True

if len(cfg) <= 2:
    raise(SystemExit("No sensors defined"))

def dht20_daemon(sensor:dht20.DHT20):
    global humidity
    global temp 
    if not sensor.begin():
        raise(IOError("Could not connect to DHT20"))
    while running:
        temp = sensor.get_temperature()
        humidity = sensor.get_humidity()
        global max_temp
        global min_temp
        try:
            if temp > max_temp:
                global new_max_temp
                max_temp = temp
                new_max_temp = (cfg["ALL"]["stationid"], temp, datetime.datetime.now().timestamp())
            if temp < min_temp:
                global new_min_temp
                min_temp = temp
                new_min_temp = (cfg["ALL"]["stationid"], temp, datetime.datetime.now().timestamp())
        except:
            pass
        time.sleep(1)
    print("dht20_daemon done")

def wind_daemon(speed_sensor:as5600.as5600, dir_sensor:as5600.as5600):
    global speed_rpm
    global wind_speed
    global list_wind_speed
    global list_wind_dir
    global wind_direction
    while running:
        now_time = time.time()
        now_angle = speed_sensor.angle()
        time.sleep(0.05)
        delta_angle = abs(speed_sensor.angle()-now_angle)
        delta_time = time.time()-now_time
        rpm = ((delta_angle/360)/delta_time)*60

        if delta_angle < 300:
            wind_speed = rpm
            list_wind_speed.append(wind_speed)
        if wind_speed > 1:
            wind_dir_raw = (dir_sensor.angle() + int(cfg["AS5600direction"]["offset"]) + 360) % 360
            dir = round(wind_dir_raw / 22.5) % 16
            list_wind_dir.append(dir)

        time.sleep(0.5)
    print("speed_daemon done")

def get_wind(wipe=True):
    if wind:
        global list_wind_speed
        global list_wind_dir
        list_speed = list_wind_speed
        list_dir = list_wind_dir
        if wipe:
            list_wind_speed = []
            list_wind_dir = []
        avg_speed = avg(list_speed)
        
        if list_dir is not None and list_dir != []:
            mode_dir = statistics.mode(list_dir)
        else:
            mode_dir = None
        
        gust = max(list_speed)
        return avg_speed, mode_dir, gust
    else:
        return None, None, None

def avg(data):
    total = 0
    for speed in data:
        total += speed
    avg = total / len(data)
    return avg

def api_service():
    flask_api.run(cfg["api"]["host"], int(cfg["api"]["port"]))

try:
    temp_humidity = dht20.DHT20(int(cfg["DHT20"]["bus"]), int(cfg["DHT20"]["address"]))
    threading.Thread(target=dht20_daemon, args=(temp_humidity,), daemon=True).start()
    dht = True
except:
    print("no dht")
    dht = False

try:
    speed = as5600.as5600(int(cfg["AS5600speed"]["bus"]), int(cfg["AS5600speed"]["address"]))
    direction = as5600.as5600(int(cfg["AS5600direction"]["bus"]), int(cfg["AS5600direction"]["address"]))
    threading.Thread(target=wind_daemon, args=(speed, direction), daemon=True).start()
    wind = True
except:
    print("no wind")
    wind = False

@flask_api.route("/temperature", methods=['GET'])
def api_temperature():
    global dht
    global temp
    if dht:
        response = flask.Response(str(temp))
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Content-Type', 'text/plain')
        return response
    else:
        return "not connected", 501, {'ContentType':'text/plain'}
    
@flask_api.route("/humidity", methods=['GET'])
def api_humidity():
    global dht
    global humidity
    if dht:
        response = flask.Response(str(humidity))
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Content-Type', 'text/plain')
        return response
    else:
        return "not connected", 501, {'ContentType':'text/plain'}
    
@flask_api.route("/wind", methods=['GET'])
def api_wind():
    global wind
    speed, direction, gust = get_wind(wipe=False)
    if wind:
        response = flask.Response(f"{speed},{gust},{direction}")
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Content-Type', 'text/plain')
        return response
    else:
        return "not connected", 501, {'ContentType':'text/plain'}

# def next_ten(): # wait until the next multiple of 10 minutes (6:02 waits until 6:10)
#     current = datetime.datetime.now()
#     minute = math.ceil(current.minute/10)*10
#     if minute == 60:
#         hour = current.hour+1
#         minute = 0
#     else:
#         hour = current.hour
#     if hour == 24:
#         day = current.day+1
#         hour = 0
#     else:
#         day = current.day
#     if day > current.
#     future = current.replace(microsecond=0, second=0, minute=minute, hour=hour)
#     delta = future - current
#     print(f"Waiting {delta.total_seconds()} until {future.isoformat()}...")
#     time.sleep(delta.total_seconds())
#     print(f"Continuing at {datetime.datetime.now().isoformat()}")

conn_params= {
"user" : cfg["database"]["username"],
"password" : cfg["database"]["password"],
"host" : cfg["database"]["address"],
"database" : cfg["database"]["database"]
}

global max_temp 
global min_temp

global new_max_temp
new_max_temp = None
global new_min_temp
new_min_temp = None

try:
    conn = mariadb.connect(**conn_params)
    print(mariadb.client_version_info)
    conn.auto_reconnect = True
    connected = True
    sql = "SELECT temperature from weatherstation.temperature_records where stationId = ? and year(timestamp) = year(now()) order by temperature desc"
    cur = conn.cursor()
    cur.execute(sql, (cfg["ALL"]["stationid"],))
    data = cur.fetchall()
    print(data)
    max_temp = float(data[0][0])
    min_temp = float(data[1][0])
    print(max_temp)
    print(min_temp)
except mariadb.OperationalError as error:
    print("Couldn't connect to database. Retrying shortly.\n" + str(error))
    connected = False


default_sql = "INSERT INTO weatherstation.weather (stationId, temperature, humidity, windspeed, rainfall, winddirection, windgust, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, FROM_UNIXTIME(?))"
buffer = []

threading.Thread(target=api_service, daemon=True).start()
try:
    cron = croniter.croniter(cfg["ALL"]["cron"])
    # next_ten()
    while running:
        current = datetime.datetime.now()
        next_time = datetime.datetime.fromtimestamp(cron.next())
        delta = next_time - current
        seconds = delta.total_seconds()
        print(f"Waiting {seconds}")
        time.sleep(seconds)
        print(next_time)
        try:
            try:
                # get max / min for the year
                if new_max_temp is not None and connected:
                    sql = "INSERT into weatherstation.temperature_records (stationId, temperature, timestamp) VALUES (?, ?, FROM_UNIXTIME(?))"
                    cur = conn.cursor()
                    cur.execute(sql, new_max_temp)
                    conn.commit()
                    cur.close()
                    new_max_temp = None

                if new_min_temp is not None and connected:
                    sql = "INSERT into weatherstation.temperature_records (stationId, temperature, timestamp) VALUES (?, ?, FROM_UNIXTIME(?))"
                    cur = conn.cursor()
                    cur.execute(sql, new_min_temp)
                    conn.commit()
                    cur.close()
                    new_min_temp = None
                
                if connected:
                    sql = "SELECT temperature from weatherstation.temperature_records where stationId = ? and year(timestamp) = year(now()) order by temperature desc"
                    cur = conn.cursor()
                    cur.execute(sql, (cfg["ALL"]["stationid"],))
                    print(data)
                    data = cur.fetchall()
                    max_temp = float(data[0][0])
                    min_temp = float(data[1][0])
                    print(max_temp)
                    print(min_temp)
                    print(max_temp)
                    print(min_temp)
            except Exception as e:
                print("Failed to update max/min")
                print(str(e))

            # log weather data
            sql = "INSERT INTO weatherstation.weather (stationId, temperature, humidity, windspeed, rainfall, winddirection, windgust, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, FROM_UNIXTIME(?))"
            speed, direction, gust = get_wind()
            data = (cfg["ALL"]["stationid"], temp, humidity, speed, rainfall, direction, gust, datetime.datetime.now().timestamp())
            if not connected:
                raise Exception
            cur = conn.cursor()
            cur.execute(sql, data)
            conn.commit()
            cur.close()

            buffer = []
        except Exception as e:
            buffer.append(data)
            print("Data added to buffer, Reconnecting due to exception:")
            print(str(e))
            try:
                conn = mariadb.connect(**conn_params)
                connected = True
                print("Reconnect successful. Executing buffered queries")
                cur = conn.cursor()
                for old_query in buffer:
                    print(f"Executing: {old_query})")
                    cur.execute(sql, old_query)
                    conn.commit()
                cur.close()
                
                buffer = []
            except Exception as e:
                connected = False
                print("Reconnect failed (retrying soon) with exception:")
                print(str(e))
        # next_ten()
except KeyboardInterrupt:
    print("Keyboard Interrupt Recieved. Wrapping things up.")
    running=False
    conn.close()
print("Service main loop has exited")