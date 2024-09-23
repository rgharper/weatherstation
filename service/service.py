import configparser, mariadb, os, time, threading
from modules import dht20, as5600

cfg = configparser.ConfigParser()
cfg.read(os.path.join(os.path.dirname(__file__), "settings.ini"))

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
        time.sleep(10)
    print("dht20_daemon done")

def wind_daemon(speed_sensor:as5600.as5600, dir_sensor:as5600.as5600):
    global speed_rpm
    global wind_speed
    global list_wind_speed
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

        wind_dir = (dir_sensor.angle() + int(cfg["AS5600direction"]["offset"]) + 360) % 360
        list_wind_dir.append(wind_dir)

        time.sleep(0.5)
    print("speed_daemon done")

def get_wind():
    if wind:
        list_speed = list_wind_speed
        list_dir = list_wind_dir
        list_wind_speed = []
        list_wind_dir = []
        avg_speed = avg(list_speed)
        if avg_speed > 1:
            avg_dir = avg(list_dir)
        else:
            avg_dir = None
        
        gust_range = 3
        total = 0
        for i in range(gust_range):
            highest = max(list_speed)
            total += highest
            list_speed.remove(highest)
        gust = total/gust_range
        return avg_speed, avg_dir, gust
    else:
        return None, None, None

def avg(data):
    total = 0
    for speed in data:
        total += speed
    avg = total / len(data)
    return avg

try:
    temp_humidity = dht20.DHT20(int(cfg["DHT20"]["bus"]), int(cfg["DHT20"]["address"]))
    threading.Thread(target=dht20_daemon, args=(temp_humidity,)).start()
    dht = True
except:
    print("no dht")
    dht = False

try:
    speed = as5600.as5600(int(cfg["AS5600speed"]["bus"]), int(cfg["AS5600speed"]["address"]))
    direction = as5600.as5600(int(cfg["AS5600direction"]["bus"]), int(cfg["AS5600direction"]["address"]))
    threading.Thread(target=wind_daemon, args=(speed, direction)).start()
    wind = True
except:
    print("no wind")
    wind = False

conn_params= {
"user" : cfg["database"]["username"],
"password" : cfg["database"]["password"],
"host" : cfg["database"]["address"],
"database" : cfg["database"]["database"]
}

conn = mariadb.connect(**conn_params)
conn.auto_reconnect = True
try:
    time.sleep(10)
    while running:
        try:
            cur = conn.cursor()
            sql = "INSERT INTO weatherstation.weather (stationId, temperature, humidity, windspeed, rainfall, winddirection, windgust) VALUES (?, ?, ?, ?, ?, ?, ?)"
            speed, direction, gust = get_wind()
            data = (cfg["ALL"]["stationid"], temp, humidity, speed, rainfall, direction, gust)
            cur.execute(sql, data)
            conn.commit()
            cur.close()
        except mariadb.Error as error:
            conn = mariadb.connect(**conn_params)
        time.sleep(int(cfg["ALL"]["interval"]))
except KeyboardInterrupt:
    print("Keyboard Interrupt Recieved. Wrapping things up.")
    running=False
    conn.close()