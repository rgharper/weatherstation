import configparser, os, time, threading
from modules import dht20, as5600

cfg = configparser.ConfigParser()
cfg.read(os.path.join(os.path.dirname(__file__), "settings.ini"))

temp, humidity, wind_speed, wind_direction, rainfall, speed_rpm = None, None, None, None, None, None

list_wind_speed = []

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

def speed_daemon(sensor:as5600.as5600):
    global speed_rpm
    global wind_speed
    global list_wind_speed
    while running:
        now_time = time.time()
        now_angle = sensor.angle()
        time.sleep(0.05)
        delta_angle = abs(sensor.angle()-now_angle)
        delta_time = time.time()-now_time
        rpm = (delta_angle/360)/delta_time
        if delta_angle < 300:
            wind_speed = rpm
            list_wind_speed.append(wind_speed)
            time.sleep(0.5)
    print("speed_daemon done")

def mean_speed():
    global list_wind_speed
    if list_wind_speed is not None:
        data = list_wind_speed
        list_wind_speed = []
        total = 0
        for speed in data:
            total += speed
        return total/len(data)
    else:
        return None

def direction_daemon(sensor:as5600.as5600):
    global wind_direction
    while running:
        wind_direction = (sensor.angle() + int(cfg["AS5600direction"]["offset"]) + 360) % 360
        time.sleep(0.5)
    print("direction_daemon done")

try:
    temp_humidity = dht20.DHT20(int(cfg["DHT20"]["bus"]), int(cfg["DHT20"]["address"]))
    threading.Thread(target=dht20_daemon, args=(temp_humidity,)).start()
except:
    print("no dht")


try:
    direction = as5600.as5600(int(cfg["AS5600speed"]["bus"]), int(cfg["AS5600speed"]["address"]))
    threading.Thread(target=speed_daemon, args=(direction,)).start()
except:
    print("no direction")

try:
    speed = as5600.as5600(int(cfg["AS5600direction"]["bus"]), int(cfg["AS5600direction"]["address"]))
    threading.Thread(target=direction_daemon, args=(speed,)).start()
except:
    print("no speed")

try:
    while running:
        time.sleep(10)
        print(f"{cfg['ALL']['stationid']}, {temp}, {humidity}, {mean_speed()}, {rainfall}, {wind_direction}")
except KeyboardInterrupt:
    print("Keyboard Interrupt Recieved. Wrapping things up.")
    running=False
