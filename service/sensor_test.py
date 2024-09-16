import configparser, os, time, threading
from modules import dht20, as5600

cfg = configparser.ConfigParser()
cfg.read(os.path.join(os.path.dirname(__file__), "settings.ini"))

temp, humidity, wind_speed, wind_direction, rainfall, speed_rpm = None, None, None, None, None, None

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

def speed_daemon(sensor:as5600.as5600):
    global speed_rpm
    global wind_speed
    while running:
        start = time.time()
        angle_offset = sensor.angle()
        angle = 0
        while angle < 360:
            time.sleep(0.1)
            angle = sensor.angle()-angle_offset
        speed_rpm = ((sensor.angle()-angle_offset)/360)/(time.time()-start)
        wind_speed = speed_rpm*int(cfg["AS5600speed"]["factor"])
        time.sleep(0.5)

def direction_daemon(sensor:as5600.as5600):
    global wind_direction
    while running:
        wind_direction = sensor.angle() - int(cfg["AS5600direction"]["offset"])
        time.sleep(0.5)

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
        print(f"{cfg['ALL']['stationid']}, {temp}, {humidity}, {wind_speed}, {rainfall}, {wind_direction}")
        time.sleep(1)
except KeyboardInterrupt:
    print("Keyboard Interrupt Recieved. Wrapping things up.")
    running=False