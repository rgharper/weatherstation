import time
import smbus2 as smbus
                
class DHT20(object):
  def __init__(self ,bus,address=0x38):
    self.i2cbus = smbus.SMBus(bus)
    self._addr = address

  def begin(self ):
    time.sleep(0.5)
    data = self.read_reg(0x71,1)
    
    if (data[0] | 0x08) == 0:
      return False
    else:
      return True

  # ambient temperature, degrees
  def get_temperature(self):
     self.write_reg(0xac,[0x33,0x00])
     time.sleep(0.1)
     data = self.read_reg(0x71,7)
     rawData = ((data[3]&0xf) <<16) + (data[4]<<8)+data[5]
     temperature = float(rawData)/5242 -50
     return temperature

  # relative humidity
  def get_humidity(self):
     self.write_reg(0xac,[0x33,0x00])
     time.sleep(0.1)
     data = self.read_reg(0x71,7)
     rawData = ((data[3]&0xf0) >>4) + (data[1]<<12)+(data[2]<<4)
     humidity = float(rawData)/0x100000
     return humidity*100
   
  def write_reg(self, reg,data):
    time.sleep(0.01)
    self.i2cbus.write_i2c_block_data(self._addr,reg,data)
   
  def read_reg(self,reg,len):
    time.sleep(0.01)
    result = self.i2cbus.read_i2c_block_data(self._addr,reg,len)
    return result
