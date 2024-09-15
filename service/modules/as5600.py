import smbus2, time, sys, math



class as5600:
        def __init__(self, bus, address=0x36):
                self.address = address
                self.bus = smbus2.SMBus(bus)

        def angle(self): # Read angle (0-360 represented as 0-4096)
                read_bytes = self.bus.read_i2c_block_data(self.address, 0x0C, 2)
                return ((read_bytes[0]<<8) | read_bytes[1])*360/4096

        def magnitude(self): # Read magnetism magnitude
                read_bytes = self.bus.read_i2c_block_data(self.address, 0x1B, 2)
                return (read_bytes[0]<<8) | read_bytes[1]