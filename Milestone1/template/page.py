from template.config import *


class Page:

    def __init__(self):
        self.num_records = 0
        self.offset = 0
        self.data = bytearray(4096)

    def has_capacity(self):
    	pass
    	# if(self.data > 4096):
    	# 	print("Has Reached Capacity\n")
    	# 	return false
     #    return true
        

    def write(self, value):
        self.num_records += 1
        pass
