from template.table import Table, Record
from template.index import Index

class BufferPool:
    def __init__(self, num_columns):
        self.dirty = 0
        self.pin = 0
        self.numTransactions = 0
        self.contents = bytearray(4096*(num_columns + 4) * 2)
        #4096 is the size of each byteArray
        #Number of columns is each number of columns in the table
        # *2 because theres two big sets of physical pages
