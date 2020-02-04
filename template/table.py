from template.page import *
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns + 3
        self.page_directory = {}
        self.RIDCounter = 0
        # mapping our keys and RIDs
        self.keyToRID = {}
#        self.index = Index(self)
        
        
        self.pageRangeArray = []
        self.onePageRange = []
        # contains sets of physical pages (id, quiz1, quiz2, etc.)
        self.physicalPages = []
        
        for i in range(self.num_columns):
            self.physicalPageToAdd = Page()
            self.physicalPages.append(self.physicalPageToAdd)
        self.onePageRange.append(self.physicalPages)
        self.pageRangeArray.append(self.onePageRange)
                
        pass

    def __merge(self):
        pass
 
