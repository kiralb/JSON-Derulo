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
        self.num_columns = num_columns
        self.page_directory = {}
        self.RIDCounter = 0
        # mapping our keys and RIDs
        self.keyToRID = {}
#        self.index = Index(self)
        
        
        self.pageRangeArray = []
        self.onePageRange = []
        # contains sets of physical pages (id, quiz1, quiz2, etc.)
        self.physicalPagesSet1 = []
        self.physicalPagesSet2 = []
        
        for i in range(self.num_columns):
            self.physicalPageToAddToSet1 = Page()
            self.physicalPageToAddToSet2 = Page()
            self.physicalPagesSet1.append(self.physicalPageToAddToSet1)
            self.physicalPagesSet2.append(self.physicalPageToAddToSet2)
        # add 2 sets of physical pages to a page range
        self.onePageRange.append(self.physicalPagesSet1)
        self.onePageRange.append(self.physicalPagesSet2)
        self.pageRangeArray.append(self.onePageRange)
                
        pass
        
    
    def addPageRange(self):
        newPageRange = []
        physPageSet1 = []
        physPageSet2 = []
        for i in range(self.num_columns):
            physPageToAddToSet1 = Page()
            physPageToAddToSet2 = Page()
            physPageSet1.append(physPageToAddToSet1)
            physPageSet2.append(physPageToAddToSet2)
        newPageRange.append(physPageSet1)
        newPageRange.append(physPageSet2)
        self.pageRangeArray.append(newPageRange)

    def __merge(self):
        pass
 
