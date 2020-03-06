from template.page import *
from template.index import *
import os
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
        """
        Create a folder with the name of the table in ECS165
        """
        path = 'ECS165/' + name

        try:
            os.mkdir(path)
        except OSError:
            print("creation of dir", path, "already exists")
        else:
            print("creation of dir ", path, "passed")

        pass


        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {} # for head
        self.page_directory2 = {} # for tail
        self.RIDCounter = 1
        self.TIDCounter = 2147483647
        # mapping our keys and RIDs
        self.keyToRID = {}
        self.keyToTID = {}

        """ START Milestone2 Stuff """
        self.index = Index(self)
        # holds an index object for each column in base pages
        self.listOfIndexObj = []
        for column in range(num_columns):
            self.indexObjectToAdd = Index(self)
            self.listOfIndexObj.append(self.indexObjectToAdd)

        self.baseRecordDirectory = {}

        self.baseMetaData = [{}, {}] # first dictionary holds base record's key to its indirection. Second holds key to schema
        self.tailMetaData = [{}, {}]



        """ END Milestone2 Stuff """


        self.pageRangeArray = [] #for base records
        self.pageRangeArray2 = [] # for tail records
        self.addToPageArray(self.pageRangeArray)
        self.addToPageArray(self.pageRangeArray2)

        pass


    def addToPageArray(self, bigPageArray):
        onePageRange = []
        # contains sets of physical pages (id, quiz1, quiz2, etc.)
        physicalPagesSet1 = []
        physicalPagesSet2 = []

        for i in range(self.num_columns + 4):
            physicalPageToAddToSet1 = Page()
            physicalPageToAddToSet2 = Page()
            physicalPagesSet1.append(physicalPageToAddToSet1)
            physicalPagesSet2.append(physicalPageToAddToSet2)
        # add 2 sets of physical pages to a page range
        onePageRange.append(physicalPagesSet1)
        onePageRange.append(physicalPagesSet2)
        bigPageArray.append(onePageRange)

    def addPageRange(self, bigPageArray):
        newPageRange = []
        physPageSet1 = []
        physPageSet2 = []
        for i in range(self.num_columns + 4):
            physPageToAddToSet1 = Page()
            physPageToAddToSet2 = Page()
            physPageSet1.append(physPageToAddToSet1)
            physPageSet2.append(physPageToAddToSet2)
        newPageRange.append(physPageSet1)
        newPageRange.append(physPageSet2)
        bigPageArray.append(newPageRange)

    def __merge(self):
        


        pass
