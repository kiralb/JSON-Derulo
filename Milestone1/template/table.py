from template.page import *
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Node:
    def __init__(self): #object initializer to set attributes (fields)
        self.val = 0
        self.forwardptr = None
        self.backwardptr = None

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns #number of pages spanned/ number of attributes

        ## create a base record and tail record objects b/c they are going to have different implementations of their indirection columns

        # node = Node()
        # node.forwardptr = Node()
        # if node.forwardptr == None:
        #     print("The forwardptr node is None/Null.")


class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    # This class uses page.py to store and retrieve records
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns + 4
        self.page_directory = {}

        # implementation start
        self.basePages = []
        self.tailPages = []
        
        # initialize base and tail pages where the number of physical pages is equal to the number of columns
        for i in range(num_columns):
            physicaPageToAddToBasePage = Page()
            physicalPageToAddToTailPage = Page()

            self.basePages.append(physicalPageToAddToBasePage)
            self.tailPages.append(physicalPageToAddToTailPage)

        ### TO DO: METACOLUMNS ###
            ### TO DO FOR INDIRECTION COLUMN: ###
            ### if base record:
            ###     - initialize forward indirection column pointer to null
            ###     - with each appended tail record, change forward indirection pointer to latest tail record's RID
            ### if tail record:
            ###     - if 1st tail record, back pointer to base record's RID
            ###     - for all following tail records, set backward pointer to previous tail record's RID
            
            ### TO DO FOR SCHEMA ENCODING: ###
            ### if base record:
            ###     - if column has been updated, set respective bit in bitmap representation to 1
            ###     - otherwise, set bit to 0
            ### if tail record:
            ###     - if column
            ###
            ###
        
        pass

    def __merge(self):
        #for milestone 2
        pass
