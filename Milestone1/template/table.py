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
        self.columns = columns #number of pages spanned/ number of attributes
        
        ## create a base record and tail record objects b/c they are going to have different implementations of their indirection columns

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
    
        
        self.basePages = []
        self.tailPages = []
        # basePage.append(PageObj)

        # initialize base and tail pages where the number of physical pages is equal to the number of columns
        for i in range(num_columns):
            physicaPageToAddToBasePage = Page()
            physicalPageToAddToTailPage = Page()
            
            self.basePages.append(physicalPageToAddToBasePage)
            self.tailPages.append(physicalPageToAddToTailPage)
            
        ### TO DO: METACOLUMNS ###
            ### TO DO FOR INDIRECTION COLUMN: ###
            ### if base record:
            ###        - initialize forward indirection column pointer to null
            ###        - with each appended tail record, change forward indirection pointer to latest tail record's RID
            ### if tail record:
            ###        - if 1st tail record, back pointer to base record's RID
            ###     - for all following tail records, set backward pointer to previous tail record's RID
            
            ### TO DO FOR SCHEMA ENCODING: ###
            ### if base record:
            ###     -
        
        pass

    def __merge(self):
        #for milestone 2
        pass
