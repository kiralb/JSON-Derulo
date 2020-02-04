from template.table import Table, Record
from template.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    """

    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key):
        pass
        
    def mapRIDToIndices(self):
        arrayOfIndices = []
        firstIndex = self.table.RIDCounter // 2048 # 1
        secondIndex = 0
        temp2 = self.table.RIDCounter - firstIndex * 2048 #0
        thirdIndex = 0
        temp4 = 0
        if (temp2 > 1023):
            secondIndex = 1
            temp4 = temp2 - 1024
        else:
            temp4 = self.table.RIDCounter % 2048
        arrayOfIndices.append(firstIndex)
        arrayOfIndices.append(secondIndex)
        arrayOfIndices.append(thirdIndex)
        arrayOfIndices.append(temp4 * 4)
        self.table.page_directory[self.table.RIDCounter] = arrayOfIndices
    

    """
    # Insert a record with specified columns
    """

    def insert(self, *columns):
		#mapping will add to page_directory
		# for example, adding RID = 0 will add
		# page_directory = { 0: [0, 0, 0, 0] }
        self.mapRIDToIndices()
        schema_encoding = '0' * self.table.num_columns
        
        ### mapping keys to RIDs ###
        RIDCounter = self.table.RIDCounter
        key = columns[0]
        # student ID matching with the RID
        self.table.keyToRID[key] = RIDCounter
        
        
        
        
        
        
        
        
        # incrementing so we're not mapping to the same RID
        self.table.RIDCounter = self.table.RIDCounter + 1
        
        
        pass

    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    """

    def select(self, key, query_columns):
        pass

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        pass

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass
