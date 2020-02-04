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
        firstIndex = self.table.RIDCounter // 2048
        secondIndex = 0
        temp2 = self.table.RIDCounter - firstIndex * 2048
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
    
    
    def addNewPageRange(self, secondIndex, thirdIndex, fourthIndex):
        if (secondIndex == 0 and thirdIndex == 0 and fourthIndex == 0):
            return True
        else:
            return False
    
    def addToByteArray(self, physicalPage, offset, attribute):
        x = (int(attribute).to_bytes(4, 'big'))
        i = 0
        while (i < 4):
            physicalPage.data[offset + i] = x[i]
            i = i + 1
    
    
    """
    # Insert a record with specified columns
    """

    def insert(self, *columns):
        self.mapRIDToIndices()
        schema_encoding = '0' * self.table.num_columns
        
        ### mapping keys to RIDs ###
        RIDCounter = self.table.RIDCounter
        key = columns[0]
        # student ID matching with the RID
        self.table.keyToRID[key] = RIDCounter
        numColumns = self.table.num_columns
        #print("numColumns: ", numColumns)
        for i in range(numColumns - 1):
            attribute = columns[i]
            firstIndex = self.table.page_directory[RIDCounter][0]
            secondIndex = self.table.page_directory[RIDCounter][1]
            thirdIndex = i
            fourthIndex = self.table.page_directory[RIDCounter][3]
            if (self.addNewPageRange(secondIndex, thirdIndex, fourthIndex)):
                self.table.addPageRange()
            #print("rid: ", RIDCounter, " firstIndex: ", firstIndex, " secondINdex: ", secondIndex, " thirdIndex: ", thirdIndex, " fourthIndex: ", fourthIndex)
            physicalPageToAdd = self.table.pageRangeArray[firstIndex][secondIndex][thirdIndex]
            self.addToByteArray(physicalPageToAdd, fourthIndex, attribute)
        
        
        
        
        
        
        
        
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
