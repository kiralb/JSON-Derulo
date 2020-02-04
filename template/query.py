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
        firstIndex = (self.table.RIDCounter - 1) // 2048 # 1
        secondIndex = 0
        temp2 = (self.table.RIDCounter - 1) - firstIndex * 2048 #0
        thirdIndex = 0
        temp4 = 0
        if (temp2 > 1023):
            secondIndex = 1
            temp4 = temp2 - 1024
        else:
            temp4 = (self.table.RIDCounter - 1) % 2048
        fourthIndex = temp4 * 4
        arrayOfIndices.append(firstIndex)
        arrayOfIndices.append(secondIndex)
        arrayOfIndices.append(thirdIndex)
        arrayOfIndices.append(fourthIndex)
        self.table.page_directory[self.table.RIDCounter] = arrayOfIndices


    def addNewPageRange(self, secondIndex, fourthIndex):
        if (secondIndex == 0 and fourthIndex == 0):
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
        numColumns = self.table.num_columns
        firstIndex = self.table.page_directory[RIDCounter][0]
        secondIndex = self.table.page_directory[RIDCounter][1]
        fourthIndex = self.table.page_directory[RIDCounter][3]
        RIDPage = 1
        if (self.addNewPageRange(secondIndex, fourthIndex)):
            self.table.addPageRange()
        RIDPhysicalPage = self.table.pageRangeArray[firstIndex][secondIndex][RIDPage]
        self.addToByteArray(RIDPhysicalPage, fourthIndex, RIDCounter)
        for i in range(numColumns - 4):
            attribute = columns[i]
            thirdIndex = i + 4
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
        recordToPrint = []
        RID = self.table.keyToRID[key]
        indices = self.table.page_directory[RID]
        
        for i in range(self.table.num_columns-4):
            firstIndex = indices[0]
            secondIndex = indices[1]
            thirdIndex = i + 4
            fourthIndex = indices[3]
            tempByteArray = bytearray(4)
            j = 0
            while(j < 4):
                tempByteArray[j] = self.table.pageRangeArray[firstIndex][secondIndex][thirdIndex].data[fourthIndex + j]
                j = j + 1
#            print( int.from_bytes(tempByteArray, byteorder = 'big'))
            if (query_columns[i] == 1):
                recordToPrint.append(int.from_bytes(tempByteArray, byteorder = 'big'))
        print(recordToPrint)
        
            
            
            
            
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
