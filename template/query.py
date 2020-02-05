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
#        print(arrayOfIndices)
        self.table.page_directory[self.table.RIDCounter] = arrayOfIndices
        
    
    def mapTIDToIndices(self):
        arrayOfIndices = []
        print(self.table.TIDCounter)
        firstIndex = ( (2**31)- self.table.TIDCounter - 1)  // 2048
        secondIndex = 0
        temp2 = ( ((2**31)- self.table.TIDCounter - 1)) - firstIndex * 2048
        thirdIndex = 0
        temp4 = 0
        if (temp2 > 1023):
            secondIndex = 1
            temp4 = temp2 - 1024
        else:
            temp4 = ( (2**31)- self.table.TIDCounter - 1)  % 2048
        fourthIndex = temp4 * 4
        arrayOfIndices.append(firstIndex)
        arrayOfIndices.append(secondIndex)
        arrayOfIndices.append(thirdIndex)
        arrayOfIndices.append(fourthIndex)
        print(arrayOfIndices)
        self.table.page_directory2[self.table.TIDCounter] = arrayOfIndices
        
        
	


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
            self.table.addPageRange(self.table.pageRangeArray)
        RIDPhysicalPage = self.table.pageRangeArray[firstIndex][secondIndex][RIDPage]
        self.addToByteArray(RIDPhysicalPage, fourthIndex, RIDCounter)
        for i in range(numColumns - 4):
            attribute = columns[i]
            thirdIndex = i + 4
#            print("rid: ", RIDCounter, " firstIndex: ", firstIndex, " secondINdex: ", secondIndex, " thirdIndex: ", thirdIndex, " fourthIndex: ", fourthIndex)
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
        listOfRecordObjects = []
        arrayOfAttributes = []
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
                arrayOfAttributes.append(int.from_bytes(tempByteArray, byteorder = 'big'))
        recordObject = Record(RID, key, arrayOfAttributes)
        listOfRecordObjects.append(recordObject)
#        print(recordObject.columns)
        return listOfRecordObjects
        
            
        pass
        
    def obtainIndirection(self, indirectionPage, fourthIndex):
        j = 0
        tempbytearray = bytearray(4)
        while (j < 4):
            tempbytearray[j] = indirectionPage.data[fourthIndex + j]
            j = j + 1
        return int.from_bytes(tempbytearray, byteorder = 'big')
            
    
    def getIndirectionFromBaseRecord(self, key):
        baseRecordRID = self.table.keyToRID[key]
        firstIndex = self.table.page_directory[baseRecordRID][0]
        secondIndex = self.table.page_directory[baseRecordRID][1]
        thirdIndex = self.table.page_directory[baseRecordRID][2]
        fourthIndex = self.table.page_directory[baseRecordRID][3]
        
        indirectionPage = self.table.pageRangeArray[firstIndex][secondIndex][thirdIndex]
        indirection = self.obtainIndirection(indirectionPage, fourthIndex)
        return indirection
        
        
    def obtainBaseRecordIndirectionPage(self, baseRecordRID):
        firstIndex = self.table.page_directory[baseRecordRID][0]
        secondIndex = self.table.page_directory[baseRecordRID][1]
        thirdIndex = 0
        return self.table.pageRangeArray[firstIndex][secondIndex][thirdIndex]

        
    def reassignBaseRecordIndirection(self, baseRecordIndirectionPage, TIDCounter, baseRecordRID):
        offset = self.table.page_directory[baseRecordRID][3]
        tempbytearray = (TIDCounter).to_bytes(4,'big')
        j = 0
        while (j < 4):
            baseRecordIndirectionPage.data[offset + j] = tempbytearray[j]
            j = j + 1
        
        
    def addSchemaString(self, TIDSchemaPage, schemaString, offset):
        y = int(schemaString).to_bytes(4, byteorder = 'big')
        j = 0
        while (j < 4):
            TIDSchemaPage.data[offset + j] = y[j]
            j = j + 1
    
    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns): # 913151525, [None, 69 , None, None, None]
        TIDCounter = self.table.TIDCounter
        self.mapTIDToIndices()
        self.table.keyToTID[key] = TIDCounter
#        print(TIDCounter)
        numColumns = self.table.num_columns
        firstIndex = self.table.page_directory2[TIDCounter][0]
        secondIndex = self.table.page_directory2[TIDCounter][1]
        fourthIndex = self.table.page_directory2[TIDCounter][3]
        TIDPage = 1
        if (self.addNewPageRange(secondIndex, fourthIndex)):
            self.table.addPageRange(self.table.pageRangeArray2)
        TIDPhysicalPage = self.table.pageRangeArray2[firstIndex][secondIndex][TIDPage]
        self.addToByteArray(TIDPhysicalPage, fourthIndex, TIDCounter)
        
        schemaPage = 3
        TIDSchemaPage = self.table.pageRangeArray2[firstIndex][secondIndex][schemaPage]
        schemaString = ""
        for i in range(len(columns)):
            if (columns[i] == None):
                schemaString += "0"
            else:
                schemaString += "1"
        self.addSchemaString(TIDSchemaPage, schemaString, fourthIndex)
        
        IndirectionPage = 0
        TIDIndirectionPage = self.table.pageRangeArray2[firstIndex][secondIndex][IndirectionPage]
        
        baseRecordsIndirection = self.getIndirectionFromBaseRecord(key)
        baseRecordsRID = self.table.keyToRID[key]
        baseRecordsIndirectionPage = self.obtainBaseRecordIndirectionPage(baseRecordsRID)

        # if base record's indirection is null or 0, that means there isn't a tail record for it yet
        # so this will be the first tail record for that base record
        if (baseRecordsIndirection == 0):
            # change tail record's indirection to the base record's RID
             self.addToByteArray(TIDIndirectionPage, fourthIndex, baseRecordsRID)

        # else handle subsequent tail records if baseRecordsIndirection is != 0
        else:
            # put base record indirection into tail's indirection column
            self.addToByteArray(TIDIndirectionPage, fourthIndex, baseRecordsIndirection)
        
        # update base record's indirection column to the tail record's TID
        self.reassignBaseRecordIndirection(baseRecordsIndirectionPage, TIDCounter, baseRecordsRID)
        
        #TODO: UPDATE BASE RECORD'S SCHEMA ENCODING

             
            
            
        
        for i in range(numColumns - 4):
            if (columns[i] != None):
                attribute = columns[i]
                thirdIndex = i + 4
#                print("Tid: ", TIDCounter, " firstIndex: ", firstIndex, " secondINdex: ", secondIndex, " thirdIndex: ", thirdIndex, " fourthIndex: ", fourthIndex)
                physicalPageToAdd = self.table.pageRangeArray2[firstIndex][secondIndex][thirdIndex]
                self.addToByteArray(physicalPageToAdd, fourthIndex, attribute)
        

        
        
        
        
        
        self.table.TIDCounter -= 1
		
        pass

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass
