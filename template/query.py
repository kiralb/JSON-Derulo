from template.table import Table, Record
from template.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    """

    def __init__(self, table):
        self.table = table
        self.numBinFiles = 1
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
#        print(self.table.TIDCounter)
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
#        print(arrayOfIndices)
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

    def insertIntoIndexObjects(self, columns):
        RID = self.table.RIDCounter
        accessListOfIndexObjects = self.table.listOfIndexObj
        for i in range(self.table.num_columns):
            indexObj = accessListOfIndexObjects[i]
            dictionaryOfIndex = indexObj.keyToRIDList
            attribute = columns[i]
            if attribute not in dictionaryOfIndex:
                dictionaryOfIndex[attribute] = [RID]
            elif attribute in dictionaryOfIndex:
                dictionaryOfIndex[attribute].append(RID)


    def addToBinFiles(self, columns):
        # print("this function got called")
        if (self.table.RIDCounter % 2049 == 0):
            self.numBinFiles += 1
        nameOfFile = "pageRange" + str(self.numBinFiles) + ".bin"
        f = open(nameOfFile, "wb")

        byteArrayToAdd = bytearray(4)
        for attribute in columns:
            byteArrayToAdd = (attribute).to_bytes(4, byteorder = 'big')
            f.write(byteArrayToAdd)
        f.close()





    """
    # Insert a record with specified columns
    """

    def insert(self, *columns):
        f = open("plsOpen.bin", "wb")
        """ Add to bin files """
        self.addToBinFiles(columns)
		#mapping will add to page_directory
		# for example, adding RID = 0 will add
		# page_directory = { 0: [0, 0, 0, 0] }
        self.mapRIDToIndices()
        schema_encoding = '0' * self.table.num_columns
        ### mapping keys to RIDs ###
        RIDCounter = self.table.RIDCounter
        self.insertIntoIndexObjects(columns)
        key = columns[0]
        # student ID matching with the RID
        self.table.keyToRID[key] = RIDCounter
        numColumns = self.table.num_columns + 4
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

    def addToRecordArray(self, key, record, rid):
        RID = rid
        firstIndex = self.table.page_directory[RID][0]
        secondIndex = self.table.page_directory[RID][1]
        fourthIndex = self.table.page_directory[RID][3]
        numColumns = self.table.num_columns + 4
        for i in range(numColumns - 4):
            thirdIndex = i + 4
            tempbytearray = bytearray(4)
            j = 0
            while (j < 4):
                tempbytearray[j] = self.table.pageRangeArray[firstIndex][secondIndex][thirdIndex].data[fourthIndex + j]
                j = j + 1
            attributeToAdd = (int).from_bytes(tempbytearray, byteorder = 'big')
            record.append(attributeToAdd)

    def getTIDIndirection(self, currentTID):
        firstIndex = self.table.page_directory2[currentTID][0]
        secondIndex = self.table.page_directory2[currentTID][1]
        thirdIndex = 0
        fourthIndex = self.table.page_directory2[currentTID][3]
        TIDIndirectionPage = self.table.pageRangeArray2[firstIndex][secondIndex][thirdIndex]

        j = 0
        tempbytearray = bytearray(4)
        while (j < 4):
            tempbytearray[j] = TIDIndirectionPage.data[fourthIndex + j]
            j = j + 1
#        print((int).from_bytes(tempbytearray, byteorder = 'big'))
        return (int).from_bytes(tempbytearray, byteorder = 'big')


    def getTIDsSchema(self, currentTID):
        firstIndex = self.table.page_directory2[currentTID][0]
        secondIndex = self.table.page_directory2[currentTID][1]
        thirdIndex = 3
        fourthIndex = self.table.page_directory2[currentTID][3]
        TIDPage = self.table.pageRangeArray2[firstIndex][secondIndex][thirdIndex]

        tempbytearray = bytearray(4)
        j = 0
        while (j < 4):
            tempbytearray[j] = TIDPage.data[fourthIndex + j]
            j = j + 1
        return (int).from_bytes(tempbytearray, byteorder = 'big')


    def putZerosInTheFront(self, number):
        y = str(number)
        i = 0
        while (i < 5):
            if (len(y) < 5):
                y = "0" + y
            i = i + 1

        return y


    def addToTIDRecordArray(self, TIDRecord, currentTID):
        firstIndex = self.table.page_directory2[currentTID][0]
        secondIndex = self.table.page_directory2[currentTID][1]
        fourthIndex = self.table.page_directory2[currentTID][3]
        numColumns = self.table.num_columns + 4

        for i in range(numColumns - 4):
            thirdIndex = i + 4
            physicalPage = self.table.pageRangeArray2[firstIndex][secondIndex][thirdIndex]

            tempbytearray = bytearray(4)
            j = 0
            while (j < 4):
                tempbytearray[j] = physicalPage.data[fourthIndex + j]
                j = j + 1
            TIDRecord.append(int.from_bytes(tempbytearray, byteorder = 'big'))





    def getLatestRecord(self, indirection, record, baseRID):
        currentTID = indirection
        TIDRecord = []
        self.addToTIDRecordArray(TIDRecord, currentTID)
        schemaIndexSet = "" # used later to check if schema index is in string

        while (self.getTIDIndirection(currentTID) != baseRID):
            TIDSchema = self.getTIDsSchema(currentTID)
            schema = self.putZerosInTheFront(TIDSchema)
#            print("TIDRecord: ", TIDRecord," schema: ", schema)
            for i in range(len(schema)):
                if (schema[i] == "1"):
                    if (str(i) not in schemaIndexSet):
                        schemaIndexSet += str(i)
                        record[i] = TIDRecord[i]
            currentTID = self.getTIDIndirection(currentTID)
            TIDRecord = []
            self.addToTIDRecordArray(TIDRecord, currentTID)
        TIDSchema = self.getTIDsSchema(currentTID)
        schema = self.putZerosInTheFront(TIDSchema)
        for i in range(len(schema)):
            if (schema[i] == "1"):
                if (str(i) not in schemaIndexSet):
                    schemaIndexSet += str(i)
                    record[i] = TIDRecord[i]


#        print("TID Record: ", TIDRecord)
#        print("record final: ", record)




    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    """

    def select(self, key, column, query_columns):
        listOfRecordObjects = []
        if (key not in self.table.listOfIndexObj[column].keyToRIDList):
            return listOfRecordObjects

        listOfRIDsToSelect = self.table.listOfIndexObj[column].keyToRIDList[key]
        for RID in listOfRIDsToSelect:
            # add to original data to record array
            record = []
            queryRecord = []

            baseRecordsRID = RID
            self.addToRecordArray(key, record, RID)

            baseRecordsIndirection = self.getIndirectionFromBaseRecord(RID)

            if (baseRecordsIndirection != 0):
                self.getLatestRecord(baseRecordsIndirection, record, baseRecordsRID)

            for i in range(self.table.num_columns):
                if (query_columns[i] == 1):
                    queryRecord.append(record[i])
                else:
                    queryRecord.append(None)

            recordObj = Record(baseRecordsRID, key, queryRecord)
            listOfRecordObjects.append(recordObj)

        # print("listOfRecordObjects: ", listOfRecordObjects[0].columns)

        return listOfRecordObjects








    def obtainIndirection(self, indirectionPage, fourthIndex):
        j = 0
        tempbytearray = bytearray(4)
        while (j < 4):
            tempbytearray[j] = indirectionPage.data[fourthIndex + j]
            j = j + 1
        return int.from_bytes(tempbytearray, byteorder = 'big')


    def getIndirectionFromBaseRecord(self, RID):
        baseRecordRID = RID
        firstIndex = self.table.page_directory[baseRecordRID][0]
        secondIndex = self.table.page_directory[baseRecordRID][1]
        thirdIndex = self.table.page_directory[baseRecordRID][2]
        fourthIndex = self.table.page_directory[baseRecordRID][3]

        indirectionPage = self.table.pageRangeArray[firstIndex][secondIndex][thirdIndex]
        indirection = self.obtainIndirection(indirectionPage, fourthIndex)
#        print("indirection: ", indirection)
        return indirection


    def obtainBaseRecordIndirectionPage(self, baseRecordRID):
        firstIndex = self.table.page_directory[baseRecordRID][0]
        secondIndex = self.table.page_directory[baseRecordRID][1]
        thirdIndex = 0
        return self.table.pageRangeArray[firstIndex][secondIndex][thirdIndex]


    def reassignBaseRecordIndirection(self, baseRecordIndirectionPage, TIDCounter, baseRecordRID):
#        print("TIDCounter: ", TIDCounter)
        offset = self.table.page_directory[baseRecordRID][3] # { 0: [0 0 0 0]}
        tempbytearray = (TIDCounter).to_bytes(4,'big')
        j = 0
        while (j < 4):
            baseRecordIndirectionPage.data[offset + j] = tempbytearray[j]
            j = j + 1
#        print(baseRecordIndirectionPage.data)
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
        numColumns = self.table.num_columns + 4
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
#        print("schemaString: ", schemaString)
        self.addSchemaString(TIDSchemaPage, schemaString, fourthIndex)

        IndirectionPage = 0
        TIDIndirectionPage = self.table.pageRangeArray2[firstIndex][secondIndex][IndirectionPage]

        baseRecordsIndirection = self.getIndirectionFromBaseRecord(self.table.keyToRID[key])
        baseRecordsRID = self.table.keyToRID[key]
        baseRecordsIndirectionPage = self.obtainBaseRecordIndirectionPage(baseRecordsRID)

        # if base record's indirection is null or 0, that means there isn't a tail record for it yet
        # so this will be the first tail record for that base record
        if (baseRecordsIndirection == 0):
            # change tail record's indirection to the base record's RID
#             print("got here")
             self.addToByteArray(TIDIndirectionPage, fourthIndex, baseRecordsRID)

        # else handle subsequent tail records if baseRecordsIndirection is != 0
        else:
#            print("got into else")
            # put base record indirection into tail's indirection column
            self.addToByteArray(TIDIndirectionPage, fourthIndex, baseRecordsIndirection)
#        print("got out of else")
        # update base record's indirection column to the tail record's TID
#        print(baseRecordsIndirectionPage.data)
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
    	summation = 0

    	for i in range(start_range, end_range + 1):
    		columnToAdd = self.select(i, 0, [1, 1, 1, 1, 1])
    		if (len(columnToAdd) != 0):
    			summation += columnToAdd[0].columns[aggregate_column_index]
    		# summation += columnToAdd










    	return summation


    	pass
