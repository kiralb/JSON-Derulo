from template.table import Table, Record
from template.index import Index
from template.db import Database
from template.bufferpool import BufferPool
import os

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    """

    def __init__(self, table):
        self.table = table
        self.chdirFlag = 0
        self.bufferpoolSize = 0
        self.bufferpool = []
        self.BufferpoolFiles = [] # ["basePageRange1.bin", "tailPageRange1.bin"]
        self.numBaseBinFiles = 1
        self.numTailBinFiles = 1
        self.globalTransactionsCount = 0
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




    """
    # Insert a record with specified columns
    """
    def createBinFile(self):
        # print("RID: ", self.table.RIDCounter)
        if (self.table.RIDCounter % 2048 == 1):
            return 1
        return 0

    def makeNewBinFile(self):
        nameOfFile = "basePageRange" + str(self.numBaseBinFiles) + ".bin"
        # print("making new bin file: ", nameOfFile)
        f = open(nameOfFile, "ab")
        return f

    def recordsPageNotInPool(self, baseRID):
        if (self.bufferpoolSize == 0):
            return 1
        baseBinFileNeeded = "basePageRange" + str(((baseRID - 1)// 2048) + 1) + ".bin"
        # print("base bin file: ", baseBinFileNeeded)
        if (baseBinFileNeeded not in self.BufferpoolFiles):
            return 1
        return 0

    def makeCopyOfBinFileInPool(self, bufferpoolObj):
        fileToCopy = "basePageRange" + str(((self.table.RIDCounter - 1)// 2048) + 1) + ".bin"
        with open(fileToCopy, "rb") as binaryfile :
            bufferpoolObj.contents = bytearray(binaryfile.read())


    def addRecordTocopy(self, columns, copy):
        contentsToAdd = columns
        # contentsToAdd = columns

        tempbytearray = bytearray(4)
        for attribute in contentsToAdd:
            if (attribute == None):
                continue
            # print("attribute: ", attribute)
            tempbytearray = attribute.to_bytes(4, byteorder = 'big')
            # print("HI HELLO WTF")
            # print("offset: ", offset, " j: ", j)
            # copy.contents[offset] = tempbytearray[j]
            copy.contents = copy.contents + tempbytearray
            # offset += 1


    def evictPage(self, leastRecentlyUsed, FilesArray, FileNamesArray, fileToGetEvictedByteArray, nameOfFileOfEvicted, nameOfReplacementFileByteArray):
    	#fileToGetEvictedByteArray
        x = leastRecentlyUsed
    	#Need to get the name of the file, righ tnow we will pass it in but we can fix that by calling the contents.name
        f = open(nameOfFileOfEvicted , "wb")
        f.write(fileToGetEvictedByteArray)
        f.close()

        f = open(nameOfReplacementFileByteArray, "rb")
        readingByteArray = f.read()
    	#print(readingByteArray)
        f.close()


        FilesArray[x].contents = bytearray(readingByteArray)
        FileNamesArray[x] = nameOfReplacementFileByteArray

    def bringToBufferpool(self, bufferpoolObj, fileToAdd):
        #if bufferpool not full, just add page from disk to bufferpool
        if (self.bufferpoolSize != 5):
            self.bufferpool.append(bufferpoolObj)
            self.makeCopyOfBinFileInPool(bufferpoolObj)
            # bufferpoolObj = bufferPoolObjToAdd
            self.BufferpoolFiles.append(fileToAdd)
            self.bufferpoolSize += 1
            # print("files: ", self.BufferpoolFiles)
        # if it is full, need to evict a page using LRU
        else:
            leastRecentlyUsed = self.bufferpool[0].numTransactions
            indexOfLeastRecentlyUsed = 0
            for index, bufferpoolObject in enumerate(self.bufferpool):
                # print("LRU: ", leastRecentlyUsed, " index: ", index)
                if (bufferpoolObject.numTransactions < leastRecentlyUsed):
                    leastRecentlyUsed = bufferpoolObject.numTransactions
                    indexOfLeastRecentlyUsed = index

            # evict page here
            filesArray = self.bufferpool
            fileNamesArray = self.BufferpoolFiles
            fileToGetEvictedByteArray = self.bufferpool[indexOfLeastRecentlyUsed].contents
            nameOfFileOfEvicted = self.BufferpoolFiles[indexOfLeastRecentlyUsed]
            nameOfReplacementFileByteArray = fileToAdd

            self.evictPage(indexOfLeastRecentlyUsed, filesArray,
                fileNamesArray, fileToGetEvictedByteArray, nameOfFileOfEvicted,
                nameOfReplacementFileByteArray )

            #I'm trying to see if i can access and traverse through the
            #bytearray that was replaced due to eviction
            # print("Evicted Contents", self.bufferpool[indexOfLeastRecentlyUsed].contents)


            """ SUS 2 """
            # bufferpoolObj = self.bufferpool[indexOfLeastRecentlyUsed]
            # bufferpoolObj.numTransactions = self.globalTransactionsCount
            return indexOfLeastRecentlyUsed




    def insert(self, *columns):
        # student ID matching with the RID
        key = columns[0]
        self.table.keyToRID[key] = self.table.RIDCounter

        """ START of Durable Implentation """

        self.globalTransactionsCount += 1
        """ Add to bin files """
        if (self.chdirFlag == 0):
            os.chdir('ECS165/' + self.table.name)
            self.chdirFlag = 1

        file = None
        if (self.createBinFile()):
            file = self.makeNewBinFile()
            self.numBaseBinFiles += 1


        baseFileAdded = "basePageRange" + str(((self.table.RIDCounter - 1)// 2048) + 1) + ".bin"

        bufferpoolObj = None
        if (self.recordsPageNotInPool(self.table.RIDCounter)):
            # add empty bufferpool object to bufferpool
            bufferpoolObj = BufferPool(self.table.num_columns)
            bufferpoolObj.pin = 1
            bufferpoolObj.numTransactions = self.globalTransactionsCount

            self.bringToBufferpool(bufferpoolObj, baseFileAdded)
        else:
            index = self.BufferpoolFiles.index(baseFileAdded)
            bufferpoolObj = self.bufferpool[index]

        # print("bufferpoolObj: ", bufferpoolObj.contents)
        self.addRecordTocopy(columns, bufferpoolObj)
        bufferpoolObj.dirty = 1
        bufferpoolObj.pin = 0

        """ END Durable implementation """


        # mapping base RID to what file it can be found in and at what offset in that file
        offset = 4 * self.table.num_columns * (self.table.RIDCounter - 1)
        self.table.baseRecordDirectory[self.table.RIDCounter] = [baseFileAdded, offset]

        # initialize metadata columns
        self.table.baseMetaData[0][self.table.RIDCounter] = 0 # map all RIDs indirection to 0
        self.table.baseMetaData[1][self.table.RIDCounter] = '0' * self.table.num_columns # map all RIDs schema to 0



        """ END of Durable Implentation """


		#mapping will add to page_directory
		# for example, adding RID = 0 will add
		# page_directory = { 0: [0, 0, 0, 0] }
        self.mapRIDToIndices()
        schema_encoding = '0' * self.table.num_columns
        ### mapping keys to RIDs ###
        RIDCounter = self.table.RIDCounter
        self.insertIntoIndexObjects(columns)
        key = columns[0]
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
        print("TIDRecord: ", TIDRecord)


    def addToTIDRecordArray2(self, TIDRecord, currentTID):
        # check if TIDs corresponding page is in bufferpool
        bufferpoolObj = BufferPool(self.table.num_columns)
        tailBinFileNeeded = "tailPageRange" + str(( (2**31)- currentTID - 1)  // 2048 + 1) + ".bin"
        # print("tailbinfileneeded: ", tailBinFileNeeded)
        # print("files: ", self.BufferpoolFiles)
        index = 0
        if (tailBinFileNeeded not in self.BufferpoolFiles):
            print("bringing to bufferpool")
            index = self.bringToBufferpool(bufferpoolObj, tailBinFileNeeded)
            # print("printing bufferpool: ", self.BufferpoolFiles)

        else:
            index = self.BufferpoolFiles.index(tailBinFileNeeded)
            bufferpoolObj = self.bufferpool[index]
            # print("here: " ,bufferpoolObj.contents)

        offset = 2047 - (currentTID % 2048)
        #do obaid's bytearray shite
        tempbyteArray = bytearray(4)
        for i in range(self.table.num_columns):
            j = 0
            while (j < 4):
                # print("PLS: ",bufferpoolObj.contents)
                # print(bufferpoolObj.contents[offset])
                tempbyteArray[j] = bufferpoolObj.contents[offset]
                j = j + 1
                offset = offset + 1
            # print(int.from_bytes(   tempbyteArray, byteorder = 'big'))
            TIDRecord.append(int.from_bytes(tempbyteArray, byteorder = 'big'))
            tempbyteArray = bytearray(4)
        print("TIDREcord: ", TIDRecord)



    def getLatestRecord2(self, indirection, record, baseRID):
        currentTID = indirection
        TIDRecord = []
        self.addToTIDRecordArray(TIDRecord, currentTID)
        schemaIndexSet = ""
        # whichSchema = -1
        while (self.table.tailMetaData[0][currentTID] != baseRID):
            TIDSchema = self.table.tailMetaData[1][currentTID]
            for i in range(len(TIDSchema)):
                if (TIDSchema[i] == "1"):
                    if (str(i) not in schemaIndexSet):
                        schemaIndexSet += str(i)
                        record[i] = TIDRecord[i]
            currentTID = self.table.tailMetaData[0][currentTID]
            TIDRecord = []
            self.addToTIDRecordArray(TIDRecord, currentTID)
        TIDSchema = self.table.tailMetaData[1][currentTID]
        for i in range(len(TIDSchema)):
            if (TIDSchema[i] == "1"):
                if (str(i) not in schemaIndexSet):
                    schemaIndexSet += str(i)
                    record[i] = TIDRecord[i]


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

            # baseRecordsIndirection = self.getIndirectionFromBaseRecord(RID)
            baseRecordsIndirection = self.table.baseMetaData[0][RID]

            if (baseRecordsIndirection != 0):
                # print("entered getLatestRecord")
                # self.getLatestRecord(baseRecordsIndirection, record, baseRecordsRID)
                self.getLatestRecord2(baseRecordsIndirection, record, baseRecordsRID)

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


    def createTailBinFile(self):
        # print("TID: ", self.table.TIDCounter)
        if ((self.table.TIDCounter % 2048 + 2) % 2048 == 1):
            # print("got here in createTailBinFile")
            return 1
        return 0

    def makeNewTailBinFile(self):
        nameOfFile = "tailPageRange" + str(self.numTailBinFiles) + ".bin"
        # print("making new bin file: ", nameOfFile)
        f = open(nameOfFile, "ab")
        return f

    def tailrecordsPageNotInPool(self, tailTID):
        if (self.bufferpoolSize == 0):
            return 1
        tailBinFileNeeded = "tailPageRange" + str(( (2**31)- tailTID - 1)  // 2048 + 1) + ".bin"


        # print("tail bin file: ", tailBinFileNeeded)

        if (tailBinFileNeeded not in self.BufferpoolFiles):
            # print("returning 1")
            return 1
        # print("returning 0")
        return 0


    def assignTailIndirection(self, key):
        RID = self.table.keyToRID[key]
        indirColOfRID = self.table.baseMetaData[0][RID]


        # sets current base record's indirection column equal to the TID of the latest tail record
        self.table.baseMetaData[0][RID] = self.table.TIDCounter
        if (indirColOfRID == 0):
            # if base record hasn't been updated, just return RID
            return RID
        else:
            # if it has been updated, return previous TID
            return indirColOfRID

    def assignTailSchema(self, columns):
        schemaString = ""
        for i in range(len(columns)):
            if (columns[i] == None):
                schemaString += "0"
            else:
                schemaString += "1"
        return schemaString

    def assignTailMetaData(self, key, columns):
        indirection = self.assignTailIndirection(key)
        schema = self.assignTailSchema(columns)
        self.table.tailMetaData[0][self.table.TIDCounter] = indirection
        # print("SCHEMA IS: ", schema)
        self.table.tailMetaData[1][self.table.TIDCounter] = schema





    def addTailRecordTocopy(self, key, columns, copy):
        self.assignTailMetaData(key, columns) # indirection and schema columns placed in memory
        contentsToAdd = columns
        # print(contentsToAdd)
        # contentsToAdd = columns

        offset = 2047 - (self.table.TIDCounter % 2048)
        #tempbytearray = bytearray(4)
        j = 0

        #offset = 10
        #tempbyteArray = (914143011).to_bytes(4, byteorder = 'big')
        #while(j < 4):
            #copy.contents[j] = tempbyteArray[j]
            # j = j + 1
            #copy.contents[j] = tempbyteArray[j]
            #j = j + 1


        for attribute in contentsToAdd: # [none, 14, none, none, none]
            # print("copy.contents ", copy.contents[0:32])
            if (attribute == None):
                tempbytearray = (0).to_bytes(4, byteorder = 'big')
                copy.contents += tempbytearray
                continue

            tempbytearray = attribute.to_bytes(4, byteorder = 'big')
            i = 0
            copy.contents += tempbytearray
            # print("temp: ", tempbytearray)
            # print("HI HELLO WTF")
            # print("offset: ", offset, " j: ", j)
            # copy.contents[offset] = tempbytearray[j]
            # copy.contents = copy.contents + tempbytearray

            # print(copy.contents[0:16])

        # print(copy.contents[0:32])
        # print("attribute: ", contentsToAdd)

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns): # 913151525, [None, 69 , None, None, None]
        print("entering update")
        self.globalTransactionsCount += 1
        self.table.keyToTID[key] = self.table.TIDCounter
        """ START Durable implementation """

        file = None
        # print("TID: ", self.table.TIDCounter)

        if (self.createTailBinFile()):
            file = self.makeNewTailBinFile()
            self.numTailBinFiles += 1
        tailFileAdded = "tailPageRange" + str(((2**31)- self.table.TIDCounter - 1)  // 2048 + 1) + ".bin"

        bufferpoolObj = None
        index = 0
        if (self.tailrecordsPageNotInPool(self.table.TIDCounter)):
            print("ENTERING HERE")
            # add empty bufferpool object to bufferpool
            bufferpoolObj = BufferPool(self.table.num_columns)
            bufferpoolObj.pin = 1
            bufferpoolObj.numTransactions = self.globalTransactionsCount
            """ SUS 1 """
            index = self.bringToBufferpool(bufferpoolObj, tailFileAdded)
        else:
            index = self.BufferpoolFiles.index(tailFileAdded)
            bufferpoolObj = self.bufferpool[index]

        # print("bufferpoolObj: ", bufferpoolObj.contents)
        """ SUS 3 """
        # self.addTailRecordTocopy(key, columns, bufferpoolObj)
        self.addTailRecordTocopy(key, columns, self.bufferpool[index])
        """ bufferpoolObj not connected to bufferpool[0] """
        # print("CONTENTS: ", self.bufferpool[0].contents)
        # print("CONTENTS: ", bufferpoolObj.contents[0:32])
        # print("here + ", bufferpoolObj.contents)
        bufferpoolObj.pin = 0

        """ END Durable implementation """



        TIDCounter = self.table.TIDCounter
        self.mapTIDToIndices()
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
