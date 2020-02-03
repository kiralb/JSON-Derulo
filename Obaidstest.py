

def addIDToByteArray(idNumber, OFFSET, currentIDsInTheByteArray, biteArray):
	# Converts Any idNumber that is given and returns a
	# bye array from it 

	
	idNumberString = str(idNumber)
	#arr = bytearray(len(idNumberString))
	#i = currentIDsInTheByteArray * OFFSET
	i = OFFSET * currentIDsInTheByteArray
	j = 0
	while(j < len(idNumberString)):
		biteArray[i] = int(idNumberString[j])
		#print("BiteArray at i" ,biteArray[i], "\n")
		i = i + 1
		j = j + 1

	#print(biteArray)
	#return arr 
def addQuiz1ToByteArray(Quiz1, QOFFSET, currentQuiz1sInTheByteArray, q1BiteArray):
	# Converts Any idNumber that is given and returns a
	# bye array from it 

	
	quizNumberString = str(Quiz1)
	#arr = bytearray(len(idNumberString))
	#i = currentIDsInTheByteArray * OFFSET
	i = QOFFSET * currentQuiz1sInTheByteArray
	j = 0
	while(j < len(quizNumberString)):
		q1BiteArray[i] = int(quizNumberString[j])
		#print("BiteArray at i" ,biteArray[i], "\n")
		i = i + 1
		j = j + 1



#Given a Byte Array, convert it back to it's ID
def convertByteArrayToID(arr):
#	arr

	j = 0
	appendedId = ""
	while(j < len(arr)):
		appendedId = appendedId + str(arr[j])
		j = j + 1
	int(appendedId)
	return(appendedId)



idNumbers = [914143011, 123456789, 913151525, 987654321]
OFFSET = 9
currentIDsInTheByteArray = 0
size = 4096
biteArray = bytearray(size)

QOFFSET = 3 #Quiz Offset 
q1BiteArray = bytearray(size)




j = 0
while(j < len(idNumbers)):
	addIDToByteArray(idNumbers[j], OFFSET, currentIDsInTheByteArray, biteArray)
	currentIDsInTheByteArray += 1
	j = j + 1

z = 0
while(z < 36):
	print(biteArray[z], "z Value: ", z, "\n")

	if((z+1) % 9 == 0):
		print("\n")
	z = z + 1


#ADd IDs to Our BiteArray (Different BiteArray)
	##Change the offset

#Add Quiz1 to our BiteArray  (DifferentBiteArray)
#Add Quiz2 to our BiteArray (DifferentBiteArray)
#Add Quiz3 to our BiteArray (DifferentBiteArray)
#Add Keys to bite array 