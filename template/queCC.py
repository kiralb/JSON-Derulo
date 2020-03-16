import threading
from queue import Queue


class QueCC:
    """
    # Creates a new queCC object
    lstOfPlanningthreads = []
    listofworkerthreads =[]
    """

    def __init__(self):
        self.setsOfQueues = [] # holds 16 queues
        self.plannerThreadsRun = 0
        for _ in range(4):
            listOfQueues = []
            for _ in range(4):
                q = Queue(maxsize = 0) # if maxsize = 0, infinite size
                listOfQueues.append(q)
            self.setsOfQueues.append(listOfQueues)
        pass
