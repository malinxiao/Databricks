# Databricks notebook source
##################################################################################
# The master class that partitions the travel groups and trigger the sub notebooks 
# execution for each partition in parallel
##################################################################################
class GroupTravelPlanning:
  
  def __init__(self, partitionNum, travelGroups):
    self.partitionNum = partitionNum
    self.travelGroups = travelGroups
    self.groupPartitionList = self.PartitionTravelGroups()
  
  #Partition travel groups
  def PartitionTravelGroups(self):

    #Initialise a dictionary for hosting the partitions of travel groups
    partitions={}
    for partitionID in range(0, self.partitionNum):
      partitions[partitionID]=[]
      
    #Partition the travel groups 
    for index, groupID in enumerate(self.travelGroups.keys(), start=0):
      partitionID = index % self.partitionNum
      partitions[partitionID].append(groupID)
  
    #Convert the travel groups partition dictionary into an list of travel 
    #group strings, such as ['Group-1,Group-2', 'Group-3,Group4',...] 
    groupPartitionList = []
    for group in partitions.values():
      groupPartition = ','.join(group)
      groupPartitionList.append(groupPartition)
    return groupPartitionList
  
  #Run group travel planning sub-notebooks in parallel
  def RunGroupTravelPlanningNotebooks(self):
    from multiprocessing.pool import ThreadPool
    pool = ThreadPool(self.partitionNum)  
    pool.map(
       lambda groupIDs: dbutils.notebook.run(
          '/Users/malinxiao@hotmail.com/Group Travel/GroupTravelPlanning_Parallel',
           timeout_seconds=0,
           arguments = {"groupIDs":groupIDs}),
       self.groupPartitionList)
  

# COMMAND ----------

#sample travel groups data dictionary
groups = {'Group-1':[('Seymour','BOS'),('Franny','DAL'),('Zooey','CAK'),('Walt','MIA'),('Buddy','ORD'),('Les','OMA'), ('Andy', 'CAK')],
          'Group-2':[('Eddie','BOS'),('Franny','DAL'),('Zooey','CAK'),('Walt','MIA'),('Buddy','ORD'),('Les','OMA'), ('Andy', 'CAK')],
          'Group-3':[('Tony','BOS'),('Franny','DAL'),('Zooey','CAK'),('Walt','MIA'),('Les','OMA'), ('Walt','MIA'),('Buddy','ORD') ],
          'Group-4':[('Tim','BOS'),('Franny','DAL'),('Zooey','CAK'),('Walt','MIA'),('Buddy','ORD'), ('Walt','MIA'),('Buddy','ORD') ],
          'Group-5':[('Simon','BOS'),('Franny','DAL'),('Zooey','CAK'),('Walt','MIA'),('Buddy','ORD'), ('Walt','MIA'),('Buddy','ORD') ],
          'Group-6':[('Seymour','BOS'),('Franny','DAL'),('Zooey','CAK'),('Walt','MIA'),('Buddy','CAK'),('Les','OMA'),('Buddy','ORD') ],
          'Group-7':[('Tom','BOS'),('Franny','DAL'),('Zooey','CAK'),('Walt','MIA'),('Les','OMA'),('Buddy','ORD'), ('Buddy','MIA')],
          'Group-8':[('Andy','BOS'),('Franny','DAL'),('Zooey','CAK'),('Walt','MIA'),('Buddy','ORD'), ('Buddy','MIA'), ('Seymour','BOS')]
         }

if __name__== "__main__":
  groupTravelPlanning = GroupTravelPlanning(4, groups)
  groupTravelPlanning.RunGroupTravelPlanningNotebooks()
