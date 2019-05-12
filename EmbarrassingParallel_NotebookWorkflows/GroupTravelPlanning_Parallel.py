# Databricks notebook source
#parameter of groupIDs string passed in from Master notebook
dbutils.widgets.text("groupIDs", "", "Group IDs")

# COMMAND ----------

#The code in this cell is from https://github.com/arthur-e/Programming-Collective-Intelligence/blob/master/chapter5/optimization.py which 
#is authored by Toby Segaran as an example of optimisation algorithems for group travel planning. The orginal code only processes one group.
#Some small changes have been made in order to take 'group' as a parameter so that the code can be suitable for multiple groups.

import time
import random
import math

def getminutes(t):
  x=time.strptime(t,'%H:%M')
  return x[3]*60+x[4]

def printschedule(r, people):
  for d in range(int(len(r)/2)):
    name=people[d][0]
    origin=people[d][1]
    out=flights[(origin,destination)][int(r[d])]
    ret=flights[(destination,origin)][int(r[d+1])]
    print( name,origin,out[0],out[1],out[2], ret[0],ret[1],ret[2])

def schedulecost(sol, people):
  totalprice=0
  latestarrival=0
  earliestdep=24*60
  
  for d in range(int(len(sol)/2)):
    # Get the inbound and outbound flights
    origin=people[d][1]
    outbound=flights[(origin,destination)][int(sol[d])]
    returnf=flights[(destination,origin)][int(sol[d+1])]
    
    # Total price is the price of all outbound and return flights
    totalprice+=outbound[2]
    totalprice+=returnf[2]
    
    # Track the latest arrival and earliest departure
    if latestarrival<getminutes(outbound[1]): latestarrival=getminutes(outbound[1])
    if earliestdep>getminutes(returnf[0]): earliestdep=getminutes(returnf[0])
  
  # Every person must wait at the airport until the latest person arrives.
  # They also must arrive at the same time and wait for their flights.
  totalwait=0  
  for d in range(int(len(sol)/2)):
    origin=people[d][1]
    outbound=flights[(origin,destination)][int(sol[d])]
    returnf=flights[(destination,origin)][int(sol[d+1])]
    totalwait+=latestarrival-getminutes(outbound[1])
    totalwait+=getminutes(returnf[0])-earliestdep  

  # Does this solution require an extra day of car rental? That'll be $50!
  if latestarrival>earliestdep: totalprice+=50
  
  return totalprice+totalwait

def geneticoptimize(domain,costf, group, popsize=50,step=1,
                    mutprob=0.2,elite=0.2,maxiter=2000):
  # Mutation Operation
  def mutate(vec):
    i=random.randint(0,len(domain)-1)
    if random.random()<0.5 and vec[i]>domain[i][0]:
      return vec[0:i]+[vec[i]-step]+vec[i+1:] 
    elif vec[i]<domain[i][1]:
      return vec[0:i]+[vec[i]+step]+vec[i+1:]
  
  # Crossover Operation
  def crossover(r1,r2):
    i=random.randint(1,len(domain)-2)
    return r1[0:i]+r2[i:]

  # Build the initial population
  pop=[]
  for i in range(popsize):
    vec=[random.randint(domain[i][0],domain[i][1]) 
         for i in range(len(domain))]
    
    pop.append(vec)
  
  
  # How many winners from each generation?
  topelite=int(elite*popsize)
  
  # Main loop 
  for i in range(maxiter):
    scores=[(costf(v, group),v) for v in pop if v is not None]
    scores.sort()
    ranked=[v for (s,v) in scores]
    
    # Start with the pure winners
    pop=ranked[0:topelite]
    
    # Add mutated and bred forms of the winners
    while len(pop)<popsize:
      if random.random()<mutprob:

        # Mutation
        c=random.randint(0,topelite)
        pop.append(mutate(ranked[c]))
      else:
      
        # Crossover
        c1=random.randint(0,topelite)
        c2=random.randint(0,topelite)
        pop.append(crossover(ranked[c1],ranked[c2]))
    
  return scores[0][1]


#Laguardia
destination='LGA'

flights={}

for line in open('/dbfs/ninjago/poc/schedule.txt'):
  origin,dest,depart,arrive,price=line.strip().split(',')
  flights.setdefault((origin,dest),[])

  # Add details to the list of possible flights
  flights[(origin,dest)].append((depart,arrive,int(price)))

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

#get the groupIDs string and fetch the travel group items in the sample travel groups data dictionary
groupIDs = dbutils.widgets.get("groupIDs").split(",")
currentGroups = []
for groupID in groupIDs:
  currentGroups.append(groups[groupID])
  
#run optimisation algorithem on the travel groups specified by the parameter
for group in currentGroups:
  domain=[(0,9)]*(len(group)*2)
  schedule = geneticoptimize(domain, schedulecost, group)

