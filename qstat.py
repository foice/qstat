#!/usr/bin/python
import subprocess
#from tabulate import tabulate
#from prettytable import PrettyTable
#from texttable import Texttable
#from beautifultable import BeautifulTable
import table_print

class job:
	def __init__(self):
        	pass
	
	def valorize(self,fields):
		#Job id                    Name             User            Time Use S Queue
		#------------------------- ---------------- --------------- -------- - -----
		#4772985.qmgr01             III-220K         mmconde         5082:05: R mpi_ib
		#[self.job_id,self.name,self.user,self.time,self.status,self.queue]=fields


		#                                                                         Req'd  Req'd   Elap
		#Job ID               Username Queue    Jobname          SessID NDS   TSK Memory Time  S Time
		#4784877.qmgr01.c     eberhard mpi_ib   Y_S155            10669     1  12    --  1500: R 13:33 
		[self.job_id,self.user,self.queue,self.name,self.sessID,self.nodes,self.tasks,self.mem,self.booked_time,self.status,self.elapsed_time]=fields


def list_users(jobs):
	job_users = [ j.user for j in jobs ] 
	job_users_running = [ j.user for j in jobs if j.status == 'R' ] 
	job_users_queued = [ j.user for j in jobs if j.status == 'Q' ] 
	job_users_running_tasks = [ j.user for k in range(int(j.tasks)) for j in jobs if j.status == 'R' ] 
	job_users_queued_tasks = [ j.user for k in range(int(j.tasks)) for j in jobs if j.status == 'Q' ] 
	
	unique_users=[]
	# COLLECT USER NAMES
	for job in jobs:
		unique_users.append(job.user)
	unique_users_set = set(unique_users)
	# GATHER INFO ON THE JOBS OF EACH USER AND MAKE A DICTIONARY OR LIST OF THEM
	user_activity={}
	for unique_user in unique_users_set:
		user_activity[unique_user] = [ [] ] # and empty matrix
	for job in jobs:
		user_activity[job.user].append( [job.queue, job.tasks, job.booked_time, job.elapsed_time, job.status]) # append new rows to the matrix
	# PRINT STUFF FOR EACH USER
	for unique_user in unique_users_set:
		print unique_user 
		print '\t Total: \t ',job_users.count(unique_user)
		print '\t Running Jobs:\t ',job_users_running.count(unique_user), '\t Running Tasks:\t ',job_users_running_tasks.count(unique_user)
		print '\t Queued Jobs:\t ',job_users_queued.count(unique_user), '\t Queued Tasks:\t ',job_users_queued_tasks.count(unique_user)
		


proc = subprocess.Popen(['qstat','-a'], stdout=subprocess.PIPE)
#
jobs=[]

while True:
  line = proc.stdout.readline()
  if line != '':
	#the real code does filtering here
	fields=line.split()
	if len(fields) == 11:
		# print fields
		# Job id                    Name             User            Time Use S Queue
		_job=job()
		_job.valorize(fields)
		if line[0] != '-':
			jobs.append(_job)
  else:
    break

print '****************************************'
print '****************************************'
list_users(jobs)
print '****************************************'
print '****************************************'

queues=[]
for job in jobs:
	queues.append(job.queue)
queues_set=set(queues)


job_users_running_queue={}
for q in queues_set:
	job_users_running_queue[q] = [ j for j in jobs if j.queue == q ] 

for q in queues_set:
	print '========================================'
	print q
	print '========================================'
	list_users(job_users_running_queue[q]) 
