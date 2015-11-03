# Get a handle to the API client
from cm_api.api_client import ApiResource
import time
import sys
cm_host = raw_imput ("Enter IP address of CM: ")
cm_username = raw_imput ("Enter username: ")
cm_password = raw_imput ("Enter password: ")

api = ApiResource(cm_host,server_port=7180, username=cm_username, password=cm_password)
hosts = api.get_all_hosts()

# Get a list of all clusters
print "Clusters:"
cdh5 = None
for c in api.get_all_clusters():
       print c.name
       if c.version == "CDH5":
               cdh5 = c

#Print all hosts
print "Hosts:"
for i in hosts:
        print i

#cdh5.rolling_restart(stale_configs_only=1) works only in Enterprise version

#Get list of all services
print "Services:"
for s in cdh5.get_all_services():
	print s
	if s.type == "HDFS":
		hdfs = s
	print s.name, s.serviceState, s.healthSummary

#Get list of roles on Data Nodes
print "Data Nodes:"
nn = None
for r in hdfs.get_all_roles():
	if r.type == 'DATANODE':
		nn = r
		print "Role name: %s\nState: %s\nHealth: %s\nHost: %s" % ( nn.name, nn.roleState, nn.healthSummary, nn.hostRef.hostId)
		hdfs.restart_roles(nn.name)
		kk=hdfs.get_role(nn.name)
		print kk.roleState
		while kk.roleState <> 'STARTED':
			time.sleep(5)
			kk=hdfs.get_role(nn.name)
			print kk.roleState
			if (kk.roleState == 'N/A') or (kk.roleState =='UNKNOWN'):
                                sys.exit("The service can't be started, exiting....")

