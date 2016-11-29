######## import buil-in ################################################
import sys, os
import logging

############## get root path ###############################
cwd = os.path.realpath(__file__)
disk_path = cwd.split('\\')
if len(disk_path) == 1:
	disk_path = '/'
else:
	disk_path = disk_path[0] + '/'	

root_path = disk_path + 'home/hieppm/git_repos/'
parent_dir = 'data_mining_python/' # SUBJECT TO CHANGE
parent_path = root_path + parent_dir


############## Append general paths #############################
sys.path.append(parent_path + 'config/')
sys.path.append(parent_path + 'libraries/')
sys.path.append(parent_path + 'utilities/')
sys.path.append(parent_path + 'action/collect_data/')

# Testing
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.debug('Done initialization')