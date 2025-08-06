import pickle

with open('/home/kinow/autosubmit/o002/pkl/job_list_o002.pkl', 'rb') as fd:
    job_list = pickle.load(fd)

with open('/home/kinow/autosubmit/o002/pkl/job_list_o002_broken.pkl', 'rb') as fd:
    job_list_broken = pickle.load(fd)

print('done')