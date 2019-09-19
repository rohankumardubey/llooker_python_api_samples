import yaml ### install the pyyaml package
from lookerapi import LookerApi
from datetime import datetime
from pprint import pprint
from argparse import ArgumentParser
### ------- HERE ARE PARAMETERS TO CONFIGURE -------
host = 'localhost'
project_name = 'hub'
branch_name = 'dev-spoke-spoke-pwrc'
user_id = 945
role_to_add_id = 136
### ------- OPEN THE CONFIG FILE and INSTANTIATE API -------

f = open('config.yml')
params = yaml.load(f)
f.close()

my_host = params['hosts'][host]['host']
my_secret = params['hosts'][host]['secret']
my_token = params['hosts'][host]['token']

looker = LookerApi(host=my_host,
                 token=my_token,
                 secret = my_secret)

#add role to get access to core model

roles = looker.get_user_role(user_id,"id")


roles_source = roles.copy()
roles.append({"id":role_to_add_id})

try:
    looker.set_user_role(user_id,[role['id'] for role in roles])
    #begin issuing calls as the user who needs to catch up to production
    looker.login_user(user_id)
    looker.update_session_workspace()
    looker.switch_git_branch(project_name,branch_name)
    looker.reset_to_production(project_name)

except:
    print('Production reset failed. Resetting roles.')
    pass
#
# #might need to use ref  in production catch up rather than reset to prod but seems fine in testing
# #alternate would be PUT /projects/{project_id}/git_branch with  {"ref":"master_ref"} as body which we could get as the admin
#
# # #need to go back to old authentication with admin
admin_looker = LookerApi(host=my_host,
                 token=my_token,
                 secret = my_secret)
#
# # #remove role added

roles.remove({"id":role_to_add_id})
admin_looker.set_user_role(user_id,body=[role['id'] for role in roles])
