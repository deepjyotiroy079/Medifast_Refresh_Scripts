from datetime import datetime
import time
import tableauserverclient as TSC
from datetime import datetime
import time
import local_tokens
import logging, sys

logging.basicConfig(level=logging.WARNING, filename='C:/Users/esummers/Documents/python logs/_log_python_refreshes.log', filemode='a', format='%(asctime)s - %(lineno)d - %(message)s - %(name)s')# - %(levelname)s')

#'https://prod-useast-a.online.tableau.com/#/site/ictableaudevqauat/home'
sitename_prod = 'ictableauprod'
token_name = local_tokens.token_name
token_value_prod = local_tokens.token_secret
tableau_auth = TSC.PersonalAccessTokenAuth(token_name, token_value_prod, sitename_prod)
server = TSC.Server('https://prod-useast-a.online.tableau.com/', use_server_version=True)
server.auth.sign_in(tableau_auth)


names = ['SEM_OrderRecon_FACT_ORDER_MASTER_LITE','DC_Taken_Fulfilled_CarryInOut_Orders']
logging.warning(f'{sys.argv[0]} findingIDs for {names}')
#ids_to_refresh = ['86514b52-d6a1-46f7-b616-ac46f4863a11']      ###########REMOVE id after testing#################
ids_to_refresh = []
req_option = TSC.RequestOptions()
req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                 TSC.RequestOptions.Operator.In,
                                 names)) ####################searching all data sources for those in 'name' list
with server.auth.sign_in(tableau_auth):
    all_datasources, pagination_item = server.datasources.get(req_option)
    print("\nThere are {} datasources on site: ".format(pagination_item.total_available))
    #print([datasource.name for datasource in all_datasources])
    for d in all_datasources:
        print(d.id, d.name, d.updated_at, d.webpage_url, d.description) #description comes from info button/data source details/About
        ids_to_refresh.append(d.id)

import csv, pendulum, sys
############################LOOP through IDs and kick off refresh for each########################################
with server.auth.sign_in(tableau_auth):
    for x in ids_to_refresh:
        logging.warning(f"{sys.argv[0]} Started refresh at: {datetime.now().strftime('%H:%M:%S')}")
        print(f"Started refresh at: {datetime.now().strftime('%H:%M:%S')}")
        results = server.datasources.refresh(x)
        myJobId = results.id
        logging.warning(f'{sys.argv[0]} results for job {myJobId, d.name}: {results}')
        try:
            jobinfo = server.jobs.get_by_id(myJobId)
            while jobinfo.progress != '100':
                time.sleep(1)
                jobinfo = server.jobs.get_by_id(myJobId)
                print(jobinfo)
            print(f"{jobinfo.name} refresh reached {jobinfo.progress}% Completed at: {datetime.now().strftime('%H:%M:%S')}")
            logging.warning(f'{sys.argv[0]} results for job {myJobId} completed successfully at {end_time}')
        except: #############################Need to test more on info from bridge refreshes. Error thrown during jobinfo = server.jobs.get_by_id(myJobId)
            print('No job info available. Possible Bridge refresh.')
            time.sleep(5)
            end_time = pendulum.now('America/New_York')
            logging.warning(f'{sys.argv[0]} results for job {myJobId} completed successfully at {end_time}')
            continue