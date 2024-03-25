from datetime import datetime
import time
import tableauserverclient as TSC
from datetime import datetime
import time

#'https://prod-useast-a.online.tableau.com/#/site/ictableaudevqauat/home'
sitename = 'ictableaudevqauat'
sitename_prod = 'ictableauprod'
token_name = 'python'
token_value = 'YgxTB5RfRhOBW3PuDVL0Xw==:ExKfF2EijjbJHHzUKM1kM2biPB0dNdCF'
token_value_prod = '+ekebdFcTl2flSrpvyjhyw==:2MCkhwsZudmhFeUe2xWsnFUF8RJQbClJ'

tableau_auth = TSC.TableauAuth('insightscenter@medifastinc.com', 'r2A=fbj#/@q.', sitename_prod)

##############################authenticate to DEV#############################
#tableau_auth = TSC.PersonalAccessTokenAuth(token_name, token_value, sitename)

###################UNCOMMENT to authenticate to PROD###################################
#tableau_auth = TSC.PersonalAccessTokenAuth(token_name, token_value_prod, sitename_prod)
server = TSC.Server('https://prod-useast-a.online.tableau.com/', use_server_version=True)

server.auth.sign_in(tableau_auth)
#server = TSC.Server('https://34.230.1.91') 


##############################OBTAIN DATASOURCE IDs#######################################
##########################UPDATE names list with your datasource names#####################

names = ['SEM_OrderRecon_FACT_ORDER_MASTER_LITE']
#names = ['SEM_OrderRecon_FACT_ORDER_MASTER_LITE','SEM_OrderRecon_FACT_SUMM_ORDER_RECON']
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


############################LOOP through IDs and kick off refresh for each########################################
with server.auth.sign_in(tableau_auth):
    for x in ids_to_refresh:
        print(f"Started refresh at: {datetime.now().strftime('%H:%M:%S')}")
        results = server.datasources.refresh(x)
        myJobId = results.id
        try:
            jobinfo = server.jobs.get_by_id(myJobId)
            while jobinfo.progress != '100':
                time.sleep(1)
                jobinfo = server.jobs.get_by_id(myJobId)
                print(jobinfo)
            print(f"{jobinfo.name} refresh reached {jobinfo.progress}% Completed at: {datetime.now().strftime('%H:%M:%S')}")
        except: #############################Need to test more on info from bridge refreshes. Error thrown during jobinfo = server.jobs.get_by_id(myJobId)
            print('No job info available. Possible Bridge refresh.')
            time.sleep(5)
            continue