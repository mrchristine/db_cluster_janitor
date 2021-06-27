import time
from dbclient import *
from datetime import datetime as dt
from datetime import datetime, date
import re

class SQLAnalyticsClient(dbclient):

    def get_spark_versions(self):
        return self.get("/clusters/spark-versions", printJson=True)
    

    @staticmethod
    def has_keep_alive_tags(cinfo):
        keep_alive_tags = ["keepalive", "keep_alive"]
        customtags = cinfo.get("tags", {}).get("custom_tags")
        if customtags:
            tag_keys = [tag['key'].lower() for tag in customtags]
            if ("keepalive" in tag_keys) or ("keep_alive" in tag_keys):
                return True
            else:
                return False
        else:
            return False

    @staticmethod
    def has_keep_until_tags(cinfo):
        keep_until_tags = ["keepuntil", "keep_until"]
        customtags = cinfo.get("tags", {}).get("custom_tags")

        if customtags:
            for tag in customtags:
                if(tag['key'] in keep_until_tags):
                    date_str = tag['value'].lower()
                    date_str  = re.sub('[/\-]','-', date_str )
                    try:
                        expiry_dt = datetime.strptime(date_str, '%m-%d-%Y').date()
                    except:
                        return "Expired"  #not formatted right. Expire.
                    if(expiry_dt >= date.today()):
                        return "Stop" #not expired
                    else:
                        return "Expired" #expired
        return "False"        
    
    def get_sqlendpoints_list(self, alive=True):
        """ Returns an array of json objects for the endpoints. Grab the cluster_name or cluster_id """
        cluster_list = self.get("/sql/endpoints/", printJson=False).get('endpoints', None)
        if cluster_list != None:
            if alive:
                running = list(filter(lambda x: x['state'] == "RUNNING", cluster_list))
                for x in running:
                    print(x['name'] + ' : ' + x['id'])
                return running
            else:
                return cluster_list
        return []

    def get_terminate_sqlendpoints(self):

        cluster_list = self.get_sqlendpoints_list()

        if cluster_list:
            # kill list array
            terminate_cluster_list = []
            for cluster in cluster_list:
                co = dict()
                co['cluster_name'] = cluster['name']
                co['creator_user_name'] = cluster['creator_name']
                co['cluster_id'] = cluster['id']
                co['autotermination_minutes'] = cluster['auto_stop_mins']
                
                co['cluster_details'] = {'min_num_clusters': cluster['min_num_clusters'],
                                         'max_num_clusters': cluster['max_num_clusters'],
                                         'cluster_size': cluster['cluster_size']}
  
                keep1 = SQLAnalyticsClient.has_keep_alive_tags(cluster)
                keep2 = SQLAnalyticsClient.has_keep_until_tags(cluster)
                co['keep_alive']=keep1 #False means no keepalive flag so stop
                co['keep_until']=keep2 #Expire or Stop 
                if (keep1==True and keep2=="Expired" ): #keepalive and keepuntil expired. Expiry takes precedence
                    terminate_cluster_list.append(co)
                elif keep1==True and keep2=="Stop": #keepalive and keepuntil Stop. keepalive takes precedence
                    pass 
                elif keep1==True and keep2=="False": #keepalive and no keepuntil.
                    pass
                else: #no keepalive. keepuntil rules kick in.
                    terminate_cluster_list.append(co)

            return terminate_cluster_list
        return []

    def stop_cluster(self, cid=None):
        """ Stop the Endpoint with id """
        if cid:
            url = f"/sql/endpoints/{cid}/stop" 
            resp = self.post(url)
            pprint_j(resp)

    def kill_cluster(self, cid=None):
        """ Kill the endpoint with id """
        if cid:
            url = f"/sql/endpoints/{cid}" 
            resp = self.delete(url)
            pprint_j(resp)




