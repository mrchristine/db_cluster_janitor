import time
from dbclient import *


class ClustersClient(dbclient):

    def get_model_endpoints(self):
        model_resp = self.get('/preview/mlflow/endpoints/list')
        if model_resp['http_status_code'] != 200:
            return []
        return model_resp.get('endpoints', [])

    def terminate_all_model_endpoints(self, model_list=None):
        responses = []
        for model in model_list:
            terminate_resp = self.post("/preview/mlflow/endpoints/disable",
                                       {"registered_model_name": model['registered_model_name']})
            responses.append(terminate_resp)
        return responses

    def get_spark_versions(self):
        return self.get("/clusters/spark-versions", printJson=True)

    def get_cluster_list(self, alive=True):
        """ Returns an array of json objects for the running clusters. Grab the cluster_name or cluster_id """
        cluster_list = self.get("/clusters/list", printJson=False).get('clusters', None)
        if cluster_list:
            if alive:
                running = list(filter(lambda x: x['state'] == "RUNNING", cluster_list))
                for x in running:
                    print(x['cluster_name'] + ' : ' + x['cluster_id'])
                return running
            else:
                return cluster_list
        return []

    @staticmethod
    def reset_min_pool_args(pool_info):
        update_pool = {"instance_pool_id": pool_info.get('instance_pool_id', None),
                       "instance_pool_name": pool_info.get('instance_pool_name', None),
                       "node_type_id": pool_info.get('node_type_id', None),
                       "min_idle_instances": 0}
        return update_pool

    @staticmethod
    def get_pool_details(pool_info):
        pd = {"instance_pool_id": pool_info.get('instance_pool_id', None),
              "instance_pool_name": pool_info.get('instance_pool_name', None),
              "node_type_id": pool_info.get('node_type_id', None),
              "min_idle_instances": pool_info.get('min_idle_instances', None)}
        return pd

    def get_instance_pool_list(self):
        pool_list = self.get('/instance-pools/list').get('instance_pools', None)
        if pool_list:
            return pool_list
        return []

    def reset_instance_pool_min(self):
        pool_details = []
        pool_list = self.get_instance_pool_list()
        for pool in pool_list:
            pd = self.get_pool_details(pool)
            if pd.get('min_idle_instances', None) > 0:
                pool_details.append(pd)
            resp = self.post('/instance-pools/edit', self.reset_min_pool_args(pool))
        return pool_details

    @staticmethod
    def has_keep_alive_tags(cinfo):
        keep_alive_tags = ["keepalive", "keep_alive"]
        if cinfo.get('custom_tags', None):
            tag_keys = [tag.lower() for tag in cinfo['custom_tags'].keys()]
            if ("keepalive" in tag_keys) or ("keep_alive" in tag_keys):
                return True
        else:
            return False

    @staticmethod
    def is_serverless_cluster(cinfo):
        custom_tags = cinfo.get('custom_tags', None)
        if custom_tags:
            type_tag = custom_tags.get('ResourceClass', None)
            if type_tag == 'Serverless':
                return True
        return False

    def get_runtime_from_events(self, cid):
        print(f"Getting events for {cid}")
        resp = self.post('/clusters/events', {'cluster_id': cid})
        events = resp.get('events', None)
        if events:
            now = datetime.datetime.utcnow()
            for x in events:
                event = x['type']
                if event == 'RESTARTING' or event == 'STARTING':
                    runtime = now - datetime.datetime.utcfromtimestamp(x['timestamp'] / 1000)
                    hours_run = runtime.total_seconds() / 3600
                    return hours_run
        else:
            raise ValueError("No longer an admin ...")
        return 0

    def get_long_clusters(self, run_time_hours=0):
        # get current time
        now = datetime.datetime.utcnow()
        cluster_list = self.get('/clusters/list').get('clusters', None)

        if cluster_list:
            # grab the running clusters into a list
            running_cl_list = filter(lambda x: x['state'] == 'RUNNING', cluster_list)
            # kill list array
            long_cluster_list = []
            for x in running_cl_list:
                co = dict()
                co['start_time'] = str(datetime.datetime.utcfromtimestamp(x['start_time'] / 1000))
                co['cluster_name'] = x['cluster_name']
                co['creator_user_name'] = x['creator_user_name']
                co['cluster_id'] = x['cluster_id']
                co['autotermination_minutes'] = x['autotermination_minutes']
                if x.get('autoscale', None):
                    cluster_size = x.get('autoscale')
                else:
                    cluster_size = x.get('num_workers')
                co['cluster_details'] = {'worker_node_type': x['node_type_id'],
                                         'driver_node_type': x['driver_node_type_id'],
                                         'cluster_size': cluster_size}
                co['is_serverless'] = self.is_serverless_cluster(x)
                co['keep_alive'] = self.has_keep_alive_tags(x)
                # get the current time of cluster run times
                rt = now - datetime.datetime.utcfromtimestamp(x['start_time'] / 1000)
                hours_run_cluster = rt.total_seconds() / 3600
                hours_run_events = self.get_runtime_from_events(x['cluster_id'])
                # need to check hours run and stuff
                if hours_run_cluster > hours_run_events > 0:
                    co['hours_run'] = hours_run_events
                else:
                    co['hours_run'] = hours_run_cluster
                if co['hours_run'] > run_time_hours:
                    long_cluster_list.append(co)
            return long_cluster_list
        return []

    def kill_cluster(self, cid=None):
        """ Kill the cluster id of the given cluster """
        resp = self.post('/clusters/delete', {"cluster_id": cid})
        pprint_j(resp)

    def get_global_init_scripts(self):
        """ return a list of global init scripts """
        ls = self.get('/dbfs/list', {'path': '/databricks/init/'}).get('files', None)
        if ls is None:
            return []
        else:
            global_scripts = [{'path': x['path']} for x in ls if x['is_dir'] == False]
            return global_scripts

    def delete_init_script(self, path=None):
        """ delete the path passed into the api """
        resp = self.post('/dbfs/delete', {'path': path, 'recursive': 'false'})
        pprint_j(resp)

    def is_passthrough_cluster(self, cid=None):
        """ check if this is a single user cluster only """
        print("CID: {0}".format(cid))
        c_json = self.get("/clusters/get?cluster_id={0}".format(cid))
        c_config = c_json.get('spark_conf', None)
        if c_config:
            for k, v in c_config.items():
                if (k == "spark.databricks.passthrough.enabled") and (v == "true"):
                    return True
        return False

    def is_stream_running(self, cid=None):
        """ check if a streaming job is running on the cluster """
        # Get an execution context id. You only need 1 to run multiple commands. This is a remote shell into the environment
        print("CID: {0}".format(cid))
        ec = self.post('/contexts/create', {"language": "scala", "clusterId": cid}, version="1.2")
        # Grab the execution context ID
        ec_id = ec.get('id', None)
        if ec_id is None:
            return False
        # This launches spark commands and print the results. We can pull out the text results from the API
        command_payload = {'language': 'scala',
                           'contextId': ec_id,
                           'clusterId': cid,
                           'command': 'import scala.collection.JavaConverters._ ; Thread.getAllStackTraces().keySet().asScala.filter(_.getName.startsWith("stream execution thread for")).isEmpty'}

        command = self.post('/commands/execute', \
                            command_payload, \
                            version="1.2")
        com_id = command.get('id', None)
        if com_id is None:
            print(command)
            raise ValueError('Unable to run command on cluster')
        result_payload = {'clusterId': cid, 'contextId': ec_id, 'commandId': com_id}
        resp = self.get('/commands/status', result_payload, version="1.2")
        is_running = resp['status']

        # loop through the status api to check for the 'running' state call and sleep 1 second
        while (is_running == "Running"):
            resp = self.get('/commands/status', result_payload, version="1.2")
            is_running = resp['status']
            time.sleep(1)

        # check the final result to see if a streaming job is running
        is_not_streaming = 'false'
        if resp.get("results", None):
            payload = resp['results'].get('data', None)
            if payload is not None:
                is_not_streaming = payload.split('=')[1].strip().lower()
        else:
            print("Results are missing: \n")
            print(str(resp))
        # this returns false if there is no stream running, otherwise it returns true
        # false: stream is not running
        # true: stream is running
        if is_not_streaming == 'true':
            return False
        else:
            return True
