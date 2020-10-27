from dbclient import *

# python 3.6

kill_clusters = True


def is_excluded_cluster(cinfo):
    is_keep_alive = cinfo.get('keep_alive', None)
    if is_keep_alive:
        return True
    return False


def cleanup_clusters(url, token, env_name):
    # cluster object used to document different types of cluster uses
    c_types = ['env_name', 'excluded', 'serverless', 'passthrough',
               'streaming', 'autoterminate', 'init', 'model_endpoints',
               'default', 'whitelist', 'pools']

    report = dict([(key, []) for key in c_types])
    report['env_name'] = (env_name, url)
    # Simple class to list versions and get active cluster list

    print("Starting cleanup cluster process. Fetching cleanup client ....")
    c_client = ClustersClient(token, url)

    # get the clusters that have run more than 4 hours
    print("Get long running clusters ....")
    long_cluster_list = c_client.get_long_clusters(4)

    ## cleanup global init scripts
    print("Get init scripts ....")
    init_scripts = c_client.get_global_init_scripts()

    print("Reset min pool settings ...")
    old_pool_settings = c_client.reset_instance_pool_min()
    report['pools'].append(old_pool_settings)

    print("Terminate model endpoints ...")
    running_model_endpoints = c_client.get_model_endpoints()
    if running_model_endpoints:
        report['model_endpoints'].append(running_model_endpoints)
        model_resp = c_client.terminate_all_model_endpoints(running_model_endpoints)
        print(model_resp)

    hour_min = datetime.datetime.now().strftime("%H_%M")
    print("# Long running clusters\n")

    for c in long_cluster_list:
        # check if cluster is in excluded list defined by the above function
        if is_excluded_cluster(c):
            c['is_excluded'] = True
            report['excluded'].append(c)
            continue

        # kill if its a serverless cluster
        elif c.get('is_serverless', None):
            print("Killing serverless cluster: {0}\t User: {1}".format(c['cluster_name'], c['creator_user_name']))
            report['serverless'].append(c)

        # kill if passthrough cluster / aka single user cluster
        elif c_client.is_passthrough_cluster(c['cluster_id']):
            print("Killing single-user / passthrough cluster: {0}\t User: {1}".format(c['cluster_name'],
                                                                                      c['creator_user_name']))
            report['passthrough'].append(c)

        # kill if a stream is running on the cluster
        elif c_client.is_stream_running(c['cluster_id']):
            print("Killing streaming cluster: {0}\t User: {1}".format(c['cluster_name'], c['creator_user_name']))
            report['streaming'].append(c)

        # kill long running cluster that should have a shorter auto-termination period
        elif c.get('autotermination_minutes', None) == 0:
            print("Clusters should always have auto-terminated enabled. Killing cluster now! {0}\t User: {1}".format(
                c['cluster_name'], c['creator_user_name']))
            report['autoterminate'].append(c)
        else:
            auto_terminate_hours = c.get('autotermination_minutes', None) / 60
            if auto_terminate_hours > 6:
                print("Cluster should have auto-termination enabled to reasonable settings. "
                      "Killing cluster: {0}\t User: {1}".format(c['cluster_name'], c['creator_user_name']))
                report['autoterminate'].append(c)
            else:
                if c['hours_run'] > 24:
                    print("Cluster has been running for over 24 hours. Killing: {0}\t User: {1}".format(
                        c['cluster_name'], c['creator_user_name']))
                    report['autoterminate'].append(c)
                else:
                    print("Cluster has auto-termination enabled. Keeping alive: {0}\t User: {1}".format(
                        c['cluster_name'], c['creator_user_name']))
                    report['default'].append(c)

    if kill_clusters:
        for k, v in report.items():
            # safe list of cluster types that we can skip
            if k in ('excluded', 'init', 'env_name', 'model_endpoints',
                     'default', 'whitelist', 'pools'):
                continue
            else:
                # terminate these clusters that fall outside the above categories
                for cluster in v:
                    c_client.kill_cluster(cluster['cluster_id'])

    print("# Global init scripts\n")
    for path in init_scripts:
        report['init'].append(path)
        if "enable_delta" not in path['path']:
            c_client.delete_init_script(path['path'])
    return report


def lambda_handler(event, context):
    # get a list of configurations in json format
    configs = get_job_configs()
    envs = {}
    log_bucket = []
    for env in configs:
        envs[env['desc']] = (env['url'], env['token'])
        log_bucket.append(env['s3_bucket'])

    # logging configuration
    bucket_name = log_bucket[0]
    table_name = "cluster_usage_logs"

    full_report = ""
    html_report = ""
    for x, y in envs.items():
        print("Env name to cleanup clusters: {0}".format(y[0]))
        cluster_report = cleanup_clusters(y[0], y[1], x)
        # log report to S3
        log_to_s3(bucket_name, table_name, cluster_report)
        full_report += pprint_j(cluster_report)
        full_report += "\n######################################################\n"
        html_report += get_html(cluster_report)

    print(full_report)
    email_list = ["mwc@databricks.com"]
    send_email("Databricks Automated Cluster Usage Report", email_list, full_report, html_report)
    # Print Spark Versions
    message = "Completed running cleanup across all field environments!"
    return {
        'message': message
    }

