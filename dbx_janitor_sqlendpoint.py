#%%

from dbclient import *

print(dir())
# python 3.6

kill_endpoints = False
stop_endpoints = True

def cleanup_sqlendpoints(url, token, env_name):

    sqlClient = SQLAnalyticsClient(token, url)
    # Simple class to list versions and get active cluster list

    # get the list of sql endpoints
    terminate_list = sqlClient.get_terminate_sqlendpoints()
    for endpoint in terminate_list:
        if endpoint['keep_until']=="Expired":
            sqlClient.kill_cluster(endpoint['cluster_id'])
        else:
            sqlClient.stop_cluster(endpoint['cluster_id'])
        


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

#testing
#configs = get_job_configs()
#envs=()
#for env in configs:
#    envs = (env['url'], env['token'])
#    cleanup_sqlendpoints(env['url'], env['token'], env['desc'])