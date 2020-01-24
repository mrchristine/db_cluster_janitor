## Databricks DBU Alerts 

This is a lambda package to auto-terminate on DBU usage for long running jobs that could be caused by streaming demos, or serverless clusters..   

Goal: Run an hourly check for DBU usage across the platform and send an e-mail alert on high usage workloads.   

Lambda Requirements:
* Load all dependencies into a zip file
* Load the code base into the same zip file

This package requires configurations to access the environments via tokens. 
The configurations are placed in a `config/job.conf` directory in json format. One record per line. 
```
{"url": "https://mydatabricksenv.cloud.databricks.com", "token": "mysupercooltoken", "desc" : "Dev Env", "s3_bucket": "my-s3-logs"}
```

**Build**:
The `rebuild.sh` script will build the artifacts for the lambda package. Upload the ZIP to Lambda and make sure the proper permissions are there. 

Note:
* Email alerts are sent
* There is a bug in the `create_timestamp` that I'm waiting for eng to fix :( Runtimes are wildly off because we don't track last start times properly and use create time. This problem comes up for clusters that are restarted. 
