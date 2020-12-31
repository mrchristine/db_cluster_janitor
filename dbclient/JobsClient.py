from dbclient import *
from cron_descriptor import get_description
import json, datetime


class JobsClient(dbclient):

    def get_jobs_list(self, printJson=False):
        """ Returns an array of json objects for jobs """
        jobs = self.get("/jobs/list", printJson)
        return jobs['jobs']

    def delete_job(self, job_id=None):
        resp = self.post('/jobs/delete', {"job_id": job_id})
        return resp

    def get_job_id(self, name):
        jobs = self.get_jobs_list()
        for i in jobs:
            if i['settings']['name'] == name:
                return i['job_id']
        return None

    def get_jobs_duration(self, run_time=0):
        """ get running jobs list for jobs running over N hours """
        # get current time
        now = datetime.datetime.utcnow()
        run_list = self.get('/jobs/runs/list')['runs']

        running_jobs = filter(lambda x: x['state']['life_cycle_state'] == "RUNNING", run_list)
        # Build a list of long running jobs
        job_list = []
        if running_jobs:
            print("Long running jobs debugging ...")
        for x in running_jobs:
            print(x)
            run_obj = dict()
            run_obj['run_id'] = x['run_id']
            run_obj['start_time'] = x['start_time'] / 1000
            run_obj['creator_user_name'] = x['creator_user_name']
            # If its a spark-submit job, it doesn't contain a job_id parameter. continue with other jobs.
            jid = x.get('job_id', None)
            if jid == None:
                continue
            else:
                run_obj['job_id'] = jid
            # get the run time for the job
            rt = now - datetime.datetime.utcfromtimestamp(run_obj['start_time'])
            hours_run = rt.total_seconds() / 3600
            if (hours_run > run_time):
                # return a list of job runs that we need to stop using the `run_id`
                job_list.append(run_obj)
        return job_list

    def kill_run(self, run_id=None):
        """ stop the job run given the run id of the job """
        if run_id is None:
            raise ("Invalid run_id")
        else:
            resp = self.post('/jobs/runs/cancel', {"run_id": run_id})
            # Grab the run_id from the result
            pprint_j(resp)

    def find_empty_jobs(self):
        jobs = self.get_jobs_list()
        untilted_jobs = filter(lambda x: x['settings']['name'] == "Untitled", jobs)
        empty_jobs = filter(
            lambda x: x['settings'].get('spark_jar_task', None) == x['settings'].get('notebook_task', None), jobs)
        # find the creators of this job to see how often these users create empty jobs
        creators_untitled = map(lambda x: x['creator_user_name'], untilted_jobs)
        creators_empty = map(lambda x: x['creator_user_name'], empty_jobs)
        # convert into a set to remove duplicates from the list
        empty_job_ids = map(lambda x: {'job_id': x['job_id'], 'creator': x['creator_user_name']},
                            empty_jobs + untilted_jobs)
        unique_empty_jobs = [dict(t) for t in set([tuple(d.items()) for d in empty_job_ids])]
        return unique_empty_jobs

    def get_scheduled_jobs(self):
        # Grab job templates
        run_list = self.get('/jobs/list')['jobs']

        # Filter all the jobs that have a schedule defined
        scheduled_jobs = filter(lambda x: x['settings'].has_key('schedule'), run_list)
        jobs_list = []
        for x in scheduled_jobs:
            y = dict()
            y['creator_user_name'] = x['creator_user_name']
            y['job_id'] = x['job_id']
            y['job_name'] = x['settings']['name']
            y['created_time'] = datetime.datetime.fromtimestamp(x['created_time'] / 1000.0).strftime(
                '%Y-%m-%d %H:%M:%S.%f')
            y['schedule'] = get_description(x['settings']['schedule']['quartz_cron_expression'])
            jobs_list.append(y)
        return jobs_list

    def reset_job_schedule(self, job_id=None):
        if job_id is not None:
            resp = self.get('/jobs/get?job_id={0}'.format(job_id))
            print("Job template: ")
            pprint_j(resp)
            # Remove the created_time field
            resp.pop('created_time', None)
            # Pop off the job settings from the results structure
            settings = resp.pop('settings', None)
            # Grab the current schedule
            schedule = settings.pop('schedule', None)
            print("Defined schedule: ")
            print(schedule)
            # Define the new config with the created_time removed
            new_config = resp
            new_config['new_settings'] = settings
            print("Applying new config without schedule: ")
            pprint_j(new_config)

            resp = self.post('/jobs/reset', new_config)
            # Grab the run_id from the result
            pprint_j(resp)
        else:
            print("Invalid job id")

    def get_duplicate_jobs(self):
        job_dups = {}
        jl = self.get_jobs_list()
        for job in jl:
            jname = job['settings']['name']
            jid = job['job_id']
            if job_dups.get(jname, None) is None:
                job_dups[jname] = [jid]
            else:
                job_dups[jname] = sorted(job_dups[jname] + [jid])

        duplicate_jobs = {k: v for k, v in job_dups.items() if len(v) > 1}
        return duplicate_jobs

    def get_delta_pipelines(self):
        pipeline_jobs = self.get('/pipelines').get('statuses', [])
        return pipeline_jobs

    def stop_pipelines(self):
        pipelines = self.get_delta_pipelines()
        terminated = []
        for job in pipelines:
            id = job.get('pipeline_id', '')
            if id:
                endpoint = f'/pipelines/{id}/stop'
                resp = self.post(endpoint)
                print(resp)
                terminated.append(job)
            else:
                print("Id missing from job ...")
                print(job)
        return terminated
