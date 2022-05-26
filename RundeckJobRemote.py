from pyrundeck import Rundeck

def callRemoteRundeckJob(jobName):
    rundeck = Rundeck('http://aosqa-testocc.cloud.operative.com:4440',
                      token='mIlUYMyxwn6ccqNEix691fGUL245GqVx');
    rundeck.run_job_by_name(name=jobName);


#callRemoteRundeckJob("PlannerStream_changeToNormalFlag")
