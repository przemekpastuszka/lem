from fabric.operations import put, run, local
from fabric.context_managers import cd
from fabric.api import execute

class Instance:
    pass

def get_running_instances(properties_file):
    result = local("whirr list-cluster --config={0} --quiet".format(properties_file), capture=True)
    instances = []
    for line in result.split('\n'):
	    instance_info = line.split('\t')
	    instance = Instance()
	    instance.identity = instance_info[0]
	    instance.ami = instance_info[1]
	    instance.external_ip = instance_info[2]
	    instance.internal_ip = instance_info[3]
	    instance.state = instance_info[4]
	    instance.zone = instance_info[5]
	    instance.roles = instance_info[6]
	    instances.append(instance)
    return instances
    
def run_on_cluster(jar, class_name, properties_file):
    instances = get_running_instances(properties_file)
    masters = [x for x in instances if 'jobtracker' in x.roles]
    execute(deploy_and_run, jar, class_name, hosts = [masters[0].external_ip])

def deploy_and_run(jar, class_name):
    with cd('/tmp'):
        remote_path = put(jar, 'hadoop_jar')[0]
        run("hadoop jar {0} {1}".format(remote_path, class_name))
