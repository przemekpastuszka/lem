from fabric.operations import put, run, local
from fabric.context_managers import cd, prefix
from fabric.api import execute
import os


def deploy_and_run(jar, class_name, classpath):
    """Deploy and run hadoop job

    Arguments:
    jar - jar with the job's class
    class_name - class to run
    classpath - semicolon-delimited list of jars, that should be used as hadoop's classpath
    """
    with cd('/tmp'):
        run('mkdir -p libs')
        put_if_absent(classpath.split(';'), 'libs')

        remote_jar = put(jar, 'hadoop_jar')[0]
        hadoop_classpath = ':'.join(
            ['/tmp/libs/' + os.path.basename(path) for path in classpath.split(';')])
        with prefix("export HADOOP_CLASSPATH=" + hadoop_classpath):
            run("hadoop jar {0} {1}".format(remote_jar, class_name))


def put_if_absent(local_paths, remote_dir):
    """Sends artifacts to remote location only if they don't already exist there

    Arguments:
    local_paths - python list of paths that should be uploaded
    remote_dir
    """
    local_artifacts = {
        os.path.basename(local_path): local_path for local_path in local_paths}
    remote_artifacts = list_dir(remote_dir)
    absent_artifacts = set(local_artifacts.keys()) - set(remote_artifacts)
    for absent_artifact in absent_artifacts:
        put(local_artifacts[absent_artifact], remote_dir)


def list_dir(path):
    """Return content of remote path"""
    output = run('ls ' + path)
    return output.split()


def get_master_ip(properties_file):
    """Given properties file find external ip of cluster master"""
    instances = get_instances(properties_file)
    masters = [x for x in instances if 'jobtracker' in x.roles]
    return masters[0].external_ip


def get_instances(properties_file):
    """Returns list of all instances in the cluster specified by properties_file"""
    result = local(
        "whirr list-cluster --config={0} --quiet".format(properties_file),
        capture=True)
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


class Instance:
    pass
