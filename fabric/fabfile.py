from fabric.operations import put, run
from fabric.context_managers import cd


def deploy_and_run(jar, class_name):
    with cd('/tmp'):
        remote_path = put(jar, 'hadoop_jar')[0]
        run("hadoop jar {0} {1}".format(remote_path, class_name))
