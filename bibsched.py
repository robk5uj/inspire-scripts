from fabric.api import (run,
                        task,
                        env,
                        settings,
                        local,
                        cd,
                        sudo,
                        lcd,
                        hide,
                        roles,
                        execute)


try:
    import fabric_config_local
except ImportError:
    pass
else:
    fabric_config_local.init_env()

env.host_string = ""
env.roledefs = {
    'prod_main': ['pcudssw1506.cern.ch'],
}

env.roles = ['prod_main']

@task(default=True)
def start_monitor():
    sudo("/opt/cds-invenio/bin/bibsched", user="apache", pty=True, shell=True)
    # local("/opt/invenio/bin/bibsched")