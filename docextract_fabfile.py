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
from fabric.context_managers import shell_env


try:
    import fabric_config_local
except ImportError:
    pass
else:
    fabric_config_local.init_env()


env.hosts = ['inspirehepdev-r.cern.ch']
env.nokeys = True


@task
def run_tests(revision):
    # Prepare repository
    with cd('src/invenio-vm'):
        run('git fetch origin')
        run('git reset --hard %s' % revision)

    # Install invenio
    home = run("echo $HOME")
    invenio_src_dir = '%s/src/invenio-vm' % home
    inspire_src_dir = '%s/src/inspire-vm' % home
    invenio_prefix = '/opt/invenio'
    invenio_user = 'invenio'
    with shell_env(CFG_INVENIO_SRCDIR=invenio_src_dir,
                   CFG_INSPIRE_SRCDIR=inspire_src_dir,
                   CFG_INVENIO_PREFIX=invenio_prefix,
                   CFG_INVENIO_USER=invenio_user):

        # Install
        print("Installing invenio from %s" % invenio_src_dir)
        with cd('src/invenio-vm'):
            run('make -s')
            sudo('make -s install', user=invenio_user)
        print("Installing inspire from %s" % inspire_src_dir)
        with cd('src/inspire-vm'):
            run('make -s')
            sudo('make -s install', user=invenio_user)
        print("Recreating configuration file")
        sudo('%s/bin/inveniocfg --update-config-py' % invenio_prefix, user=invenio_user)

        # Show installed revision to the user
        print("Currently deployed")
        with cd('src/invenio-vm'):
            run('git log HEAD~1..')

        # Run tests
        cmd = "python ~/scripts/test_refextract.py"
        run('dtach -n `mktemp -u /tmp/refextract-tests-%s.XXXX` %s'  % (revision, cmd))
