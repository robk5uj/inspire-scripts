import time
import re
import traceback
import os
import sys

from functools import wraps
from tempfile import mkstemp
from itertools import chain

import fabric.state
from fabric.api import run, \
                       task as fab_task, \
                       env, \
                       settings, \
                       local, \
                       cd, \
                       sudo, \
                       lcd, \
                       hide, \
                       roles, \
                       execute
from fabric.operations import prompt
from fabric.contrib.files import exists

from invenio.mailutils import send_email
from invenio.config import CFG_SITE_ADMIN_EMAIL


CFG_LINES_TO_IGNORE = ("#", )
CFG_CMDDIR = os.environ.get('TMPDIR', '/tmp')
CFG_FROM_EMAIL = CFG_SITE_ADMIN_EMAIL
CFG_LOG_EMAIL = "admin@inspirehep.net"
CFG_INVENIO_DEPLOY_RECIPE = "/afs/cern.ch/project/inspire/repo/invenio-create-deploy-recipe"
CFG_DEFAULT_RECIPE_ARGS = " --inspire --use-source --no-pull --via-filecopy"

if os.environ.get('EDITOR'):
    CFG_EDITOR = os.environ.get('EDITOR')
elif os.environ.get('VISUAL'):
    CFG_EDITOR = os.environ.get('VISUAL')
else:
    print("ERROR: NO EDITOR/VISUAL variable found. Exiting.")
    sys.exit(1)

env.roledefs = {
    'dev': ['pccis84.cern.ch'],
    'test': ['pcudssw1505.cern.ch'],
    'prod_main': ['pcudssw1506.cern.ch'],
    'prod_aux': ['pcudssw1507.cern.ch',
                 'pcudssx1506.cern.ch',
                 'pcudssw1504.cern.ch'],
    'proxy': ['pcudssw1503'],
    'prod1': ['pcudssw1506.cern.ch'],
    'prod2': ['pcudssw1507.cern.ch'],
    'prod3': ['pcudssx1506.cern.ch'],
    'prod4': ['pcudssw1504.cern.ch'],
}

dev_backends = [
                "inspiredev",
                "inspiredev-ssl",
               ]

test_backends = [
                "inspiretest",
                "inspiretest-ssl",
                ]

prod_backends = [
                "inspireprod_app",
                "inspireprod-ssl",
                "inspireprod_static",
                "inspireprod_rss",
                "inspireprod_author",
                "inspireprod_robot",
                ]

env.proxybackends = {
    'dev': ['pccis84', dev_backends],
    'test': ['pcudssw1505', test_backends],
    'prod1': ['pcudssw1506', prod_backends],
    'prod2': ['pcudssw1507', prod_backends],
    'prod3': ['pcudssx1506', prod_backends],
    'prod4': ['pcudssw1504', prod_backends],
}

env.branch = ""
env.fetch = None
env.repodir = ""
env.dolog = True
env.roles_aux = []
env.mi_roles = []

def task(f):
    @wraps(f)
    def fun(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception, e:
            if fabric.state.output.debug:
                traceback.print_exc()
            else:
                sys.stderr.write("Fatal error: %s\n" % str(e))
            sys.exit(1)

    return fab_task(fun)


@task
def origin():
    """
    Activate origin fetching-
    """
    env.fetch = "origin"


@task
def localhost():
    global run, cd, exists, sudo

    def sudo(cmd, user=None, shell=False):
        if user:
            user_str = '-u %s ' % user
        else:
            user_str = ''
        if shell:
            cmd = 'bash -c "%s"'
        return run('sudo %s%s' % (user_str, cmd))

    def run(cmd, shell=True, warn_only=False):
        if shell:
            shell = 'sh'
        else:
            shell = None

        old_warn_only = fabric.state.env.warn_only
        if warn_only:
            fabric.state.env.warn_only = warn_only

        try:
            r = local(cmd, capture=True, shell=shell)
        finally:
            fabric.state.env.warn_only = old_warn_only

        print r
        return r

    def cd(*args, **kwargs):
        return lcd(*args, **kwargs)

    def exists(path):
        with settings(hide('everything'), warn_only=True):
            return run('test -e "$(echo %s)"' % path, warn_only=True).succeeded


@task
def dev():
    """
    Activate configuration for INSPIRE DEV server.
    """
    env.roles = ['dev']
    env.dolog = False
    env.branch = "dev"


@task
def test():
    """
    Activate configuration for INSPIRE TEST server.
    """
    env.roles = ['test']
    env.dolog = False
    env.branch = "test"


@task
def prod():
    """
    Activate configuration for INSPIRE PROD main server.
    """
    env.roles = ['prod_main']
    env.roles_aux = ['prod_aux']
    env.dolog = True
    env.branch = "prod"
    env.mi_roles = ['prod1', 'prod2', 'prod3', 'prod4']


@task
def proxy():
    env.hosts = env.roledefs['proxy']


@task
def prod1():
    """
    Activate configuration for INSPIRE PROD 1.
    """
    env.roles += ['prod1']
    env.mi_roles += ['prod1']
    env.dolog = True
    env.branch = "prod"


@task
def prod2():
    """
    Activate configuration for INSPIRE PROD 2.
    """
    env.roles += ['prod2']
    env.mi_roles += ['prod2']
    env.dolog = True
    env.branch = "prod"


@task
def prod3():
    """
    Activate configuration for INSPIRE PROD 3.
    """
    env.roles += ['prod3']
    env.mi_roles += ['prod3']
    env.dolog = True
    env.branch = "prod"


@task
def prod4():
    """
    Activate configuration for INSPIRE PROD 4.
    """
    env.roles += ['prod4']
    env.mi_roles += ['prod4']
    env.dolog = True
    env.branch = "prod"


@task
def ops():
    """
    Activate configuration for INSPIRE PROD aux servers.
    """
    env.repodir = run("echo $CFG_INVENIO_SRCDIR")


@task
def inspire():
    """
    Activate configuration for INSPIRE PROD aux servers.
    """
    env.repodir = run("echo $CFG_INSPIRE_SRCDIR")


@task
def repo(repo):
    """
    Pull changes into checked out branch
    """
    env.fetch = repo


# MAIN TASKS

@task
def safe_makeinstall(opsbranch=None, inspirebranch="master",
                                                          reload_apache="yes"):
    # Remove roles for makeinstall to not run all the hosts at once.
    env.roles = []
    env.roles_aux = []
    env.dolog = False
    needs_autoconf = True
    print 'targets', env.mi_roles
    for target in env.mi_roles:
        if target == env.mi_roles[-1]:
            env.dolog = True
        execute(disable, target)
        with settings(roles=[target]):
            env.roles = [target]
            execute(makeinstall, opsbranch=opsbranch,
                                 inspirebranch=inspirebranch,
                                 reload_apache=reload_apache,
                                 needs_autoconf=needs_autoconf)
        execute(enable, target)
        needs_autoconf = False
        # FIXME for logs later on:
        # env.dolog = True



@task
def mi(opsbranch=None, inspirebranch="master", reload_apache="yes"):
    makeinstall(opsbranch, inspirebranch, reload_apache)


@task
def install_jquery_plugins():
    invenio_srcdir = run("echo $CFG_INVENIO_SRCDIR")
    apacheuser = run("echo $CFG_INVENIO_USER")

    choice = prompt("Install jquery-plugins? (y/N)", default="no")
    if choice.lower() in ["y", "ye", "yes"]:
        cmd = "sudo -u %s make -s install-jquery-plugins" % (apacheuser,)
        _run_command(invenio_srcdir, cmd)


@task
def autoconf():
    invenio_srcdir = run("echo $CFG_INVENIO_SRCDIR")
    prefixdir = run("echo $CFG_INVENIO_PREFIX")
    config_cmd = "aclocal && automake -a && autoconf && ./configure prefix=%s" % (prefixdir,)
    with cd(invenio_srcdir):
        run(config_cmd)

@task
def makeinstall(opsbranch=None, inspirebranch="master", reload_apache="yes", needs_autoconf=True):
    """
    This task implement this recipe which re-installs the server.

    On every individual worker node:

    #+BEGIN_SRC sh
    sudo -u %(apache)s /usr/bin/id
    cd %(repodir1)s
    git pull
    make -s
    sudo -u %(apache)s make install
    sudo -u %(apache)s %(prefixdir)s/bin/inveniocfg --update-all
    sudo %(prefixdir)s/bin/inveniocfg --update-dbexec
    cd %(repodir2)s
    git pull
    sudo -u %(apache)s make install
    #+END_SRC

    On DEV, uglify interface:

    #+BEGIN_SRC sh
    sudo -u %(apache)s make reset-ugly-ui
    #+END_SRC

    On TEST, uglify interface like this:

    #+BEGIN_SRC sh
    sudo -u %(apache)s make reset-ugly-ui
    sudo -u %(apache)s cp webstyle/inspire_logo_beta_ugly_test.png \
         %(prefixdir)s/var/www/img/inspire_logo_beta.png
    #+END_SRC

    Now restart Apache:

    #+BEGIN_SRC sh
    sudo /etc/init.d/httpd restart
    #+END_SRC

    Note that on PROD we have higher safety for =dbexec= which is to be
    reset now:

    #+BEGIN_SRC sh
    sudo %(prefixdir)s/bin/inveniocfg --update-dbexec
    sudo chmod go-rxw %(prefixdir)s/bin/dbexec*
    sudo chown root.root %(prefixdir)s/bin/dbexec*
    ls -l %(prefixdir)s/bin/dbexec*
    #+END_SRC

    Also on PROD the bibsched rights on the second worker node should be
    revoked:

    #+BEGIN_SRC sh
    sudo chmod a-rwx %(prefixdir)s/bin/bibsched
    ls -l %(prefixdir)s/bin/bibsched
    #+END_SRC
    """
    if opsbranch is None:
        opsbranch = env.branch

    invenio_srcdir = run("echo $CFG_INVENIO_SRCDIR")
    inspire_srcdir = run("echo $CFG_INSPIRE_SRCDIR")

    choice = prompt("Do you want to fetch new branches? (Y/n)", default="yes")
    if choice.lower() in ["y", "ye", "yes"]:
        # Prepare branches in the two repos
        ready_branch(opsbranch, invenio_srcdir)
        ready_branch(inspirebranch, inspire_srcdir)

    if needs_autoconf:
        choice = prompt("Do you want to run autoconf? (Y/n)", default="yes")
        if choice.lower() in ["y", "ye", "yes"]:
            autoconf()

    prefixdir = run("echo $CFG_INVENIO_PREFIX")
    apacheuser = run("echo $CFG_INVENIO_USER")

    recipe_text = """
    #+BEGIN_SRC sh
    sudo -u %(apache)s /usr/bin/id
    cd %(opsdir)s
    make -s
    sudo -u %(apache)s make -s install
    """ % {'apache': apacheuser,
           'opsdir': invenio_srcdir}

    recipe_text += """
    cd %(inspiredir)s
    sudo -u %(apache)s make -s install
    """ % {'apache': apacheuser,
           'opsdir': invenio_srcdir,
           'prefixdir': prefixdir,
           'inspiredir': inspire_srcdir}

    if 'dev' in env.roles:
        recipe_text += "sudo -u %s make reset-ugly-ui\n" % (apacheuser,)

    if 'test' in env.roles:
        recipe_text += "sudo -u %s make reset-test-ui\n" % (apacheuser,)

    # Here we see if any of the current hosts are production machines, if so - special rules apply
    is_production_machine = bool([True for role in env.roles \
                                  if role.startswith('prod')])
    if is_production_machine:
        recipe_text += """
        sudo -u %(apache)s %(prefixdir)s/bin/inveniocfg --update-config-py --update-dbquery-py
        sudo %(prefixdir)s/bin/inveniocfg --update-dbexec
        sudo chmod go-rxw %(prefixdir)s/bin/dbexec*
        sudo chown root.root %(prefixdir)s/bin/dbexec*
        ls -l %(prefixdir)s/bin/dbexec*
        """ % {'apache': apacheuser,
               'prefixdir': prefixdir}
        if env.hosts == env.roledefs['prod_aux']:
            recipe_text += """
            sudo chmod a-rwx %(prefixdir)s/bin/bibsched
            ls -l %(prefixdir)s/bin/bibsched
            """ % {'prefixdir': prefixdir}
    else:
        recipe_text += "sudo -u %s %s/bin/inveniocfg --update-all\n" % (apacheuser, prefixdir)

    if reload_apache == "yes":
        recipe_text += "sudo /etc/init.d/httpd reload\n"

    recipe_text += "#+END_SRC"
    print

    recipe_text = ready_command_file(recipe_text)
    print '#+BEGIN_SRC sh'
    print recipe_text
    print '#+END_SRC'
    cmd_filename = save_command_file(recipe_text)
    if not cmd_filename:
        print("ERROR: No command file")
        sys.exit(1)

    hosts_touched = env.hosts
    executed_commands = perform_deploy(cmd_filename, invenio_srcdir)

    install_jquery_plugins()

    # Run commands (allowing user to edit them beforehand)
    # Users can also run the commands on other hosts right away
    for host in chain.from_iterable(env.roledefs[role] for role in env.roles_aux):
        choice = prompt("Press enter to run these commands on %s" % host)
        # For every host in defined role, perform deploy
        with settings(host_string=host):
            hosts_touched.append(host)
            executed = perform_deploy(cmd_filename, invenio_srcdir)
            # FIXME - we want log per node! This is "un peu retard"
            if not executed_commands:
                executed_commands = executed
            install_jquery_plugins()

    # Logging?
    if env.dolog:
        choice = prompt("Log this deploy to %s? (Y/n)" % (CFG_LOG_EMAIL,), default="yes")
        if choice.lower() in ["y", "ye", "yes"]:
            log_text = """
Upgraded %(hosts)s to latest git master sources (using make).

First, wait for bibsched jobs to stop and put the queue to manual mode
(on the first worker node).  Then run upgrade in the following way:
            """ % {"hosts": ", ".join(hosts_touched)}

            log_filename = _safe_mkstemp()
            log_deploy(log_filename, executed_commands, log_text, CFG_LOG_EMAIL)


@task
def deploy(branch=None, commitid=None, recipeargs=CFG_DEFAULT_RECIPE_ARGS,
                                                                 repodir=None):
    """
    Do a deployment in given repository using any commitid
    and recipe arguments given.
    """
    if branch is None:
        branch = env.branch

    if repodir is None:
        repodir = env.repodir

    if not repodir:
        print("Error: No repodir")
        sys.exit(1)

    # Prepare remote version of the given branch for deployment
    ready_branch(branch=branch, repodir=repodir)

    # Prepare list of commands to run
    out = _get_recipe(repodir, recipeargs, commitid)
    cmd_filename = save_command_file(ready_command_file(out))

    if not cmd_filename:
        print("ERROR: No command file")
        sys.exit(1)

    executed_commands = perform_deploy(cmd_filename, repodir)

    # Run commands (allowing user to edit them beforehand)
    # Users can also run the commands on other hosts right away
    for host in chain.from_iterable(env.roledefs[role] for role in env.roles_aux):
        choice = prompt("Press enter to run these commands on %s" % host)
        # For every host in defined role, perform deploy
        with settings(host_string=host):
            perform_deploy(cmd_filename, repodir)

    # Logging?
    if env.dolog:
        choice = prompt("Log this deploy to %s? (Y/n)" % (CFG_LOG_EMAIL,), default="yes")
        if choice.lower() in ["y", "ye", "yes"]:
            log_text = out.split("#+END_EXAMPLE")[0]
            log_filename = _safe_mkstemp()
            log_deploy(log_filename, executed_commands, log_text, CFG_LOG_EMAIL)


@task
def perform_deploy(cmd_filename, repodir=None):
    """
    Given a path to a file with commands, this function will run
    each command on the remote host in the given directory, line by line.

    Returns a list of executed commands.
    """
    if repodir is None:
        repodir = env.repodir
    if not repodir:
        raise Exception("Error: No repodir")

    choice = prompt("Edit the commands to be executed (between BEGIN_SRC and END_SRC)? (y/N)", default="no")
    if choice.lower() in ["y", "ye", "yes"]:
        local("%s %s" % (CFG_EDITOR, cmd_filename))

    print "--- COMMANDS TO RUN ---"
    with open(cmd_filename) as filecontent:
        print filecontent.read()
    print "--- END OF COMMANDS ---"
    choice = prompt("Run these commands on %s? (Y/n)" % (env.host_string, ), default="yes")
    if choice.lower() not in ["y", "ye", "yes"]:
        return []

    current_directory = repodir
    executed_commands = []
    with open(cmd_filename) as commands:
        for command in commands:
            command = command.strip()
            if command.startswith('cd '):
                current_directory = command[3:]
            else:
                _run_command(current_directory, command)
            executed_commands.append(command)
    return executed_commands


@task
def check_branch(base_branch, repodir=None):
    """
    Run a kwalitee check of the files to be deployed. May be run locally.
    """
    if repodir is None:
        repodir = env.repodir
    if not repodir:
        raise Exception("No repodir")

    with cd(repodir):
        files_to_check = run("git log HEAD..%s --pretty=format: --name-only | grep '\.py'" %
                                  base_branch)
        for filepath in files_to_check.split('\n'):
            if exists(filepath):
                run("python modules/miscutil/lib/kwalitee.py --check-all %s" %
                        (filepath, ), warn_only=True)


@task
def host_type():
    """
    Check host type of remote hosts. Used for tests.
    """
    run('uname -s')


@task
def reload_apache():
    run("sudo /etc/init.d/httpd graceful")


@task
def ready_branch(branch=None, repodir=None, repo=None):
    """
    Connect to hosts and checkout given branch in given
    repository.
    """
    if branch is None:
        branch = env.branch
    if repodir is None:
        repodir = env.repodir
    if repo is None:
        repo = env.fetch

    with cd(repodir):
        if repo:
            run("git fetch %s" % repo)
            branch = "%s/%s" % (repo, branch)
        run("git reset --hard %s" % branch)


@roles(['proxy'])
@task
def disable(server=None):
    """
    Disable a server in the haproxy configuration. Use with proxy.
    """
    if not server:
        print("No server defined")
        return
    backends = env.proxybackends
    if not backends or server not in backends:
        print("No backends defined")
        return

    servername, backends = backends[server]

    choice = prompt("Disable the following server? %s (Y/n)" % (servername, ), default="yes")
    if choice.lower() not in ["y", "ye", "yes"]:
        return
    proxy_action(servername, backends, action="disable")


@roles(['proxy'])
@task
def enable(server=None):
    """
    Enable a server in the haproxy configuration. Use with proxy.
    """
    if not server:
        print("No server defined")
        return
    backends = env.proxybackends
    if not backends or server not in backends:
        print("No backends defined")
        return

    servername, backends = backends[server]

    choice = prompt("Enable the following server? %s (Y/n)" % (servername, ), default="yes")
    if choice.lower() not in ["y", "ye", "yes"]:
        return
    proxy_action(servername, backends, action="enable")


@task
def unit():
    """
    Run unit-tests on selected server.
    """
    prefixdir = run("echo $CFG_INVENIO_PREFIX")
    apacheuser = run("echo $CFG_INVENIO_USER")
    sudo("%(prefix)s/bin/inveniocfg --run-unit-tests" % {
            'apache': apacheuser,
            'prefix': prefixdir,
        }, user=apacheuser)


pull_changes = ready_branch


def proxy_action(server, backends, action="enable"):
    for backend in backends:
        if 'ssl' in backend:
            # special ssl suffix
            current_server_suffix = '-ssl'
        else:
            current_server_suffix = ''
        current_server = server + current_server_suffix
        cmd = 'echo "%s server %s/%s" | sudo nc -U /var/lib/haproxy/stats' \
               % (action, backend, current_server)
        sudo(cmd)


def ready_command_file(out):
    if not "#+BEGIN_SRC sh" in out:
        print("Error, no commands in output")
        return

    # Get everything between SRC
    src = "".join(re.findall("BEGIN_SRC sh\r?\n(.*)#\+END_SRC", str(out), re.S))
    # Take out stuff we don't want from the command list
    cleaned_src = "\n".join([line.strip() for line in src.split("\n") \
                 if line.strip() and not line.startswith(CFG_LINES_TO_IGNORE)])

    return cleaned_src


def save_command_file(out):
    """
    Will prepare al list of commands, line by line, in a file - based
    on given recipe-text.
    """
    try:
        cmd_filename = _safe_mkstemp()
        # Write default commands
        command_file = open(cmd_filename, 'w')
        command_file.write(out)
        command_file.close()
    except:
        if os.path.exists(cmd_filename):
            os.remove(cmd_filename)
        raise
    return cmd_filename


def log_deploy(log_filename, executed_commands, log, log_mail):
    """
    Perform logging of deployment using executed commands. Sends a mail
    to given mailaddress with deploy log.
    """
    # Write default logs
    log_file = open(log_filename, 'w')
    log_file.write("%s\n#+END_EXAMPLE\n\n#+BEGIN_SRC sh\n%s\n#+END_SRC\n" \
                                         % (log, "\n".join(executed_commands)))
    log_file.close()

    # Open log for edit
    local("%s %s" % (CFG_EDITOR, log_filename))

    with open(log_filename) as logs:
        full_log = logs.readlines()
        subject = full_log[0]
        content = "".join(full_log[1:])
        if send_email(fromaddr=CFG_FROM_EMAIL, \
                      toaddr=log_mail, \
                      subject=subject, \
                      content=content, \
                      header="", \
                      footer=""):
            print "Email sent to %s" % (log_mail,)
        else:
            print "ERROR: Email not sent"
            print subject
            print content

# HELPER FUNCTIONS


def _run_command(directory, command):
    """
    Runs a given command in the directory given.

    If the command begins with a diff-like program, a prompt
    is given in order to allow for user inspection.
    """
    if command and not command.startswith(CFG_LINES_TO_IGNORE):
        match = re.match("sudo -u ([a-z]+) (.*)", command)
        if match:
            with cd(directory):
                sudo(match.group(2), user=match.group(1))
        elif command.startswith('sudo'):
            sudo(command[5:], shell=False)
        elif command.startswith('cd'):
            # ignore cause cd doesn't work with run
            # but we should already be wrapped
            # with a "with cd()" context manager
            pass
        else:
            if command.startswith(('colordiff', 'diff')):
                with cd(directory):
                    with hide('warnings'):
                        run(command, warn_only=True)
                prompt("Press Enter to continue..")
            else:
                with cd(directory):
                    run(command)


def _get_recipe(repodir, recipeargs, commitid=None):
    """
    Fetches the output from the recipe generated by invenio-devscript
    invenio-create-deploy-recipe.
    """
    if commitid:
        commitid_arg = " %s" % commitid
    else:
        commitid_arg = ""
    return run('CFG_INVENIO_SRCDIR=%s %s%s%s' % \
              (repodir, CFG_INVENIO_DEPLOY_RECIPE, recipeargs, commitid_arg))


def _safe_mkstemp():
    """
    Create a tempfile in CFG_CMDDIR.
    """
    current_time = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())
    fd_commands, filename_commands = mkstemp(prefix="fab_commands_%s" % (current_time,), \
                                             dir=CFG_CMDDIR)
    os.close(fd_commands)
    return filename_commands
