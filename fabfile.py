import time
import re
import traceback
import os
import sys
import urllib2

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

try:
    import fabric_config_local
except ImportError:
    pass
else:
    fabric_config_local.init_env()

CFG_LINES_TO_IGNORE = ("#", )
CFG_CMDDIR = os.environ.get('TMPDIR', '/tmp')
CFG_FROM_EMAIL = CFG_SITE_ADMIN_EMAIL
CFG_LOG_EMAIL = "admin@inspirehep.net"
CFG_INVENIO_DEPLOY_RECIPE = "/afs/cern.ch/project/inspire/repo/invenio-create-deploy-recipe"
FABRIC_DEPLOYMENT_LOCK_SCRIPT_PATH = "/afs/cern.ch/project/inspire/repo/fabric_deployment_check_lock.py"
FABRIC_DEPLOYMENT_LOCK_PATH = "/afs/cern.ch/project/inspire/repo/fabric_deployment.lock"
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


env.graceful_reload = True
env.branch = ""
env.fetch = None
env.reset = True
env.repodir = ""
env.dolog = True
env.roles_aux = []
env.roles_aux = []


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


def with_lock(f):
    @wraps(f)
    def fun(*args, **kwargs):
        if env.roles == ["test"] or env.roles == ["dev"]:
            return f(*args, **kwargs)
        with hide("output"):
            error_code = run("python %s %s" % (FABRIC_DEPLOYMENT_LOCK_SCRIPT_PATH, FABRIC_DEPLOYMENT_LOCK_PATH))
        if int(error_code):
            sys.stderr.write("Error: another user is performing a deployment.\n")
            choice = prompt("Are you sure you want to continue? [NOT recommended] (y/N)", default="no")
            if choice.lower() not in ["y", "ye", "yes"]:
                sys.exit(1)
        try:
            return f(*args, **kwargs)
        finally:
            run("rm -f %s" % FABRIC_DEPLOYMENT_LOCK_PATH)
    return fun


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
    env.roles_aux = ['dev']
    env.dolog = False
    env.branch = "dev"


@task
def test():
    """
    Activate configuration for INSPIRE TEST server.
    """
    env.roles = ['test']
    env.roles_aux = ['test']
    env.dolog = False
    env.branch = "test"


@task
def prod():
    """
    Activate configuration for INSPIRE PROD main server.
    """
    env.roles = ['prod_main']
    env.roles_aux = ['prod1', 'prod2', 'prod3', 'prod4']
    env.dolog = True
    env.branch = "prod"


@task
def proxy():
    env.hosts = env.roledefs['proxy']


@task
def prod1():
    """
    Activate configuration for INSPIRE PROD 1.
    """
    env.roles += ['prod1']
    env.roles_aux += ['prod1']
    env.dolog = True
    env.branch = "prod"


@task
def prod2():
    """
    Activate configuration for INSPIRE PROD 2.
    """
    env.roles += ['prod2']
    env.roles_aux += ['prod2']
    env.dolog = True
    env.branch = "prod"


@task
def prod3():
    """
    Activate configuration for INSPIRE PROD 3.
    """
    env.roles += ['prod3']
    env.roles_aux += ['prod3']
    env.dolog = True
    env.branch = "prod"


@task
def prod4():
    """
    Activate configuration for INSPIRE PROD 4.
    """
    env.roles += ['prod4']
    env.roles_aux += ['prod4']
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


@task
def graceful():
    """
    Be graceful when reloading apache by taking the node out of rotation and
    by performing a request to load workers.
    """
    env.graceful_reload = True


@task
def nograceful():
    """
    Be graceful when reloading apache by taking the node out of rotation and
    by performing a request to load workers.
    """
    env.graceful_reload = False


@task
def noreset():
    """
    Enabling this flag will not run 'git reset --hard' given branch. Use with care!
    """
    env.reset = False


# MAIN TASKS


@task
def safe_makeinstall(opsbranch=None, inspirebranch="master",
                     reload_apache="yes"):
    makeinstall(opsbranch, inspirebranch, reload_apache, True)


@task
def mi(opsbranch=None, inspirebranch="master", reload_apache="yes",
       safe=False):
    makeinstall(opsbranch, inspirebranch, reload_apache, safe)


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
def stop_bibsched():
    choice = prompt("Stop bibsched? (Y/n)", default="yes")
    if choice.lower() in ["y", "ye", "yes"]:
        prefixdir = run("echo $CFG_INVENIO_PREFIX")
        apacheuser = run("echo $CFG_INVENIO_USER")
        sudo("%s/bin/bibsched stop" % (prefixdir,), user=apacheuser)


@task
def makeinstall(opsbranch=None, inspirebranch="master", reload_apache="yes",
                safe=False):
    """
    This task implement the recipe to re-install the server. Use the safe flag
    to disable bibsched on the node and to safely disable the node in haproxy
    before installing.
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

    choice = prompt("Do you want to run autoconf? (Y/n)", default="yes")
    if choice.lower() in ["y", "ye", "yes"]:
        autoconf()

    prefixdir = run("echo $CFG_INVENIO_PREFIX")
    apacheuser = run("echo $CFG_INVENIO_USER")

    recipe_text = """
    #+BEGIN_SRC sh
    sudo -u %(apache)s /usr/bin/id
    cd %(opsdir)s
    make -s clean
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
    recipe_text = ready_command_file(recipe_text)
    cmd_filename = save_command_file(recipe_text)
    if not cmd_filename:
        print("ERROR: No command file")
        sys.exit(1)

    hosts_touched = []
    executed_commands = {}

    # Run commands (allowing user to edit them beforehand)
    # Users can also run the commands on other hosts right away
    for host in chain.from_iterable(env.roledefs[role] for role in env.roles_aux):
        # For every host in defined role, perform deploy
        with settings(host_string=host):
            stop_bibsched()
            execute(disable, host)
            res = execute(perform_deploy, cmd_filename, invenio_srcdir)
            executed_commands.update(res)
            install_jquery_plugins()
            hosts_touched.append(host)
            ping_host(host)
            execute(enable, host)

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
            print("Log sent")
    _print_end_message()


@task
@with_lock
def deploy(branch=None, commitid=None,
           recipeargs=CFG_DEFAULT_RECIPE_ARGS, repodir=None):
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
    if env.reset:
        ready_branch(branch=branch, repodir=repodir)
    else:
        print("Warning! Skipping reset")

    # Prepare list of commands to run
    out = _get_recipe(repodir, recipeargs, commitid)
    cmd_filename = save_command_file(ready_command_file(out))

    if not cmd_filename:
        print("ERROR: No command file")
        sys.exit(1)

    # Run commands (allowing user to edit them beforehand)
    # Users can also run the commands on other hosts right away
    executed_commands = {}
    for role in env.roles_aux:
        choice = prompt("Press enter to prepare commands for %s" % role)
        # For every host in defined role, perform deploy
        with settings(roles=[role]):
            res = execute(perform_deploy, cmd_filename, repodir)
            executed_commands.update(res)

    # Logging?
    if env.dolog:
        choice = prompt("Log this deploy to %s? (Y/n)" % (CFG_LOG_EMAIL,), default="yes")
        if choice.lower() in ["y", "ye", "yes"]:
            log_text = out.split("#+END_EXAMPLE")[0]
            log_filename = _safe_mkstemp()
            log_deploy(log_filename, executed_commands, log_text, CFG_LOG_EMAIL)
            print("Log sent!")
    _print_end_message()


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

    done_editing = False
    print_command_file(cmd_filename)

    while not done_editing:
        choice = prompt("Edit the commands to be executed (between BEGIN_SRC and END_SRC)? (y/N)", default="no")
        if choice.lower() in ["y", "ye", "yes"]:
            local("%s %s" % (CFG_EDITOR, cmd_filename))
            print_command_file(cmd_filename)
        else:
            done_editing = True

    choice = prompt("Run these commands on %s? (Y/n) (answering 'No' will skip this node)" % (env.host_string, ), default="yes")
    if choice.lower() not in ["y", "ye", "yes"]:
        return []

    current_directory = repodir
    executed_commands = []
    with open(cmd_filename) as commands:
        for command in commands:
            command = command.strip()
            if command.startswith('cd '):
                current_directory = command[3:]
            elif "httpd" in command and env.graceful_reload is True:
                # We are touching apache. Should we take out the node?
                target = env.roles[0]
                execute(disable, target)
                _run_command(current_directory, command)
                ping_host(env.host_string)
                execute(enable, target)
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
def disable(host):
    """
    Disable a server in the haproxy configuration. Use with proxy.

    host is expected to be the full host-name such as: pcudssw1504.cern.ch
    """
    if isinstance(host, basestring):
        host = [host]

    backends = env.proxybackends
    if not backends:
        print("No backends defined")
        return

    server = None
    for alias, item in backends.items():
        # item = ('hostname', [list of backends])
        hostname, list_of_backends = item
        if hostname in host or alias in host:
            server = alias
            break
    if not server:
        print("No server defined")
        return

    servername, backends = backends[server]

    choice = prompt("Disable the following server? %s (Y/n)" % (servername, ), default="yes")
    if choice.lower() not in ["y", "ye", "yes"]:
        return
    proxy_action(servername, backends, action="disable")


@roles(['proxy'])
@task
def enable(host):
    """
    Enable a server in the haproxy configuration. Use with proxy.

    host is expected to be the full host-name such as: pcudssw1504.cern.ch
    """

    if isinstance(host, basestring):
        host = [host]

    backends = env.proxybackends
    if not backends:
        print("No backends defined")
        return

    server = None
    for alias, item in backends.items():
        # item = ('hostname', [list of backends])
        hostname, list_of_backends = item
        if hostname in host or alias in host:
            server = alias
            break
    if not server:
        print("No server defined")
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
    sudo("%s/bin/inveniocfg --run-unit-tests" % (prefixdir,),
         user=apacheuser)


@task
def ping_host(target):
    """
    Does a simple request of the targets web app. Used to load workers.
    """
    if not target.startswith("http"):
        target = "http://%s/" % (target)
    print "Pinging %s..." % (target,)
    while True:
        try:
            urllib2.urlopen(target).read()
            break
        except urllib2.HTTPError, e:
            print e
            time.sleep(1)




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
        run(cmd)


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


def print_command_file(cmd_filename):
    """
    Will print the commands listed in the given file.
    """
    print "--- COMMANDS TO RUN ---"
    with open(cmd_filename) as filecontent:
        print filecontent.read()
    print "--- END OF COMMANDS ---"


def log_deploy(log_filename, executed_commands, log, log_mail):
    """
    Perform logging of deployment using executed commands. Sends a mail
    to given mailaddress with deploy log.

    executed_commands is a dictionary of host -> executed_commands
    """
    out = []
    out.append(log)
    out.append("#+END_EXAMPLE")
    for host, executed in executed_commands.items():
        out.append("")  # Some spacing
        out.append("On host %s:" % (host,))
        out.append("")  # Some spacing
        out.append("#+BEGIN_SRC sh")
        for cmd in executed:
            out.append(cmd)
        out.append("#+END_SRC")

    # Write default logs
    log_file = open(log_filename, 'w')
    log_file.write("\n".join(out))
    log_file.close()

    options = ""
    if CFG_EDITOR in ["subl", "sublime", "sublime-text"]:
        # Wait for sublime to close
        options += "-w"

    # Open log for edit
    local("%s %s %s" % (CFG_EDITOR, options, log_filename))

    with open(log_filename) as logs:
        full_log = logs.readlines()
        subject = full_log[0]
        content = "".join(full_log[1:])
        if send_email(fromaddr=CFG_FROM_EMAIL,
                      toaddr=log_mail,
                      subject=subject,
                      content=content,
                      header="",
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


def _print_end_message():
    print
    print("Deployment completed!")
    print
    print("Remember to push the changes to the operations repository!")
    print("`$ git push origin prod` or `$ git push ops prod` etc.")
    print
