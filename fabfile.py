import os
import sys
from fabric.api import run, \
                       task, \
                       env, \
                       settings, \
                       local, \
                       cd, \
                       sudo
from fabric.operations import prompt
from tempfile import mkstemp
import time
import re

from invenio.mailutils import send_email
from invenio.config import CFG_SITE_ADMIN_EMAIL

CFG_LINES_TO_IGNORE = ("#",)
CFG_CMDDIR = os.environ.get('TMPDIR', '/tmp')
CFG_FROM_EMAIL = CFG_SITE_ADMIN_EMAIL
CFG_LOG_EMAIL = "admin@inspirehep.net"
CFG_REPODIR = None
CFG_INVENIO_DEPLOY_RECIPE = "/afs/cern.ch/project/inspire/repo/invenio-create-deploy-recipe"

if os.environ.get('EDITOR'):
    CFG_EDITOR = os.environ.get('EDITOR')
elif os.environ.get('VISUAL'):
    CFG_EDITOR = os.environ.get('VISUAL')
else:
    print("ERROR: NO EDITOR/VISUAL variable found. Exiting.")
    sys.exit(1)

env.roledefs = {
    'dev': ['pcudssw1508.cern.ch'],
    'test': ['pcudssw1505.cern.ch'],
    'prod': ['pcudssw1506.cern.ch'],
    'prod_aux': ['pcudssw1507.cern.ch', 'pcudssx1506.cern.ch', 'pcudssw1504.cern.ch']
}
env.fetch = None


@task
def dev():
    """
    Activate configuration for INSPIRE DEV server.
    """
    env.hosts = env.roledefs['dev']
    env.dolog = False
    env.branch = "rebased-2012-03-07-DEV-INSTANCE"


@task
def test():
    """
    Activate configuration for INSPIRE TEST server.
    """
    env.hosts = env.roledefs['test']
    env.dolog = False
    env.branch = "rebased-2012-03-07-TEST-INSTANCE"


@task
def prod():
    """
    Activate configuration for INSPIRE PROD main server.
    """
    env.hosts = env.roledefs['prod']
    env.dolog = True
    env.branch = "rebased-2012-03-07"


@task
def prod_aux():
    """
    Activate configuration for INSPIRE PROD aux servers.
    """
    env.hosts = env.roledefs['prod_aux']
    env.dolog = True


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
def deploy(branch=None, commitid="", recipeargs="--inspire --use-source --no-pull --via-filecopy", repodir=None):
    """
    Do a deployment in given repository using any commitid
    and recipe arguments given.
    """
    if not branch:
        branch = env.branch

    if not repodir:
        if 'repodir' in env and env.repodir:
            repodir = env.repodir
        else:
            print("Error: No repodir")
            sys.exit(1)

    # Checkout remote version of the given branch for deployment
    ready_branch(repodir, branch, env.fetch)

    # Prepare list of commands to run
    out = _get_recipe(repodir, recipeargs, commitid)
    cmd_filename = ready_command_file(out)

    if not cmd_filename:
        print("ERROR: No command file")
        sys.exit(1)

    executed_commands = perform_deploy(cmd_filename, repodir)

    if env.hosts == env.roledefs['prod']:
        default = "prod_aux"
    else:
        default = "no"
    # Run commands (allowing user to edit them beforehand)
    # Users can also run the commands on other hosts right away
    while True:
        choice = prompt("Run these commands more hosts? (One of: %s)" % \
                       (', '.join(env.roledefs.keys()),), default=default)
        if choice != 'no' and choice in env.roledefs:
            # For every host in defined role, perform deploy
            for host in env.roledefs[choice]:
                with settings(host_string=host):
                    perform_deploy(cmd_filename, repodir)
            default = 'no'
        else:
            break

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
    if not repodir:
        if 'repodir' in env and env.repodir:
            repodir = env.repodir
        else:
            print("Error: No repodir")
            sys.exit(1)

    choice = prompt("Edit the commands to be executed (between BEGIN_SRC and END_SRC)? (y/N)", default="no")
    if choice.lower() in ["y", "ye", "yes"]:
        local("%s %s" % (CFG_EDITOR, cmd_filename))

    print("--- COMMANDS TO RUN ---:")
    local("cat %s" % (cmd_filename,))
    print
    choice = prompt("Run these commands on %s? (Y/n)" % (env.host_string,), default="yes")
    if choice.lower() not in ["y", "ye", "yes"]:
        sys.exit(1)

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
def host_type():
    """
    Check host type of remote hosts. Used for tests.
    """
    run('uname -s')


@task
def reset_apache():
    run("sudo /etc/init.d/httpd graceful")


def ready_branch(repodir, branch, repo):
    """
    Connect to hosts and checkout given branch in given
    repository.
    """

    with cd(repodir):
        if repo:
            run("git fetch %s" % repo)
            branch = "%s/%s" % (repo, branch)
        run("git reset --hard " % branch)


def pull_changes(repodir, repo, branch):
    with cd(repodir):
        run("git fetch %s && git reset --hard %s/%s" % (repo, repo, branch))


def ready_command_file(out):
    """
    Will prepare al list of commands, line by line, in a file - based
    on given recipe-text.
    """
    if not "#+BEGIN_SRC sh" in out:
        print("Error, no commands in output")
        return
    # Get everything between SRC
    src = "".join(re.findall("BEGIN_SRC sh\r\n(.*)#\+END_SRC", str(out), re.S))

    # Take out stuff we don't want from the command list
    cleaned_src = "\n".join([line for line in src.split("\r\n") \
                            if line and line.strip() != "" and not line.startswith(CFG_LINES_TO_IGNORE)])

    try:
        # Write default commands
        cmd_filename = _safe_mkstemp()
        command_file = open(cmd_filename, 'w')
        command_file.write(cleaned_src)
        command_file.close()
    except:
        if os.path.exists(cmd_filename):
            os.remove(cmd_filename)
        return None
    return cmd_filename


def log_deploy(log_filename, executed_commands, log, log_mail):
    """
    Perform logging of deployment using executed commands. Sends a mail
    to given mailaddress with deploy log.
    """
    # Write default logs
    log_file = open(log_filename, 'w')
    log_file.write("%s\n#+END_EXAMPLE\n\n#+BEGIN_SRC sh\n%s\n#+END_SRC\n" % (log, "\n".join(executed_commands)))
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
            print("ERROR: Email not sent")
            print(subject)
            print(content)

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
            with cd(directory):
                run(command)
    if command.startswith(('colordiff', 'diff')):
        prompt("Press Enter to continue..")


def _get_recipe(repodir, recipeargs, commitid=""):
    """
    Fetches the output from the recipe generated by invenio-devscript
    invenio-create-deploy-recipe.
    """
    return run('CFG_INVENIO_SRCDIR=%s %s %s %s' % \
              (repodir, CFG_INVENIO_DEPLOY_RECIPE, recipeargs, commitid))


def _safe_mkstemp():
    """
    Create a tempfile in CFG_CMDDIR.
    """
    current_time = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())
    fd_commands, filename_commands = mkstemp(prefix="fab_commands_%s" % (current_time,), \
                                             dir=CFG_CMDDIR)
    os.close(fd_commands)
    return filename_commands
