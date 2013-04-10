import sublime
import sublime_plugin
import threading
import subprocess
import os
import functools


MAP = {
    'g s': ['git_status'],
    'g b': ['git_branch'],
    'g show file': ['git_show'],
    'g m': ['git_rename_branch'],
    'g l': ['git_log_all'],
    'g a': ['git_add', 'git_status'],
    'g clean': ['git_clean'],
    'cb': ['invenio_check_branch']
}


class LauncherShowInputCommand(sublime_plugin.WindowCommand):
    last_command = None

    def run(self):
        self.window.show_input_panel("Command", "",
            self.run_command, None, None)

    def run_command(self, command):
        # Ability to repeat last command
        if command == 'l':
            command = self.last_command
        else:
            self.last_command = command

        real_commands = MAP[command]
        for cmd in real_commands:
            self.window.run_command(cmd)


def scratch(output, window=None):
    print 'window', window
    if window is None:
        window = sublime.active_window()
    scratch_file = window.new_file()
    scratch_file.set_scratch(True)
    edit = scratch_file.begin_edit()
    scratch_file.insert(edit, 0, output)
    scratch_file.end_edit(edit)
    scratch_file.set_read_only(True)


def do_when(conditional, callback, *args, **kwargs):
    if conditional():
        return callback(*args, **kwargs)
    sublime.set_timeout(functools.partial(do_when, conditional, callback, *args, **kwargs), 50)


def main_thread(callback, *args, **kwargs):
    # sublime.set_timeout gets used to send things onto the main thread
    # most sublime.[something] calls need to be on the main thread
    sublime.set_timeout(functools.partial(callback, *args, **kwargs), 0)


class CommandThread(threading.Thread):
    def __init__(self, command, on_done, working_dir="", fallback_encoding="", **kwargs):
        threading.Thread.__init__(self)
        self.command = command
        self.on_done = on_done
        self.working_dir = working_dir
        if "stdin" in kwargs:
            self.stdin = kwargs["stdin"]
        else:
            self.stdin = None
        if "stdout" in kwargs:
            self.stdout = kwargs["stdout"]
        else:
            self.stdout = subprocess.PIPE
        self.fallback_encoding = fallback_encoding
        self.kwargs = kwargs

    def run(self):
        try:

            # Ignore directories that no longer exist
            if os.path.isdir(self.working_dir):

                # For editiing commit messages and such which are in the .git
                # directory, we need to cd out of it
                git_dir_suffix = '.git'
                if self.working_dir.endswith(git_dir_suffix):
                    self.working_dir = self.working_dir[:-len(git_dir_suffix)]

                # Per http://bugs.python.org/issue8557 shell=True is required to
                # get $PATH on Windows. Yay portable code.
                shell = os.name == 'nt'
                if self.working_dir != "":
                    os.chdir(self.working_dir)

                print 'env', os.environ
                print 'command', self.command
                proc = subprocess.Popen(self.command,
                    stdout=self.stdout, stderr=subprocess.STDOUT,
                    stdin=subprocess.PIPE,
                    shell=shell, universal_newlines=True)
                output = proc.communicate(self.stdin)[0]
                if not output:
                    output = ''
                proc.wait()
                # if sublime's python gets bumped to 2.7 we can just do:
                # output = subprocess.check_output(self.command)
                if proc.returncode:
                    main_thread(scratch, output)
                main_thread(self.on_done,
                            proc.returncode,
                            output,
                            **self.kwargs)

        except subprocess.CalledProcessError, e:
            main_thread(self.on_done, e.returncode)
        except OSError, e:
            if e.errno == 2:
                main_thread(sublime.error_message, "Git binary could not be found in PATH\n\nConsider using the git_command setting for the Git plugin\n\nPATH is: %s" % os.environ['PATH'])
            else:
                raise e


class Command(object):
    def run(self):
        self.run_command(self.command)

    def run_command(self, command, callback=None, show_status=False,
            filter_empty_args=True, no_save=False, **kwargs):
        if filter_empty_args:
            command = [arg for arg in command if arg]
        if 'working_dir' not in kwargs:
            kwargs['working_dir'] = self.get_working_dir()

        s = sublime.load_settings("Launcher.sublime-settings")

        if command[0] == 'git' and s.get('git_command'):
            command[0] = s.get('git_command')

        if not callback:
            callback = self.generic_done

        thread = CommandThread(command, callback, **kwargs)
        thread.start()

        if show_status:
            message = kwargs.get('status_message', False) or ' '.join(command)
            sublime.status_message(message)

    def generic_done(self, result):
        if not result.strip():
            return
        self.panel(result)

    def _output_to_view(self, output_file, output, clear=False,
            syntax="Packages/Diff/Diff.tmLanguage", **kwargs):
        output_file.set_syntax_file(syntax)
        edit = output_file.begin_edit()
        if clear:
            region = sublime.Region(0, self.output_view.size())
            output_file.erase(edit, region)
        output_file.insert(edit, 0, output)
        output_file.end_edit(edit)

    def scratch(self, output, title=False, position=None, **kwargs):
        scratch_file = self.view.window().new_file()
        if title:
            scratch_file.set_name(title)
        scratch_file.set_scratch(True)
        self._output_to_view(scratch_file, output, **kwargs)
        scratch_file.set_read_only(True)
        if position:
            sublime.set_timeout(lambda: scratch_file.set_viewport_position(position), 0)
        return scratch_file

    def panel(self, output, **kwargs):
        if not hasattr(self, 'output_view'):
            self.output_view = self.get_window().get_output_panel("git")
        self.output_view.set_read_only(False)
        self._output_to_view(self.output_view, output, clear=True, **kwargs)
        self.output_view.set_read_only(True)
        self.get_window().run_command("show_panel", {"panel": "output.git"})

    def quick_panel(self, *args, **kwargs):
        self.get_window().show_quick_panel(*args, **kwargs)


class InvenioCheckBranch(Command, sublime_plugin.TextCommand):
    def run(self, edit):
        self.run_command(['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
                         self.status_done)

    def get_working_dir(self):
        return os.path.realpath(os.path.dirname(self.view.file_name()))

    def status_done(self, returncode, current_branch):
        current_branch = current_branch.strip()
        self.run_command(['invenio-check-branch', 'master', current_branch],
                         self.check_branch_done)

    def check_branch_done(self, returncode, output):
        self.scratch(output)
