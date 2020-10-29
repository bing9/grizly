#!/usr/bin/env python
import subprocess
import click
import os

#chmod a+x ptest

def get_blocks():
    with open(".gitignore", "r") as f:
            lines = [line.replace("\n", "") for line in f.readlines()]
            try:
                indx = lines.index('#GITS3')
            except ValueError:
                indx = -1
            if indx == -1:
                git_ignore_lines = lines
                git_s3_lines = []
            else:
                git_ignore_lines = lines[0:indx]
                git_s3_lines = lines[indx+1:]
    return git_ignore_lines, git_s3_lines

def write_blocks(git_ignore_lines, git_s3_lines, path=None):
    with open(".gitignore", "w") as f:
        git_s3_lines.insert(0, "#GITS3")
        if path != None:
            git_s3_lines.append(path)
        final = git_ignore_lines + git_s3_lines
        out = ""
        for line in final:
            out += line + "\n"
        f.write(out)

def filter_paths(path_filter, paths_to_search):
    index = -1
    paths_found = []
    for path in paths_to_search:
        index += 1
        if path_filter != None:
            if path_filter in path:
                paths_found.append((index, path))
        else:
            paths_found.append((index, path))
    return paths_found

def sync_s3(path, bucket="acoe-s3", local_to_s3=True):
    cwd = os.getcwd()
    repo = os.path.basename(os.path.normpath(cwd))
    full_path = os.path.join(cwd, path)
    s3_uri = f"s3://{bucket}/{repo}/{path}"
    if local_to_s3:
        cmd = f"aws s3 sync {full_path} {s3_uri}"
    else:
        cmd = f"aws s3 sync {s3_uri} {full_path}"
    process = subprocess.Popen(cmd, shell=True, stdout = subprocess.PIPE)
    process.wait()
    print(process.communicate())

def getobjects_s3(to_file = True):
    cwd = os.getcwd()
    repo = os.path.basename(os.path.normpath(cwd))
    cwd = os.getcwd()
    repo = os.path.basename(os.path.normpath(cwd))
    cmd = f"aws s3 ls s3://acoe-s3/{repo}/ --recursive"
    process = subprocess.Popen(cmd, shell=True, stdout = subprocess.PIPE)
    process.wait()
    output, err = process.communicate(b"input data that is passed to subprocess' stdin")
    objects = []
    for item in output.split():
        if "grizly_webui" in str(item):
            objects.append(item.decode("utf-8"))
    with open(".s3synced", "w") as f:
        out = "\n".join(objects)
        f.write(out)
    return objects

CONTEXT_SETTINGS = dict(help_option_names=['-h'])

@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    """Stores and manages Git repo large files in AWS S3.
    """
    pass

@click.command()
@click.argument('path')
def track(path):
    """Adds input path to the tracked files. 
       Files are tracked inside .gitignore under tag #GITS3
    """
    git_ignore_lines, git_s3_lines  = get_blocks()
    if path in git_s3_lines:
        raise ValueError("Path is already tracked")
    else:
      write_blocks(git_ignore_lines, git_s3_lines, path)
            

@click.command()
@click.argument('path')
def push(path):
    """Pushes input path files/objects into S3.
       Input can be value or index
    """
    git_ignore_lines, git_s3_lines  = get_blocks()
    if path.isdigit():
        indx = int(path)
        try:
            _path = git_s3_lines[indx]
            sync_s3(_path)
            getobjects_s3()
        except IndexError:
            raise IndexError(f"""Your path index {path} is not an index of 
                                tracked files. Check available indexes with git 
                                s3 list
                            """)
    else:
        paths_found = filter_paths(path, git_s3_lines)
        if paths_found == []:
            raise ValueError(f"Path is not in tracked paths, try: git s3 track {path}")
        else:
            for path in paths_found:
                sync_s3(path[1])
                getobjects_s3()

@click.command()
@click.argument('path')
def pull(path):
    """Pulls all files from S3 inside input path.
    """
    git_ignore_lines, git_s3_lines  = get_blocks()
    paths_found = filter_paths(path, git_s3_lines)
    for path in paths_found:
        sync_s3(path[1], local_to_s3=False)

@click.command()
@click.argument('path')
def remove(path, bucket="acoe-s3"):
    """Removes input path from the tracked paths.
       Input can be value or index
    """
    git_ignore_lines, git_s3_lines  = get_blocks()
    if path.isdigit():
        indx = int(path)
        try:
            _path = git_s3_lines[indx]
            git_s3_lines.remove(_path)
            write_blocks(git_ignore_lines, git_s3_lines)
        except IndexError:
            raise IndexError(f"""Your path index {path} is not an index of 
                                tracked files. Check available indexes with git 
                                s3 list
                            """)
    elif path in git_s3_lines:
        git_s3_lines.remove(path)
        write_blocks(git_ignore_lines, git_s3_lines)
    else:
        raise ValueError(f"Path {path} is not in tracked paths")

@click.command()
@click.option('-f', "path_filter", help="filters paths containing input value")
def list(path_filter=None):
    """Lists all tracked paths to be in sync in s3. 
       These folders are stored in .gitignore under the tag #GITS3
    """
    git_ignore_lines, git_s3_lines = get_blocks()
    paths_found = filter_paths(path_filter, git_s3_lines)
    for path in paths_found:
        click.echo(str(path[0]) + " " + path[1])

@click.command()
def synced():
    """Lists of all objects (files) stored into S3. 
       from the current repo
    """
    objects = getobjects_s3()
    for obj in objects:
        click.echo(obj)


cli.add_command(track)
cli.add_command(push)
cli.add_command(pull)
cli.add_command(remove)
cli.add_command(list)
cli.add_command(synced)