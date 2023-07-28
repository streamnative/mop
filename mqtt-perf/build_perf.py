import os
import sys
import subprocess
import zipfile
import shutil


def check_current_dir():
    cwd = os.getcwd()
    if not cwd.endswith("mqtt-perf"):
        print("Please call this script in your mqtt-perf root dir. :)")
        sys.exit(-1)


def cleanup_previous():
    cwd = os.getcwd()
    for name in os.listdir(cwd):
        if name.startswith("mqtt-perf-"):
            shutil.rmtree(name)


def build_project():
    clean_std = subprocess.run(['./gradlew', 'clean'], capture_output=True, text=True)
    if clean_std.returncode == 0:
        print(clean_std.stdout)
    else:
        print(clean_std.stderr)

    build_std = subprocess.run(['./gradlew', 'build', '-x', 'test'], capture_output=True, text=True)
    if build_std.returncode == 0:
        print(build_std.stdout)
    else:
        print(build_std.stderr)


def unzip():
    dir_path = os.path.join(os.getcwd(), 'build', 'distributions')
    zip_file_path = None
    zip_file_name = None
    for file_name in os.listdir(dir_path):
        if file_name.endswith(".zip"):
            zip_file_path = os.path.join(dir_path, file_name)
            zip_file_name = file_name
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(os.getcwd())
    bin_parent_dir_name = zip_file_name.replace(".zip", "")
    bin_dir = os.path.join(os.getcwd(), bin_parent_dir_name, "bin")
    for bin_file_name in os.listdir(bin_dir):
        os.chmod(os.path.join(bin_dir, bin_file_name), 0o755)


if __name__ == '__main__':
    check_current_dir()
    cleanup_previous()
    build_project()
    unzip()
