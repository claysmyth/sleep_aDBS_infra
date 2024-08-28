import os
from .utils.file_utils import (
    create_zip,
    get_git_info,
    save_conda_package_versions,
)

def local_setup(path):

    # Save code, git info, and config file to run directory
    create_zip(
        f"{os.getcwd()}/python",
        f'{path}/code.zip',
        exclude=config["code_snapshot_exlude"],
    )
    save_conda_package_versions(path)
    git_info = get_git_info()
    # Write git info to a text file
    git_info_path = os.path.join(path, "git_info.txt")
    with open(git_info_path, "w") as f:
        for key, value in git_info.items():
            f.write(f"{key}: {value}\n")


def convert_polars_to_WandB_table(dataframe, table_name):
    pass

def log_to_WandB(dataframe, table_name):
    pass