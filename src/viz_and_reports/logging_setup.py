import os
from omegaconf import OmegaConf, DictConfig
import wandb
from loguru import logger

from utils.file_utils import (
    create_zip,
    get_git_info,
    save_conda_package_versions,
)


def wandb_setup(config, wandb_setup_conf):

    wandb.config = config
    
    tags = wandb_setup_conf.get("tags")
    for ele in tags:
        if isinstance(ele, list):
            tags.remove(ele)
            tags.extend(ele)
        elif ele is None:
            tags.remove(ele)
    
    run = wandb.init(
        entity=wandb_setup_conf.get("entity"),
        project=wandb_setup_conf.get("project"),
        group=wandb_setup_conf.get("group"),
        tags=tags,
        notes=wandb_setup_conf.get("notes"),
        dir=config.get("path_run"),
    )
    logger.info("WandB run url: {}".format(run.url))
    logger.info("WandB project: {}".format(run.project))
    logger.info("WandB entity: {}".format(run.entity))
    logger.info("WandB run name: {}".format(run.name))
    logger.info("WandB run id: {}".format(run.id))
    logger.info("Local Directory Path: {}".format(config["path_run"]))
    wandb.log({"metadata/local_dir": config.get("path_run")})
    # Set the WANDB_DIR environment variable to your desired directory
    os.environ["WANDB_DIR"] = os.path.abspath(config.get("path_run"))


def setup(config: dict, WandB_hyperopt=False):
    logger.add(
        config["file_log"],
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {module}:{function}:{line} - {message}",
        level="INFO",
    )
    # Save code, git info, and config file to run directory
    create_zip(
        f"{os.getcwd()}/python",
        f'{config["path_run"]}/code.zip',
        exclude=config["code_snapshot_exlude"],
    )
    save_conda_package_versions(config["path_run"])
    git_info = get_git_info()
    logger.info("Git info: {}".format(git_info))
    config |= git_info

    logger.info(f"Beginning pipeline...")

    # 2. Log config file to wandb, set up hydra logging, and save to disk
    if (wandb_setup_conf := config.get("wandb")) is not None and not WandB_hyperopt:
        wandb_setup(config, wandb_setup_conf)

    return logger
