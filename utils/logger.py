from loguru import logger
import sys

def setup_logger(level="INFO"):
    logger.remove()

    logger.add(
        sys.stdout,
        level=level,
        format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}"
    )

    logger.add(
        "logs/pipeline.log",
        level=level,
        rotation="5 MB",
        format="{time} | {level} | {message}"
    )

    return logger
