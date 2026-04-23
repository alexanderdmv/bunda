from loguru import logger
import sys

def setup_logger(level: str = "INFO"):
    logger.remove()

    # Безопасно добавляем кастомный уровень (если ещё не добавлен)
    if level not in ["TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"]:
        try:
            logger.level(level, no=20, color="<cyan>", icon="🚀")
        except ValueError:
            pass  

    logger.add(
        sys.stdout,
        level=level,
        format="{time:HH:mm:ss} | <level>{level}</level> | {message}"
    )
    logger.add(
        "logs/pipeline.log",
        level=level,
        rotation="5 MB",
        format="{time} | {level} | {message}"
    )
    return logger