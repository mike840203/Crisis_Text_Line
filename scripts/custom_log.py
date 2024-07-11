import logging

class CustomLogger:
    def __init__(self):
        self.logger = logging.getLogger()
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def info(self, class_instance, msg):
        self.logger.info(f"{class_instance.__class__.__name__}: {msg}")

    def error(self, class_instance, msg):
        self.logger.error(f"{class_instance.__class__.__name__}: {msg}")