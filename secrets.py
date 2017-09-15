import os

basedir = os.path.abspath(os.path.dirname(__file__))

class Config:
    S3_CREDS = {
        "aws_access_key_id": "",
        "aws_secret_access_key": ""
    }

    SCHEMA = ""
    PAGE_DATA_TABLE_NAME = ""
    PROGRESS_DATA_TABLE_NAME = ""
    S3_BUCKET = ""
    CONNECTION_STRING = ''

    FILE_PATH = './cache/'


class DevelopmentConfig(Config):
    pass


class TestingConfig(Config):
    pass


class ProductionConfig(Config):
    pass


config = {
    'development': DevelopmentConfig,
    'testing': TestingConfig,
    'production': ProductionConfig,

    'default': DevelopmentConfig
}
