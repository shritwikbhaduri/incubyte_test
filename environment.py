from dataclasses import dataclass
from os import environ, path


@dataclass
class IntegrationSettings:
    DB_HOST: str = environ.get('DB_HOST', '34.71.203.37')
    DB_USER: str = environ.get('DB_USER', 'incubytetestuser')
    DB_PASSWORD: str = environ.get('DB_PASSWORD', 'incubyte1234')
    DB_NAME: str = environ.get('DB_NAME', 'tempdb')
    SSL_CA = path.join(path.dirname(path.abspath(__file__)), 'certificates/ssl/server-ca.pem'),
    SSL_CERT = path.join(path.dirname(path.abspath(__file__)), 'certificates/ssl/client-cert.pem'),
    SSL_KEY = path.join(path.dirname(path.abspath(__file__)), 'certificates/ssl/client-key.pem')


@dataclass
class ProductionSettings:
    DB_HOST: str = environ.get('DB_HOST', '34.71.203.37')
    DB_USER: str = environ.get('DB_USER', 'incubytetestuser')
    DB_PASSWORD: str = environ.get('DB_PASSWORD', 'incubyte1234')
    DB_NAME: str = environ.get('DB_NAME', 'tempdb')


def get_settings(env: str = 'integration'):
    ENV = environ.get('ENV', env)
    if ENV == "production":
        return ProductionSettings()
    ENV = environ.get('ENV', env)
    if ENV == "integration":
        return IntegrationSettings()
