from os import path

import click
from mysql.connector import ClientFlag, errorcode
from environment import get_settings

import mysql.connector

config = get_settings()


class DBManager:
    def __init__(self,
                 host=config.DB_HOST,
                 user=config.DB_USER,
                 password=config.DB_PASSWORD,
                 database=config.DB_NAME):
        self.host: str = host
        self.user: str = user
        self.password: str = password
        self.db_name: str = database
        self.ssl_ca = path.join(path.dirname(path.abspath(__file__)), '../certificates/ssl/server-ca.pem')
        self.ssl_cert = path.join(path.dirname(path.abspath(__file__)), '../certificates/ssl/client-cert.pem')
        self.ssl_key = path.join(path.dirname(path.abspath(__file__)), '../certificates/ssl/client-key.pem')
        self.db = None
        self.cursor = None

    def connect_db(self):
        """
        this function is responsible for database connection operation
        :return: cursor
        """
        try:
            self.db = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.db_name,
                client_flags=[ClientFlag.SSL],
                ssl_ca=self.ssl_ca,
                ssl_cert=self.ssl_cert,
                ssl_key=self.ssl_key
            )
            self.cursor = self.db.cursor()
        except mysql.connector.Error as err:
            click.echo(f"Error in connecting to database({err})")
            exit(1)

    def execute(self, query: str, **kwargs):
        try:
            result = self.cursor.execute(query, **kwargs)
            click.echo("successfully executed all queries")
            return result
        except mysql.connector.Error as err:
            print(f"Failed to execute self({err})")

    def fetch_all(self, query: str):
        try:
            result = self.cursor.fetch_all(query)
            click.echo("successfully fetched all rows")
            return result
        except mysql.connector.Error as err:
            click.echo(f"Failed to fetch data({err})")

    def close_connection(self):
        self.cursor.close()
