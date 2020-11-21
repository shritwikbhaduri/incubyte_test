import click
import pyfiglet
import configparser
from typing import Dict

from pandas import DataFrame

from .common import extract_file, transform_tables, load_tables
from os import path


@click.command()
@click.option('--detail-record',
              default='default', help='name of record detail file required for the etl process')
@click.option('--file', default='demo_file.txt', help='name of the data file')
@click.option('--persist', default=False, is_flag=True, help='if set to true, changes will be applied to database')
def main(detail_record: str, file: str, persist):

    etl_banner = pyfiglet.figlet_format("ETL-INCUBYTE")
    click.echo(etl_banner)

    if persist:
        click.echo("***persist is set to true, hence changes will reflect on the database***\n", color=1)
    else:
        click.echo("***persist is set to False, hence changes will not reflect on the database***\n", color=2)

    abs_config_path = path.join(path.dirname(path.abspath(__file__)), '../config/detail_record.ini')
    abs_file_path = path.join(path.dirname(path.abspath(__file__)), f'../demo-files/{file}')

    click.echo(f"detail record: {abs_config_path}")
    click.echo(f"file-path: {abs_file_path}")

    config = configparser.ConfigParser()
    config.read(abs_config_path)

    configuration = dict(config[detail_record.upper()])
    configuration.update(file_path=abs_file_path)

    intermediate_Table: DataFrame = extract_file(**configuration)

    final_tables: Dict[str, DataFrame] = transform_tables(intermediate_Table)

    if persist:
        load_tables(final_tables, configuration["detailrecordlayout"])












