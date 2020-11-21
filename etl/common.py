from typing import List, Dict
from os import path
from jinja2 import Template

import click
import pandas as pd
import datetime as dt
import arrow

from etl.db import connect_db


def convert_date_format(date: str, format: str) -> dt.datetime:
    """
    returns datetime object
    :param date: string date that has to be converted
    :param format: format in which the date has been presented
    :return: datetime object
    """
    return arrow.get(date, format).datetime


def render_template(template_path, **kwargs) -> str:
    """
    accepts the template location and return the rendered template
    :param template_path:
    :param kwargs:
    :return: string
    """
    template = Template(open(template_path).read())
    return template.render(values=kwargs)


def extract_file(file_path: str, headerrecordlayout: str, **kwargs) -> pd.DataFrame:
    """
    this function accepts an configuration file and according to the configuration file extracts and returns a dataframe
    :param headerrecordlayout:
    :param file_path:
    :param kwargs:
    :return: a dataframe containing relevant data.
    """
    click.echo("extracting data from the file to form a dataframe")
    df: pd.DataFrame = pd.read_csv(file_path, sep="|")
    required_column = headerrecordlayout.split('|')[2:]
    df.drop(df.columns.difference(required_column), 1, inplace=True)
    click.echo()

    return df


def transform_tables(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    this function returns a list of dataframes depending on the country
    :param df: dataframe that needs to be manipulated
    :return: dictionary of dataframe
    """
    click.echo("splitting the dataframe into separated dataframes bases on the Country Column")
    country_list: List = df.Country.unique()
    final_dict: Dict = {}
    [final_dict.update({element: df[df['Country'] == element].to_dict('records')}) for element in country_list]
    return final_dict


def load_tables(table_dict: Dict, detail_record_layout: str):
    """
    accept dict of dataframe and loads in to database
    :param detail_record_layout:
    :param table_dict:
    :return:
    """
    layout_df = pd.read_csv(path.join(path.dirname(path.abspath(__file__)),
                                      f"../config/detail_record_format/{detail_record_layout}"))

    create_sql_query = render_template(path.join(path.dirname(path.abspath(__file__)),
                                                 "../templates/create_table.sql"),
                                       country_list=table_dict.keys(), **layout_df.to_dict())

    click.echo("\n***create table queries***")
    click.echo(create_sql_query)

    add_row_query = render_template(path.join(path.dirname(path.abspath(__file__)),
                                              "../templates/add_rows_to_tables.sql"), **table_dict)
    click.echo("\n***insert into queries***")
    click.echo(add_row_query)

    cursor = connect_db()
