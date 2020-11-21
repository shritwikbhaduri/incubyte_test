import ast
import json
from typing import List, Dict
from os import path
from jinja2 import Template

import click
import pandas as pd
import datetime as dt
import arrow

from etl.db import DBManager


def convert_date_format(df: pd.DataFrame, date_column: Dict, _format: str) -> pd.DataFrame:
    """
    returns dataframe with desired date format
    :param df:dataframe to be manipulated
    :param date_column: columns of the dataframe that holds dates
    :param _format: format in which the date has been presented
    :return: modified dataframe
    """
    for column in date_column.keys():
        df[column] = df.transform(
            lambda row: arrow.get(str(row[column]) if len(str(row[column])) == 8 else ("0"+str(row[column])),
                                  date_column[column]).format(_format),
            axis=1
        )
    return df


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


def transform_tables(df: pd.DataFrame, configurations: Dict) -> Dict[str, pd.DataFrame]:
    """
    this function returns a list of dataframes depending on the country
    :param configurations: configuration for the data transformation
    :param df: dataframe that needs to be manipulated
    :return: dictionary of dataframe
    """
    click.echo("splitting the dataframe into separated dataframes bases on the Country Column")
    country_list: List = df.Country.unique()

    column_mapping = ast.literal_eval(configurations["columnmapping"])

    # dropping columns that are not required in the final database.
    df.drop(columns=[col for col in df if col not in column_mapping.keys()], inplace=True)

    # renaming columns according to the database requirement
    df.rename(columns=column_mapping, inplace=True)

    # transforming dates according to the database requirements
    date_column: Dict = ast.literal_eval(configurations["datecolumn"])
    convert_date_format(df=df, date_column=date_column, _format=configurations["datetimeformat"])


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
    db = DBManager()
    db.connect_db()

    layout_df = pd.read_csv(path.join(path.dirname(path.abspath(__file__)),
                                      f"../config/detail_record_format/{detail_record_layout}"))

    create_sql_query = render_template(path.join(path.dirname(path.abspath(__file__)),
                                                 "../templates/create_table.sql"),
                                       country_list=table_dict.keys(), **layout_df.to_dict())

    click.echo("\n***create table queries***")
    click.echo(create_sql_query.strip())

    add_row_query = render_template(path.join(path.dirname(path.abspath(__file__)),
                                              "../templates/add_rows_to_tables.sql"), **table_dict)
    click.echo("\n***insert into queries***")
    click.echo(add_row_query.strip())

    db.execute(query=create_sql_query, multi=True)
    db.execute(query=add_row_query, multi=True)
    db.close_connection()
