{% for table_name in values["country_list"] %}
CREATE TABLE  IF NOT EXISTS table_{{table_name}}
(
    `temp_column` VARCHAR(1)
    {% for column in values["File_Position"] %}
    ,
        `{{values["Column_Name"][column]}}`  {{values["Data Type"][column]}}{% if values["Data Type"][column] != "DATE"%} ({{values["Filed Length"][column]}}) {% endif %} {% if values["Mandatory"][column] == 'Y' %} NOT NULL {% endif %}
    {% endfor %}
)
    ;
{% endfor %}

{% for table_name in values["country_list"] %}
    {% for column in values["File_Position"] %}
        {% if values["Key Column"][column] == 'Y' %}
        ALTER TABLE table_{{table_name }}
        ADD PRIMARY KEY ({{values['Column_Name'][column]}});
        {% endif %}
    {% endfor %}
{% endfor %}

{% for table_name in values["country_list"] %}
    ALTER TABLE table_{{table_name }}
    DROP COLUMN temp_column;
{% endfor %}
