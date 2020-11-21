{% for key, value in values.items() %}
INSERT INTO table_{{key}}
({{value[1].keys()|join(", ")}})
VALUES
{% for num in range(0,value|length) %}
({{value[num].values()|join(", ")}})
{% if num+1 < value|length%},{% endif %}
{% endfor %}
{% endfor %}