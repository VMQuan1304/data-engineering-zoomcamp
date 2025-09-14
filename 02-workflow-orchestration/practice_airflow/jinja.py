from jinja2 import Template
from datetime import datetime

execution_date = datetime(2021, 6, 15, 0, 0, 0)

context = {
    "ds": execution_date.strftime("%Y-%m-%d"),
    "execution_date": execution_date.isoformat(),
    "ts_nodash": execution_date.strftime("%Y%m%dT%H%M%S"),
}

template = Template("Run date: {{ ds }}, TS: {{ ts_nodash }}, Full: {{ execution_date }}")
print(template.render(**context))
