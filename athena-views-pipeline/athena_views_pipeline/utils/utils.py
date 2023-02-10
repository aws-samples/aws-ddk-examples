import os
import re
from textwrap import dedent


def get_events_json():
    regex_find_schedule = re.compile(r'"(cron[^\"]+)"')
    event_jsons = []
    for path, _, filenames in os.walk("athena_views_pipeline/sql/"):
        database_name = path[len("athena_views_pipeline/sql/") :]

        if not filenames:
            continue

        for filename in filenames:
            schedule = None
            if not filename.endswith(".sql"):
                continue

            view_name = filename[: -len(".sql")]
            with open(
                f"athena_views_pipeline/sql/{database_name}/{view_name}.sql"
            ) as f:
                query = f.read()

            first_line = query.split("\n", 1)[0]

            if first_line.startswith("-- SCHEDULE: "):
                schedule = regex_find_schedule.findall(first_line)[0]
                query = " ".join(query.split("\n")[1:])
            else:
                query = " ".join(query.split("\n"))

            event_json = {}
            if query:
                event_json["db"] = database_name
                event_json["view"] = view_name
                event_json["query"] = dedent(query).strip("\n")
                if schedule:
                    event_json["schedule"] = schedule
                else:
                    event_json["schedule"] = "cron(*/5 * * * ? *)"

            event_jsons.append(event_json)

    return event_jsons
