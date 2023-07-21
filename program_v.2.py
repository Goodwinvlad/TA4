import json
from statistics import median
from typing import Any
from collections import defaultdict
import time

start_time = time.time()

_path = "data.jsonl"
#_path = "data_little.jsonl"
def read_file(_path: str) -> dict[tuple[Any, Any], list]:
    grouped_data = defaultdict(list)
    with open(_path, "r") as file:
        for line in file:
            data = json.loads(line)

            date = data["date"]
            sensor = data["input"]
            value = data["value"]

            key = (date, sensor)
            grouped_data[key].append(value)

    return grouped_data


def group_file(grouped_data: dict) -> list[dict[str, Any]]:
    return list((
        {"date": key[0], "input": key[1], "median_value": median(values)}
        for key, values in grouped_data.items()
    ))

def write_file(data: dict) -> None:
    with open("new_data.jsonl", "w") as file:
        for record in data:
            json.dump(record, file)
            file.write("\n")

write_file(group_file(read_file(_path)))

print("time elapsed: {:.2f}s".format(time.time() - start_time))
