from fastapi import FastAPI
from elastic_main import My_ElasticSearch
import uvicorn
from typing import List, Optional, Union
from pydantic import BaseModel,Field
from datetime import datetime, timezone
from starlette.middleware.cors import CORSMiddleware


def datetime_format(date_string):
    # VD date_string : 2023-03-24 00:00:00
    # my_format = "%Y-%m-%d %H:%M:%S"
    local_dt = datetime.fromisoformat(date_string)

    # Convert local datetime object to UTC datetime object
    utc_dt = local_dt.astimezone(timezone.utc)

    # Format UTC datetime object to ISO 8601 format with UTC timezone
    elastic_formatted_time = utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return elastic_formatted_time

print(datetime_format("2023/04/03 17:34:28"))