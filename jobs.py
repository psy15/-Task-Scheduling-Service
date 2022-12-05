from redis_om import (EmbeddedJsonModel, Field, JsonModel)
from pydantic import PositiveInt
from typing import Optional, List


class Job(JsonModel):
    # Indexed for exact text matching
    job_id: PositiveInt = Field(index=True)
    priority: PositiveInt = Field(index=True)

    # Indexed for numeric matching
    dependency: Optional[PositiveInt]
