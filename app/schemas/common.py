from pydantic import BaseModel


class SeriesItem(BaseModel):
    label: str
    value: float
    percentage: float | None = None
