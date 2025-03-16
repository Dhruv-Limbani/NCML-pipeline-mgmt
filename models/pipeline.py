from pydantic import BaseModel

class Pipeline(BaseModel):
    email: str
    pname: str
    name: str
