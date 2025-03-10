from utils.utils import get_file_content

def pipelineEntity(item) -> dict:
    return {
        "name":item["name"],
        "email":item["email"],
        "pname":item["pname"],
        "content_loc":item["content_loc"]
    }

def pipelinesEntity(items) -> list:
    return [pipelineEntity(item) for item in items]