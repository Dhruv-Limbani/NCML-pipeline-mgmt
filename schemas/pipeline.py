def pipelineEntity(item) -> dict:
    return {
        "name":item["name"],
        "email":item["email"],
        "pname":item["pname"]
    }

def pipelinesEntity(items) -> list:
    return [pipelineEntity(item) for item in items]