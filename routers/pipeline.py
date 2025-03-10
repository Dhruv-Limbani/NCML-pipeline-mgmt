# fastapi
from fastapi import APIRouter, HTTPException, status, UploadFile
from fastapi.responses import JSONResponse, FileResponse

# db
from pymongo import ReturnDocument
from db_service.mongodb_service import conn

# models and schemas
from schemas.pipeline import pipelineEntity, pipelinesEntity

# utilities
from utils.utils import check_db_connection, save_file, update_files, delete_files

# os and environment
import os
from dotenv import load_dotenv
load_dotenv()

STORAGE_LOC = os.getenv("STORAGE_LOC")

pipeline_router = APIRouter()

@pipeline_router.get('/all', tags=['pipeline'],
    responses={
        200: {"description": "Pipelines retrieved successfully"},
        404: {"description": "Pipelines not found"},
        500: {"description": "Database not live"}
    },
    description="This endpoint retrieves all the pipelines of the given user for a particular project."
)
async def get_pipelines(email: str, pname: str):

    check_db_connection(conn)

    pipelines = conn.local.pipelines.find({"email": email, "pname": pname})

    if pipelines is not None:
        pipelines = pipelinesEntity(pipelines)
        if len(pipelines) != 0:
            return JSONResponse(status_code=200, content={"pipelines":pipelines})

    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipelines not found")

@pipeline_router.get('/', tags=['pipeline'],
    responses={
        200: {"description": "Pipeline retrieved successfully"},
        404: {"description": "Pipeline not found"},
        500: {"description": "Database not live"}
    },
    description="This endpoint retrieves a pipeline with the given details."
)
async def get_pipeline(email: str, pname: str, name: str):

    check_db_connection(conn)

    pipeline =  conn.local.pipelines.find_one({"email": email, "name": name, "pname": pname})

    if pipeline is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")

    return FileResponse(status_code=200, path=STORAGE_LOC + pipeline['content_loc'], filename=pipeline['name'], media_type="application/octet-stream")

@pipeline_router.post("/", tags=["pipeline"],
    responses = {
        200: {"description": "Pipeline saved Successfully"},
        400: {"description": "Bad request"},
        404: {"description": "User/Project not found"},
        500: {"description": "Database not live"}
    },
    description = "This endpoint saves a pipeline with the given details."
)
async def create_pipeline(email: str, pname: str, file: UploadFile):

    if file.content_type not in ["application/octet-stream", "application/x-pickle"]:
        raise HTTPException(status_code=400, detail="Invalid file type")

    check_db_connection(conn)

    try:

        if conn.local.projects.find_one({"email": email, "name": pname}) is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User/Project not found")

        if conn.local.pipelines.find_one({"email": email, "pname": pname, "name":file.filename}):
            raise HTTPException(status_code=400, detail=f"A pipeline with name: {file.filename} already exists under this email and project!")

        pipeline = save_file(email, pname, file)

        conn.local.pipelines.insert_one(dict(pipeline))

        return FileResponse(status_code=200, path=STORAGE_LOC + pipeline.content_loc, filename=pipeline.name, media_type="application/octet-stream")

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# @dataset_router.put("/", tags=["dataset"],
#     responses={
#         200: {"description": "Dataset updated successfully"},
#         400: {"description": "Bad request"},
#         404: {"description": "Project/Dataset not found"},
#         409: {"description": "Name already in use"},
#         500: {"description": "Database not live"}
#     },
#     description="This endpoint updates the dataset of an existing project."
# )
# async def update_dataset(email: str, pname: str, name: str, file: UploadFile):
#
#     check_db_connection(conn)
#
#     if conn.local.datasets.find_one({"email": email, "pname": pname, "name":name}) is None:
#         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
#
#     if name != file.filename and conn.local.datasets.find_one({"email": email, "pname": pname, "name":file.filename}) is not None:
#         raise HTTPException(status_code=400, detail=f"A dataset with name: {file.filename} already exists under this email and project!")
#
#     try:
#
#         new = update_files(email, pname, name, file)
#         update_data = {k: v for k, v in dict(new).items() if k not in ["_id", "email", "pname"]}
#         updated_dataset = conn.local.datasets.find_one_and_update(
#             {"email": email, "pname": pname, "name":name},
#             {'$set': update_data},
#             return_document=ReturnDocument.AFTER
#         )
#
#         if not updated_dataset:
#             raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
#
#         print(datasetEntity(updated_dataset)["content"])
#         return JSONResponse(status_code=200, content={"dataset": datasetEntity(updated_dataset)})
#
#     except Exception as e:
#         raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
#
#
# @dataset_router.delete("/", tags=["dataset"],
#     responses={
#         200: {"description": "Dataset deleted successfully"},
#         404: {"description": "Dataset not found"},
#         500: {"description": "Database not live"}
#     },
#     description="This endpoint deletes a dataset by it's user, project name and name"
# )
# async def delete_dataset(email: str, pname: str, name: str):
#
#     check_db_connection(conn)
#
#     deleted_dataset = conn.local.datasets.find_one_and_delete({"email": email, "pname": pname, "name": name})
#
#     if not deleted_dataset:
#         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
#
#     delete_files(email, pname, name)
#
#     return JSONResponse(status_code=200, content={"message": "Dataset deleted successfully!"})