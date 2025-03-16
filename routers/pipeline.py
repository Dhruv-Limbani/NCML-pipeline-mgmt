# fastapi
from fastapi import APIRouter, HTTPException, status, UploadFile
from fastapi.responses import JSONResponse, FileResponse

# db
from pymongo import ReturnDocument
from db_service.mongodb_service import conn
from models.pipeline import Pipeline

# models and schemas
from schemas.pipeline import pipelineEntity, pipelinesEntity

# utilities
from utils.utils import check_db_connection, update_files, write_file, delete_file

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
        500: {"description": "Internal server error"}
    },
    description="This endpoint retrieves all the pipelines of the given user for a particular project."
)
async def get_pipelines(email: str, pname: str):

    check_db_connection(conn)

    pipelines = conn.local.pipelines.find({"email": email, "pname": pname})

    if pipelines is not None:
        pipelines = pipelinesEntity(pipelines)
        if len(pipelines) != 0:
            return JSONResponse(status_code=status.HTTP_200_OK, content={"pipelines":pipelines})

    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipelines not found")

@pipeline_router.get('/', tags=['pipeline'],
    responses={
        200: {"description": "Pipeline retrieved successfully"},
        404: {"description": "Pipeline not found"},
        500: {"description": "Internal server error"}
    },
    description="This endpoint retrieves a pipeline with the given details."
)
async def get_pipeline(email: str, pname: str, name: str):

    check_db_connection(conn)

    exists =  conn.local.pipelines.find_one({"email": email, "pname": pname, "name": name})

    if exists is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")

    path = STORAGE_LOC + os.path.join(email, pname, 'pipelines', name)

    return FileResponse(status_code=status.HTTP_200_OK, path=path, filename=name, media_type="application/octet-stream")

@pipeline_router.post("/", tags=["pipeline"],
    responses = {
        200: {"description": "Pipeline saved successfully"},
        400: {"description": "Bad request"},
        404: {"description": "User/Project not found"},
        500: {"description": "Internal server error"}
    },
    description = "This endpoint saves a pipeline with the given details."
)
async def create_pipeline(email: str, pname: str, file: UploadFile):

    if file.content_type not in ["application/octet-stream", "application/x-pickle"]:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid file type")

    check_db_connection(conn)

    try:

        if conn.local.projects.find_one({"email": email, "name": pname}) is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User/Project not found")

        if conn.local.pipelines.find_one({"email": email, "pname": pname, "name":file.filename}):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"A pipeline with name: {file.filename} already exists under this email and project!")

        path = STORAGE_LOC + os.path.join(email, pname, 'pipelines', file.filename)

        write_file(path, file)

        pipeline = {
            "email": email,
            "pname": pname,
            "name" : file.filename
        }

        conn.local.pipelines.insert_one(pipeline)

        return JSONResponse(status_code=status.HTTP_200_OK, content={"pipeline":pipelineEntity(pipeline)})

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        path = STORAGE_LOC + os.path.join(email, pname, 'pipelines', file.filename)
        delete_file(path)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@pipeline_router.put("/", tags=["pipeline"],
    responses={
        200: {"description": "Pipeline updated successfully"},
        400: {"description": "Bad request"},
        404: {"description": "Project/Pipeline not found"},
        409: {"description": "Name already in use"},
        500: {"description": "Internal server error"}
    },
    description="This endpoint updates the pipeline of an existing project."
)
async def update_pipeline(email: str, pname: str, name: str, file: UploadFile):

    if file.content_type not in ["application/octet-stream", "application/x-pickle"]:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid file type")

    check_db_connection(conn)

    if conn.local.pipelines.find_one({"email": email, "pname": pname, "name": name}) is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project/Pipeline not found")

    if name != file.filename and conn.local.pipelines.find_one({"email": email, "pname": pname, "name":file.filename}) is not None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"A pipeline with name: {file.filename} already exists under this email and project!")

    try:

        update_files(email, pname, name, file)
        update_data = {
            "email": email,
            "pname": pname,
            "name": file.filename
        }
        updated_pipeline = conn.local.pipelines.find_one_and_update(
            {"email": email, "pname": pname, "name":name},
            {'$set': update_data},
            return_document=ReturnDocument.AFTER
        )

        if not updated_pipeline:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")

        return JSONResponse(status_code=status.HTTP_200_OK, content={"pipeline": pipelineEntity(updated_pipeline)})

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@pipeline_router.delete("/", tags=["pipeline"],
    responses={
        200: {"description": "Pipeline deleted successfully"},
        404: {"description": "Pipeline not found"},
        500: {"description": "Internal server error"}
    },
    description="This endpoint deletes a pipeline by it's user, project name and name"
)
async def delete_dataset(pipeline: Pipeline):

    check_db_connection(conn)

    deleted_pipeline = conn.local.pipelines.find_one_and_delete({"email": pipeline.email, "pname": pipeline.pname, "name": pipeline.name})

    if not deleted_pipeline:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")

    file_loc = STORAGE_LOC + os.path.join(pipeline.email, pipeline.pname, 'pipelines', pipeline.name)

    delete_file(file_loc)

    return JSONResponse(status_code=status.HTTP_200_OK, content={"message": "Pipeline deleted successfully!"})