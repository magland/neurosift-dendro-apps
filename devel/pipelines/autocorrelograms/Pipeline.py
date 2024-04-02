from typing import Any, List, Set
import json
from pydantic import BaseModel


class PipelineImportedFile(BaseModel):
    fname: str
    url: str


class PipelineJobInput(BaseModel):
    name: str
    fname: str


class PipelineJobOutput(BaseModel):
    name: str
    fname: str


class PipelineJobParameter(BaseModel):
    name: str
    value: Any


class PipelineJobRequiredResources(BaseModel):
    num_cpus: int
    num_gpus: int
    memory_gb: int
    time_sec: int


class PipelineJob(BaseModel):
    processor_name: str
    inputs: list[PipelineJobInput]
    outputs: list[PipelineJobOutput]
    parameters: list[PipelineJobParameter]
    required_resources: PipelineJobRequiredResources
    run_method: str


class Pipeline:
    def __init__(self):
        self.jobs: List[PipelineJob] = []
        self.imported_files: List[PipelineImportedFile] = []
        self.validator = PipelineValidator()

    def add_imported_file(self, imported_file: PipelineImportedFile):
        self.validator.add_imported_file(imported_file)
        self.imported_files.append(imported_file)

    def add_job(self, job: PipelineJob):
        self.validator.add_job(job)
        self.jobs.append(job)

    def write(self, output_json_fname: str):
        x = {
            'imported_files': [
                imported_file.model_dump() for imported_file in self.imported_files
            ],
            'jobs': [
                job.model_dump() for job in self.jobs
            ]
        }
        with open(output_json_fname, 'w') as f:
            json.dump(x, f, indent=2)


class PipelineValidator:
    def __init__(self):
        self.files: Set[str] = set()

    def add_imported_file(self, imported_file: PipelineImportedFile):
        if imported_file.fname in self.files:
            raise ValueError(f'Cannot import {imported_file.fname}. File already exists in pipeline.')
        self.files.add(imported_file.fname)

    def add_job(self, job: PipelineJob):
        for input in job.inputs:
            if input.fname not in self.files:
                raise ValueError(f'Cannot add job {job.processor_name}. Input file {input.fname} does not exist in pipeline.')
        for output in job.outputs:
            if output.fname in self.files:
                raise ValueError(f'Cannot add job {job.processor_name}. Output file {output.fname} already exists in pipeline.')
            self.files.add(output.fname)
