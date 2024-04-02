from Pipeline import Pipeline, PipelineJob, PipelineJobInput, PipelineJobOutput, PipelineJobRequiredResources, PipelineImportedFile


def create():
    pipeline = Pipeline()
    files = [
        {
            'name': '000946/sub-BH494/sub-BH494_ses-20230820T131000_ecephys.nwb.zarr.json',
            'url': 'https://lindi.neurosift.org/dandi/dandisets/000946/assets/c566ed9d-f27a-4e52-b47d-4408611f80ed/zarr.json'
        }
    ]
    for file in files:
        name = file['name']
        url = file['url']
        pipeline.add_imported_file(PipelineImportedFile(
            fname=f'imported/{name}',
            url=url
        ))
        create_autocorrelograms(
            pipeline=pipeline,
            input=f'imported/{name}',
            output=f'generated/{name}/autocorrelograms.nwb.zarr.json'
        )
    pipeline.write('pipeline.json')


def create_autocorrelograms(pipeline: Pipeline, input: str, output: str):
    pipeline.add_job(PipelineJob(
        processor_name='neurosift-1.autocorrelograms',
        inputs=[
            PipelineJobInput(name='input', fname=input)
        ],
        outputs=[
            PipelineJobOutput(name='output', fname=output)
        ],
        parameters=[],
        required_resources=PipelineJobRequiredResources(
            num_cpus=4,
            num_gpus=0,
            memory_gb=16,
            time_sec=60 * 60 * 24
        ),
        run_method='local'
    ))


if __name__ == '__main__':
    create()
