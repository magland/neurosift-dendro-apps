from Pipeline import Pipeline, PipelineJob, PipelineJobInput, PipelineJobOutput, PipelineJobRequiredResources, PipelineImportedFile


def main():
    # 000946 (draft): Neural Pathways Modulation in the Anesthetized Rat Elicited by Trials of Transcranial Focused Ultrasound Stimulation
    dandiset_id = '000946'
    dandiset_version = 'draft'
    # project: D-000946
    pipeline = Pipeline(project_id='20607ea8')
    files = [
        {
            'name': '000946/sub-BH494/sub-BH494_ses-20230820T131000_ecephys.nwb.lindi.json',
            'url': 'https://lindi.neurosift.org/dandi/dandisets/000946/assets/c566ed9d-f27a-4e52-b47d-4408611f80ed/zarr.json',
            'asset_id': 'c566ed9d-f27a-4e52-b47d-4408611f80ed'
        }
    ]
    for file in files:
        name = file['name']
        url = file['url']
        pipeline.add_imported_file(PipelineImportedFile(
            fname=f'imported/{name}',
            url=url,
            metadata={
                'dandisetId': dandiset_id,
                'dandisetVersion': dandiset_version,
                'dandiAssetId': file['asset_id']
            }
        ))
        create_autocorrelograms(
            pipeline=pipeline,
            input=f'imported/{name}',
            output=f'generated/{name}/autocorrelograms.nwb.lindi.json',
            metadata={
                'dandisetId': dandiset_id,
                'dandisetVersion': dandiset_version,
                'dandiAssetId': file['asset_id'],
                'supplemental': True
            }
        )
    pipeline.submit()


def create_autocorrelograms(*, pipeline: Pipeline, input: str, output: str, metadata: dict):
    pipeline.add_job(PipelineJob(
        processor_name='neurosift-1.autocorrelograms',
        inputs=[
            PipelineJobInput(name='input', fname=input)
        ],
        outputs=[
            PipelineJobOutput(name='output', fname=output, metadata=metadata)
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
    main()
