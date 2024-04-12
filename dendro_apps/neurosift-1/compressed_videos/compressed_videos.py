from dendro.sdk import ProcessorBase, InputFile, OutputFile
from dendro.sdk import BaseModel, Field
import numpy as np


class CompressedVideosContext(BaseModel):
    input: InputFile = Field(description="Input .nwb.lindi.json file")
    output: OutputFile = Field(description="Output .nwb.lindi.json file")


class CompressedVideosProcessor(ProcessorBase):
    name = "neurosift-1.compressed_videos"
    description = "Create compressed videos"
    label = "neurosift-1.compressed_videos"
    tags = []
    attributes = {"wip": True}

    @staticmethod
    def run(context: CompressedVideosContext):
        import shutil
        import h5py
        import uuid
        import time
        import lindi
        import kachery_cloud as kcl
        from neurosift.codecs import MP4AVCCodec
        MP4AVCCodec.register_codec()

        with lindi.StagingArea.create('staging') as staging_area:
            client = lindi.LindiH5pyFile.from_reference_file_system(context.input.get_url(), mode='r+', staging_area=staging_area)

            twophoton_group_keys = _get_twophotonseries_group_keys(client)
            if twophoton_group_keys is None:
                print('No twophoton groups found')

            for key in twophoton_group_keys:
                new_key = key + '_compressed'
                existing = client.get(new_key)
                if existing is not None:
                    print(f'Skipping: {new_key} already exists')
                    continue
                print(f'Processing twophoton group: {key}')
                G = client[key]
                assert isinstance(G, h5py.Group)
                data = G['data']
                assert isinstance(data, h5py.Dataset)
                # conversion = data.attrs['conversion']
                # resolution = data.attrs['resolution']
                if 'starting_time' not in G:
                    print(f'Skipping: {key} does not have a starting_time dataset')
                    continue
                starting_time_dataset = G['starting_time']
                assert isinstance(starting_time_dataset, h5py.Dataset)
                # starting_time = starting_time_dataset[()]
                rate = starting_time_dataset.attrs['rate']
                rate = float(rate)  # type: ignore
                for k, v in data.attrs.items():
                    print(f'{k}: {v}')
                first_chunk = data[0:50]
                assert isinstance(first_chunk, np.ndarray)
                max_val = np.max(first_chunk)
                max_val = float(max_val)  # type: ignore
                normalization_factor = 255 / max_val
                num_timepoints_per_chunk = 500
                chunk_size = [num_timepoints_per_chunk, data.shape[1], data.shape[2]]
                G2 = client.create_group(new_key)
                for k, v in G.attrs.items():
                    if k != 'object_id':
                        G2.attrs[k] = v
                G2.attrs['object_id'] = str(uuid.uuid4())
                for k in G.keys():
                    if k != 'data':
                        client.copy(key + '/' + k, client, new_key + '/' + k)
                codec = MP4AVCCodec(fps=rate)
                G2.create_dataset_with_zarr_compressor('data', shape=data.shape, chunks=chunk_size, dtype=np.uint8, compressor=codec)
                timer = 0
                for i in range(0, data.shape[0], num_timepoints_per_chunk):
                    elapsed = time.time() - timer
                    if elapsed > 3:
                        pct_complete = i / data.shape[0]
                        timer = time.time()
                        print(f'{pct_complete * 100:.1f}% complete')
                    chunk = data[i:i + num_timepoints_per_chunk]
                    G2['data'][i:i + num_timepoints_per_chunk] = (chunk * normalization_factor).astype(np.uint8)

            output_path = 'output.lindi.json'

            def on_store_blob(filename: str):
                url = kcl.store_file(filename)
                return url

            def on_store_main(filename: str):
                shutil.copyfile(filename, output_path)
                return output_path

            staging_store = client.staging_store
            assert staging_store is not None
            print('Uploading supporting files')
            staging_store.upload(
                on_store_blob=on_store_blob,
                on_store_main=on_store_main
            )

            print('Uploading output file')
            context.output.upload(output_path)


def _get_twophotonseries_group_keys(x, prefix: str = ''):
    import h5py
    grp: h5py.Group = x
    keys = []
    if grp.attrs.get('neurodata_type', None) == 'TwoPhotonSeries':
        keys.append(prefix)
    for key in grp.keys():
        a = grp[key]
        if isinstance(a, h5py.Group):
            keys0 = _get_twophotonseries_group_keys(a, prefix=_join(prefix, key))
            keys.extend(keys0)
    return keys


def _join(prefix: str, key: str):
    if not prefix:
        return key
    return prefix + '/' + key
