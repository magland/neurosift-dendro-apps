#!/usr/bin/env python


from dendro.sdk import App
from autocorrelograms.autocorrelograms import AutocorrelogramsProcessor
from compressed_videos.compressed_videos import CompressedVideosProcessor

app = App(
    name="neurosift-1",
    description="Miscellaneous processors for neurosift",
    app_image="ghcr.io/magland/neurosift-1:latest",
    app_executable="/app/main.py",
)


app.add_processor(AutocorrelogramsProcessor)
app.add_processor(CompressedVideosProcessor)

if __name__ == "__main__":
    app.run()
