import os
from datetime import datetime
from pathlib import Path
import kfp.v2.dsl, kfp.v2.compiler
from google.cloud import aiplatform
from pipeline import my_pipeline

if __name__ == "__main__":

    filename = str(Path(__file__).parent.joinpath("pipeline.json"))
    kfp.v2.compiler.Compiler().compile(my_pipeline, filename)

    run = aiplatform.PipelineJob(
        project=os.environ['PROJECT_ID'],
        location=os.environ['REGION'],
        display_name="test-pipeline",
        template_path=filename,
        job_id=f"test-pipeline-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}",
        enable_caching=True,
        pipeline_root=f"gs://{os.environ['BUCKET_ID']}",
        parameter_values={},
    )

    run.submit(service_account=os.environ["SERVICE_ACCOUNT"])