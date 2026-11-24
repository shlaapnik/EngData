FROM python:3.13.7-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app
RUN pip install --no-cache-dir boto3==1.43.118 pyarrow==26.0.0 tqdm==4.70.1
COPY MapReduce/common.py MapReduce/mapreduce_job.py Kubernetes/pairs_job.py ./

ENTRYPOINT ["python", "pairs_job.py"]
