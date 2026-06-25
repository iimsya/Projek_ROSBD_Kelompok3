import io
import json
import os
import tarfile
import tempfile
import shutil
from datetime import datetime, timezone

from minio import Minio

import config


def get_client() -> Minio:
    return Minio(
        config.MINIO_ENDPOINT,
        access_key=config.MINIO_ACCESS_KEY,
        secret_key=config.MINIO_SECRET_KEY,
        secure=False,
    )


def ensure_bucket(client: Minio):
    if not client.bucket_exists(config.MINIO_BUCKET):
        client.make_bucket(config.MINIO_BUCKET)


def version_tag() -> str:
    return "v_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _tar_dir(src_dir: str) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        tar.add(src_dir, arcname=os.path.basename(src_dir))
    return buf.getvalue()


def _extract_tar(data: bytes, dst_dir: str):
    buf = io.BytesIO(data)
    with tarfile.open(fileobj=buf, mode="r:gz") as tar:
        tar.extractall(path=dst_dir)


def upload_model(client: Minio, local_path: str, version: str):
    data = _tar_dir(local_path)
    obj_name = f"{version}/model.tar.gz"
    client.put_object(config.MINIO_BUCKET, obj_name, io.BytesIO(data), len(data))
    print(f"  Uploaded model → {config.MINIO_BUCKET}/{obj_name}")


def upload_features(client: Minio, local_path: str, version: str):
    obj_name = f"{version}/features.json"
    client.fput_object(config.MINIO_BUCKET, obj_name, local_path)
    print(f"  Uploaded features → {config.MINIO_BUCKET}/{obj_name}")


def upload_metrics(client: Minio, metrics: dict, version: str):
    data = json.dumps(metrics, indent=2).encode()
    obj_name = f"{version}/metrics.json"
    client.put_object(config.MINIO_BUCKET, obj_name, io.BytesIO(data), len(data))
    print(f"  Uploaded metrics → {config.MINIO_BUCKET}/{obj_name}")


def write_tag(client: Minio, tag: str, value: str):
    data = value.encode()
    client.put_object(config.MINIO_BUCKET, tag, io.BytesIO(data), len(data))


def read_tag(client: Minio, tag: str) -> str | None:
    try:
        resp = client.get_object(config.MINIO_BUCKET, tag)
        val = resp.read().decode().strip()
        resp.close()
        return val
    except:
        return None


def read_metrics(client: Minio, version: str) -> dict | None:
    try:
        resp = client.get_object(config.MINIO_BUCKET, f"{version}/metrics.json")
        metrics = json.loads(resp.read())
        resp.close()
        return metrics
    except:
        return None


def download_model(client: Minio, version: str, dst_dir: str):
    obj_name = f"{version}/model.tar.gz"
    resp = client.get_object(config.MINIO_BUCKET, obj_name)
    data = resp.read()
    resp.close()
    if os.path.exists(dst_dir):
        shutil.rmtree(dst_dir)
    _extract_tar(data, os.path.dirname(dst_dir))
    print(f"  Downloaded model → {dst_dir}")


def download_features(client: Minio, version: str, dst_path: str):
    obj_name = f"{version}/features.json"
    client.fget_object(config.MINIO_BUCKET, obj_name, dst_path)
    print(f"  Downloaded features → {dst_path}")


def get_latest_version(client: Minio) -> str | None:
    return read_tag(client, "LATEST")


def get_best_version(client: Minio) -> str | None:
    return read_tag(client, "BEST")
