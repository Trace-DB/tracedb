# AWS Remote Storage Baseline

This directory contains public Apache-licensed AWS deployment primitives for
TraceDB remote durable storage.

The baseline is intentionally self-hostable. TraceDB Cloud uses the same public
runtime interfaces and should not require private database behavior.

## Resources

`cloud-storage-baseline.yaml` creates:

- One private, encrypted, versioned S3 bucket for WAL chunks, checkpoints,
  manifests, lease records, segments, snapshots, and exports.
- One IAM managed policy for runtime storage access.

The baseline intentionally does not require DynamoDB, Postgres, or any other
external database for TraceDB runtime correctness. S3-compatible object storage
is the durable remote backend; local or mounted storage remains first-class for
self-hosted deployments.

## Deploy

Run this from a remote/CI environment, not the local coordination workstation:

```bash
aws cloudformation deploy \
  --stack-name tracedb-storage-hosted-alpha \
  --template-file deploy/aws/cloud-storage-baseline.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    EnvironmentName=hosted-alpha \
    BucketName=tracedb-cloud-us-west-2-<account-id>
```

Create one stack per primary region while v0.1.1 remains single-writer and
region-pinned.

## Runtime mapping

Use the stack outputs to configure `tracedb-remote-storage`:

```text
bucket=<ObjectBucketName>
object_prefix=<environment-or-tenant-prefix>
region=<aws-region>
endpoint_url=<optional-minio-or-s3-compatible-endpoint>
force_path_style=<true-for-many-minio-deployments>
```

## Validation

Remote validation must prove:

- S3 object put/get/delete.
- S3 object head/metadata reads for CAS inputs.
- Manifest compare-and-swap success and conflict using conditional writes.
- Lease acquire, refresh, conditional release, and conflict using
  storage-backed fencing.
- Engine restart rehydrates from AWS-backed state.
