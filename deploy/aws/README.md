# TraceDB AWS EC2/EBS Alpha Runtime

This directory contains deployment assets for the first AWS lane: a conservative
single-host EC2 runtime with durable EBS storage mounted at `/data` and TraceDB
state under `/data/tracedb`.

This is an alpha/lab runbook, not a managed-service promise. AWS is the durable
engine/science/backups lane. Railway can remain the simpler public gateway and
microservice lane; see `../hybrid/README.md`.

## Deployment shape

- One EC2 instance runs the TraceDB engine container and owns the EBS volume.
- EBS is mounted at `/data`; TraceDB data lives at `/data/tracedb`.
- The engine should be private to the instance/VPC. Expose the gateway through
  Caddy/TLS when public ingress is needed.
- Deploy a pinned GHCR image. Prefer an immutable digest over a mutable tag.
- Use IAM roles, SSM Session Manager, and Secrets Manager/SSM Parameter Store
  instead of SSH keys and checked-in secrets.
- Keep backups/export evidence in S3. Do not treat S3 as hot WAL or active
  mutable index storage.

## Recommended EC2 sizes

Start smaller for the alpha engine, then size science hosts separately.

| Role | Starting size | Notes |
| --- | --- | --- |
| Alpha engine | `t4g.medium` or `m7a.large` class | Good first lane for correctness, durability, smoke testing, and low-volume hosted-alpha traffic. Use ARM (`t4g`) only if the published image supports it. |
| Heavier engine validation | `m7a.large`, `m7i.large`, `m7i.xlarge` | Prefer modern general-purpose instances with EBS optimization. Increase memory before increasing replicas; the current lane is single-writer/single-volume. |
| Science/benchmark host | `m7i.2xlarge` or larger | Run bounded jobs, benchmark orchestration, or data-generation away from the engine host when possible. |
| Larger science host | `m7i.4xlarge`, `r7i.2xlarge`+ | Choose compute-heavy vs memory-heavy based on workload. Keep science claims separate from hosted-alpha runtime claims. |

Use gp3 EBS by default. For early alpha, start with enough headroom for data,
checkpoints, logs, local backup tarballs, and restore tests. Tune gp3 IOPS and
throughput if measurements show storage bottlenecks.

## EBS mount and data directory

Example first-time host setup:

```sh
sudo file -s /dev/nvme1n1
sudo mkfs.xfs /dev/nvme1n1
sudo mkdir -p /data
sudo mount /dev/nvme1n1 /data
sudo mkdir -p /data/tracedb /data/tracedb-backups
sudo chown -R 10001:10001 /data/tracedb
```

Persist the mount in `/etc/fstab` using the filesystem UUID, not the transient
NVMe device name:

```sh
sudo blkid /dev/nvme1n1
# Add a line like this to /etc/fstab:
# UUID=<volume-uuid> /data xfs defaults,nofail 0 2
sudo mount -a
```

Confirm before starting TraceDB:

```sh
findmnt /data
df -h /data
ls -ld /data/tracedb
```

## Image pinning

The compose file defaults to:

```sh
ghcr.io/trace-db/tracedb:v0.1.0
```

For real deployments, prefer a digest-pinned image:

```sh
export TRACEDB_IMAGE='ghcr.io/trace-db/tracedb@sha256:<digest>'
docker compose -f deploy/aws/docker-compose.yml --profile gateway up -d
```

Record the digest, deployment time, instance ID, EBS volume ID, and smoke-test
results in the S3 evidence bucket for each deployment.

## Secrets, IAM, and SSM

Do not commit secrets. The example env files in this directory are placeholders
only.

Recommended AWS controls:

- Attach an EC2 instance profile with least-privilege access to:
  - the S3 evidence/backup bucket prefixes this host needs;
  - SSM Session Manager;
  - CloudWatch logs/metrics if enabled;
  - Secrets Manager or SSM Parameter Store paths for TraceDB tokens.
- Prefer SSM Session Manager over public SSH. If SSH is temporarily needed,
  restrict it to a trusted admin CIDR and remove it after bootstrap.
- Store `TRACEDB_ENGINE_INTERNAL_TOKEN`, `TRACEDB_API_TOKEN`, and any catalog,
  queue, or S3 credentials in Secrets Manager/SSM Parameter Store or a local
  root-readable env file such as `/etc/tracedb/env.engine`.
- Keep env files mode `0600` and owned by root or the deployment user.

## Security group and TLS guidance

Baseline security group:

- Inbound `80/tcp` and `443/tcp` only when Caddy/public ingress is enabled.
- Inbound `22/tcp` disabled by default; use SSM Session Manager.
- No public inbound access to engine port `8080` or local host port `18081`.
- Restrict any temporary diagnostic port to a known admin CIDR and remove it.
- Allow outbound HTTPS for image pulls, SSM, CloudWatch, S3, and ACME.

Caddy can terminate TLS for `api.trace-db.com` and reverse proxy to the gateway
(`127.0.0.1:18080`) or, for a temporary diagnostic-only lane, directly to the
engine (`127.0.0.1:18081`). See `Caddyfile`.

DNS must point the public hostname at the EC2 instance or load balancer before
Caddy can complete ACME certificate issuance. For production-like lanes, prefer
public ingress through the gateway rather than direct engine exposure.

## Starting the runtime

From the repository root inside `tracedb`:

```sh
# Engine only, private/local health port.
TRACEDB_IMAGE='ghcr.io/trace-db/tracedb@sha256:<digest>' \
TRACEDB_DATA_DIR_HOST=/data/tracedb \
docker compose -f deploy/aws/docker-compose.yml up -d tracedb-engine

# Engine + gateway + Caddy public ingress.
TRACEDB_IMAGE='ghcr.io/trace-db/tracedb@sha256:<digest>' \
TRACEDB_DATA_DIR_HOST=/data/tracedb \
docker compose -f deploy/aws/docker-compose.yml --profile gateway up -d

# Optional worker lane when the workload requires it.
docker compose -f deploy/aws/docker-compose.yml --profile worker up -d tracedb-worker
```

The compose file reads `env.engine.example`, `env.gateway.example`, and
`env.worker.example` as example shape only. Copy them to root-readable host env
files or inject equivalent variables through your deployment system before a real
deployment.

## Post-deploy smoke commands

Run from the EC2 host:

```sh
./deploy/aws/scripts/healthcheck.sh http://127.0.0.1:18081
curl -fsS http://127.0.0.1:18081/v1/health
curl -fsS http://127.0.0.1:18081/v1/ready
```

If gateway/Caddy is enabled:

```sh
./deploy/aws/scripts/healthcheck.sh http://127.0.0.1:18080
curl -fsS https://api.trace-db.com/v1/health
curl -fsS https://api.trace-db.com/v1/ready
```

If the gateway requires an API token, include the expected bearer header for
application routes. Keep health/ready policy explicit for the lane you deploy.

## Backup, restore, and evidence boundaries

`deploy/aws/scripts/backup-snapshot.sh` creates a local tarball and SHA-256
checksum of `/data/tracedb`. This is an ops-level helper for evidence capture,
manual snapshots, and restore drills. It is not managed disaster recovery, not a
continuous backup system, and not a cross-region RPO/RTO guarantee.

Recommended evidence bucket layout:

```text
s3://<evidence-bucket>/tracedb/aws-alpha/
  deployments/<yyyy-mm-dd>/<instance-id>/manifest.txt
  smoke/<yyyy-mm-dd>/<instance-id>/health.json
  backups/<yyyy-mm-dd>/<instance-id>/tracedb-data-<timestamp>.tar.gz
  backups/<yyyy-mm-dd>/<instance-id>/tracedb-data-<timestamp>.tar.gz.sha256
  restores/<yyyy-mm-dd>/<instance-id>/restore-notes.txt
```

Upload backup artifacts after creating them:

```sh
./deploy/aws/scripts/backup-snapshot.sh /data/tracedb-backups /data/tracedb
aws s3 cp /data/tracedb-backups/ s3://<evidence-bucket>/tracedb/aws-alpha/backups/<date>/<instance-id>/ --recursive --exclude '*' --include '*.tar.gz' --include '*.sha256'
```

Restore only into an empty target directory and only after stopping the engine:

```sh
docker compose -f deploy/aws/docker-compose.yml stop tracedb-engine
sudo mkdir -p /data/tracedb-restore
./deploy/aws/scripts/restore-snapshot.sh /data/tracedb-backups/tracedb-data-<timestamp>.tar.gz /data/tracedb-restore
```

For an in-place restore, move aside the old data directory first, then restore
into a newly created empty `/data/tracedb`. Record restore steps and smoke-test
results in S3.

## Claim boundaries

This AWS lane proves durable single-host engine operations, EBS handling,
backup/restore drills, and evidence capture. It does not by itself claim:

- managed multi-tenant service readiness;
- multi-writer or cross-volume correctness;
- cross-region disaster recovery;
- production SLOs;
- TraceField runtime behavior;
- Agent Memory Flight Recorder behavior;
- tensor artifact infrastructure.
