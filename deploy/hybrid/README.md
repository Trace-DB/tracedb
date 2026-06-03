# TraceDB Hybrid AWS + Railway Deployment Notes

Hybrid strategy:

- **AWS** is the durable lane for the TraceDB engine, EBS-backed state, science
  hosts, backup/restore drills, and S3 evidence capture.
- **Railway** remains the simple lane for gateway and small microservices when
  fast public ingress or service iteration matters more than owning durable
  engine storage there.

This document is a boundary guide, not production runtime code.

## Reference shape

```text
Client
  -> Railway public gateway
  -> private/authenticated engine URL
  -> AWS EC2 TraceDB engine
  -> EBS mounted at /data, TraceDB data at /data/tracedb
```

For AWS-only alpha validation, Caddy on the EC2 host can front the gateway or a
temporary diagnostic engine endpoint. For hybrid validation, prefer Railway as
public ingress and keep the AWS engine reachable only through a private network,
VPN, tunnel, IP allowlist, or private load balancer path appropriate for the
experiment.

## Responsibility split

| Concern | AWS lane | Railway lane |
| --- | --- | --- |
| TraceDB engine data | Owns `/data/tracedb` on EBS | Not primary durable engine state in the hybrid lane |
| Public HTTP ingress | Optional Caddy/TLS for AWS-only alpha | Preferred gateway/public API lane |
| Gateway auth/routing | Can run locally for AWS-only smoke | Preferred for simple hosted-alpha ingress |
| Workers | Optional, private, call engine API | Optional simple microservice lane |
| Backups/evidence | S3 evidence bucket, EBS snapshots, restore drills | Can trigger/control, but does not define engine DR |
| Science/bench hosts | Dedicated EC2 instances such as `m7i.2xlarge`+ | Lightweight orchestration only |

## Engine connectivity

Use one of these, in order of preference for serious validation:

1. Private connectivity between Railway and AWS if available for the lane.
2. AWS private load balancer plus a controlled network path.
3. Public HTTPS endpoint with strict security-group allowlist and gateway-to-engine
   internal token, only for bounded alpha experiments.
4. Temporary direct public engine exposure for diagnostics only; remove it before
   calling the environment gateway-fronted or hosted-alpha.

The gateway must send the shared engine token expected by the engine. Store the
token in Railway variables and AWS Secrets Manager/SSM Parameter Store. Do not
commit it to env files.

## Backup and evidence boundaries

AWS owns durable evidence for this strategy:

- EBS volume IDs, instance IDs, image digests, deploy manifests;
- local backup tarballs/checksums from `deploy/aws/scripts/backup-snapshot.sh`;
- S3 evidence bucket copies of backup artifacts and smoke results;
- restore notes and post-restore health checks.

Railway logs and deploy metadata are useful gateway evidence, but they are not a
replacement for AWS engine backup/restore evidence.

## Claim boundaries

This hybrid shape can support alpha evidence that TraceDB runs with a durable AWS
engine and a simpler Railway gateway. It does not, by itself, claim:

- managed multi-tenant service readiness;
- production SLOs or cross-region DR;
- active/active or multi-writer behavior;
- S3 as hot WAL or active mutable index storage;
- TraceField runtime behavior;
- Agent Memory Flight Recorder behavior;
- tensor artifact infrastructure.

Keep public messaging precise: "AWS-backed alpha engine with Railway gateway"
is acceptable after smoke/restore evidence exists. Avoid broader managed-service
or science-runtime claims until those lanes have separate evidence.
