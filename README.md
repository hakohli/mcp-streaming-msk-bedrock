# MCP Streaming Demo — Amazon MSK + Bedrock Agents

A working demo of **Streaming MCP (Model Context Protocol)** on AWS — pushing real-time context to a Bedrock-powered AI agent using Amazon MSK, WebSockets, and IAM authorization.

Based on [mcp-streaming-demo](https://github.com/hakohli/mcp-streaming-demo), adapted to use managed AWS services.

## Architecture

```
log_simulator.py → Amazon MSK (app-logs) → streaming_mcp_server.py → bedrock_agent.py
                                                   ↕ WebSocket              ↕
                                          subscribe / get_anomalies    Bedrock Claude
                                                                      (invoke_model)

Lambda (Action Group) ← Bedrock Agent (managed) → MSK
```

## Components

| File | Description |
|------|-------------|
| `src/streaming_mcp_server.py` | Core server — MSK consumer (IAM auth) + WebSocket push |
| `src/bedrock_agent.py` | AI agent — subscribes to stream, invokes Bedrock for analysis |
| `src/log_simulator.py` | Produces realistic log events to MSK |
| `src/mock_kafka.py` | Patches for local dev without IAM auth |
| `lambda/handler.py` | Bedrock Agent Action Group Lambda |
| `lambda/api_schema.json` | OpenAPI schema for action group |
| `infra/template.yaml` | CloudFormation — MSK Serverless + Bedrock Agent + Lambda |
| `docker-compose.yml` | Local Kafka for development |

## Prerequisites

- Python 3.9+
- AWS account with Bedrock model access (Claude 3 Sonnet)
- AWS CLI configured with appropriate credentials
- Docker (for local development)

## Quick Start (Local Dev)

```bash
# 1. Start local Kafka
docker compose up -d

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start server (local mode)
MSK_BOOTSTRAP=localhost:9092 python src/streaming_mcp_server.py

# 4. Start log simulator (separate terminal)
MSK_BOOTSTRAP=localhost:9092 python src/log_simulator.py

# 5. Start Bedrock agent (separate terminal)
python src/bedrock_agent.py
```

## Deploy to AWS

```bash
# 1. Deploy infrastructure
aws cloudformation deploy \
  --template-file infra/template.yaml \
  --stack-name mcp-streaming-demo \
  --parameter-overrides VpcId=vpc-xxx SubnetIds=subnet-aaa,subnet-bbb \
  --capabilities CAPABILITY_NAMED_IAM

# 2. Get MSK bootstrap servers
aws kafka get-bootstrap-brokers --cluster-arn <ClusterArn from outputs>

# 3. Store bootstrap in SSM (used by Lambda)
aws ssm put-parameter --name /mcp-demo/msk-bootstrap --value "<bootstrap-servers>" --type String

# 4. Run server and simulator with MSK endpoint
export MSK_BOOTSTRAP="<bootstrap-servers>"
python src/streaming_mcp_server.py
python src/log_simulator.py
python src/bedrock_agent.py
```

## Key Differences from Original

| Original | This Version |
|----------|-------------|
| Local Kafka (Docker) | Amazon MSK Serverless |
| Custom Python agent | Bedrock Agent + direct `invoke_model` client |
| No auth | IAM OAUTHBEARER (MSK) + IAM roles (Bedrock) |
| Manual remediation mapping | Claude-powered root cause analysis |
| docker-compose only | CloudFormation IaC |

## What You'll See

1. Normal log traffic flowing through the agent in real-time
2. Every ~30 seconds, an error spike hits
3. The agent detects the spike (>30% error rate in sliding window)
4. Bedrock Claude analyzes the errors and provides root cause + remediation

## Teardown

```bash
# Local
docker compose down -v

# AWS
aws cloudformation delete-stack --stack-name mcp-streaming-demo
```
