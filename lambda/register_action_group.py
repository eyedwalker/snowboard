"""
Register the Snowflake Action Group with an existing Bedrock Agent.

Usage:
    python lambda/register_action_group.py --agent-id YOUR_AGENT_ID --lambda-arn YOUR_LAMBDA_ARN

This script:
1. Creates the action group on the Bedrock agent
2. Uploads the OpenAPI schema
3. Prepares a new agent version
"""

import argparse
import json
import boto3
import os


def register_action_group(agent_id: str, lambda_arn: str, region: str = "us-west-2"):
    """Register the Snowflake analytics action group with a Bedrock agent."""
    client = boto3.client("bedrock-agent", region_name=region)

    # Load the OpenAPI schema
    schema_path = os.path.join(os.path.dirname(__file__), "openapi_schema.json")
    with open(schema_path) as f:
        schema = f.read()

    print(f"Registering action group on agent {agent_id}...")

    # Create the action group
    response = client.create_agent_action_group(
        agentId=agent_id,
        agentVersion="DRAFT",
        actionGroupName="snowflake-analytics",
        description="Query VSP Snowflake for eyecare analytics: patients, revenue, billing, appointments, products, insurance, and office metrics.",
        actionGroupExecutor={"lambda": lambda_arn},
        apiSchema={"payload": schema},
        actionGroupState="ENABLED",
    )

    action_group_id = response["agentActionGroup"]["actionGroupId"]
    print(f"Action group created: {action_group_id}")

    # Prepare a new agent version
    print("Preparing agent version...")
    prep_response = client.prepare_agent(agentId=agent_id)
    print(f"Agent status: {prep_response['agentStatus']}")

    print("\nDone! The action group is now available on your Bedrock agent.")
    print(f"Action Group ID: {action_group_id}")
    print(f"Agent ID: {agent_id}")

    return action_group_id


def main():
    parser = argparse.ArgumentParser(description="Register Snowflake Action Group with Bedrock Agent")
    parser.add_argument("--agent-id", required=True, help="Bedrock Agent ID")
    parser.add_argument("--lambda-arn", required=True, help="Lambda function ARN")
    parser.add_argument("--region", default="us-west-2", help="AWS region")
    args = parser.parse_args()

    register_action_group(args.agent_id, args.lambda_arn, args.region)


if __name__ == "__main__":
    main()
