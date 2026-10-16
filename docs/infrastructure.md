# Infrastructure

# Configure IAM and Create Access Keys

```bash
export AWS_ACCESS_KEY_ID="AKIAxxxxxxxxxxxx"
export AWS_SECRET_ACCESS_KEY="your-secret-key-here"
export AWS_DEFAULT_REGION="us-east-1"  # Or your preferred region

```

## Create Directory Structure

```bash
mkdir pulumi-bench-infra && cd pulumi-bench-infra

# Tell Pulumi to store state files locally on your machine
pulumi login --local

pulumi new aws-python

```

## Generate custom SSH keys

```bash
ssh-keygen -t ed25519 -f ~/.ssh/bench_key -C "pulumi-ec2-benchmark" -N ""

```

## Pulumi deployment code

```python
import os
import pulumi
import pulumi_aws as aws

# Config: Adjust instance type if needed
INSTANCE_TYPE = "c6i.xlarge"  # Network-optimized compute instance

# --- SSH KEY PAIR SETUP ---
# Replace with the path to your SSH public key (~/.ssh/id_rsa.pub or ~/.ssh/id_ed25519.pub)
PUBLIC_KEY_PATH = os.path.expanduser("~/.ssh/bench_key.pub")

with open(PUBLIC_KEY_PATH, "r") as f:
    public_key_content = f.read().strip()

deployer_key = aws.ec2.KeyPair(
    "bench-deployer-key",
    public_key=public_key_content,
)

# --- NETWORK & VPC LOOKUP ---
default_vpc = aws.ec2.get_vpc(default=True)
default_subnets = aws.ec2.get_subnets(
    filters=[{"name": "vpc-id", "values": [default_vpc.id]}]
)

# Select the first subnet for all 3 instances to keep them in the same AZ
target_subnet_id = default_subnets.ids[0]

# --- UBUNTU 24.04 LTS AMI ---
ubuntu_ami = aws.ec2.get_ami(
    most_recent=True,
    owners=["099720109477"],  # Canonical
    filters=[
        {
            "name": "name",
            "values": ["ubuntu/images/hvm-ssd/ubuntu-noble-24.04-amd64-server-*"],
        },
        {"name": "virtualization-type", "values": ["hvm"]},
    ],
)

# --- SECURITY GROUP ---
bench_sg = aws.ec2.SecurityGroup(
    "bench-sg",
    vpc_id=default_vpc.id,
    description="Security group for benchmark instances",
    ingress=[
        # SSH access from anywhere
        aws.ec2.SecurityGroupIngressArgs(
            protocol="tcp",
            from_port=22,
            to_port=22,
            cidr_blocks=["0.0.0.0/0"],
        ),
    ],
    egress=[
        aws.ec2.SecurityGroupEgressArgs(
            protocol="-1",
            from_port=0,
            to_port=0,
            cidr_blocks=["0.0.0.0/0"],
        )
    ],
)

# Allow all traffic between instances attached to this specific Security Group
sg_self_ingress = aws.ec2.SecurityGroupRule(
    "allow-internal-bench-traffic",
    type="ingress",
    from_port=0,
    to_port=0,
    protocol="-1",
    security_group_id=bench_sg.id,
    source_security_group_id=bench_sg.id,  # Self-referencing rule
)

# --- COMMON KERNEL TUNING USERDATA ---
sysctl_tuning = """#!/bin/bash
# Kernel tuning for high-throughput socket recycling
sudo sysctl -w net.core.somaxconn=65535
sudo sysctl -w net.ipv4.ip_local_port_range="1024 65535"
sudo sysctl -w net.ipv4.tcp_tw_reuse=1
echo "* soft nofile 65535" | sudo tee -a /etc/security/limits.conf
echo "* hard nofile 65535" | sudo tee -a /etc/security/limits.conf
ulimit -n 65535
"""

# -------------------------------------------------------------
# TARGET 1: Nginx Server
# -------------------------------------------------------------
nginx_userdata = f"""{sysctl_tuning}
sudo apt-get update -y
sudo apt-get install -y nginx
sudo sed -i 's/worker_connections 768;/worker_connections 65535;/' /etc/nginx/nginx.conf
sudo systemctl restart nginx
"""

target_nginx_server = aws.ec2.Instance(
    "target-nginx-server",
    instance_type=INSTANCE_TYPE,
    ami=ubuntu_ami.id,
    key_name=deployer_key.key_name,
    subnet_id=target_subnet_id,
    vpc_security_group_ids=[bench_sg.id],
    user_data=nginx_userdata,
    tags={"Name": "1-Target-Nginx"},
)

# -------------------------------------------------------------
# CLIENT 1: Grafana k6
# -------------------------------------------------------------
k6_userdata = f"""{sysctl_tuning}
sudo apt-get update -y
sudo apt-get install -y gpg curl time
# Install k6
curl -fsSL https://dl.k6.io/key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/k6-archive-keyring.gpg
sudo chmod 644 /usr/share/keyrings/k6-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" | sudo tee /etc/apt/sources.list.d/k6.list
sudo apt-get update -y && sudo apt-get install -y k6
k6 version
"""

client_k6_server = aws.ec2.Instance(
    "client-k6-server",
    instance_type=INSTANCE_TYPE,
    ami=ubuntu_ami.id,
    key_name=deployer_key.key_name,
    subnet_id=target_subnet_id,
    vpc_security_group_ids=[bench_sg.id],
    user_data=k6_userdata,
    tags={"Name": "2-Client-k6"},
)

# -------------------------------------------------------------
# CLIENT 2: Strobengine (Our Project)
# -------------------------------------------------------------
strobengine_userdata = f"""{sysctl_tuning}
sudo apt-get update -y
sudo apt-get install -y python3 python3-pip git build-essential time
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh
uv --version
uv init se
cd se
uv add strobengine
"""

client_strobengine_server = aws.ec2.Instance(
    "client-strobengine-server",
    instance_type=INSTANCE_TYPE,
    ami=ubuntu_ami.id,
    key_name=deployer_key.key_name,
    subnet_id=target_subnet_id,
    vpc_security_group_ids=[bench_sg.id],
    user_data=strobengine_userdata,
    tags={"Name": "3-Client-Strobengine"},
)

# -------------------------------------------------------------
# OUTPUTS
# -------------------------------------------------------------
pulumi.export("Target_Nginx_Private_IP", target_nginx_server.private_ip)
pulumi.export("Target_Nginx_Public_IP", target_nginx_server.public_ip)

pulumi.export("Client_k6_Public_IP", client_k6_server.public_ip)
pulumi.export("Client_Strobengine_Public_IP", client_strobengine_server.public_ip)
```

```bash
pulumi preview

```

Deploy

```bash
pulumi up

```

# Run the tests

```bash
export NGINX_PRIVATE_IP=$(pulumi stack output Target_Nginx_Private_IP)
export K6_PUBLIC_IP=$(pulumi stack output Client_k6_Public_IP)
export STROBENGINE_PUBLIC_IP=$(pulumi stack output Client_Strobengine_Public_IP)

```

## Running k6 test using the dedicated key

```bash
ssh -i ~/.ssh/bench_key ubuntu@$K6_PUBLIC_IP "/usr/bin/time -v k6 run - --vus 300 --duration 10s <<< 'import http from \"k6/http\"; export default function() { http.get(\"http://$NGINX_PRIVATE_IP/\"); }'"

```

## Running strobengine test using the dedicated key

```bash
ssh -i ~/.ssh/bench_key ubuntu@$STROBENGINE_PUBLIC_IP "/usr/bin/time -v uv run strobengine http://$NGINX_PRIVATE_IP/ -c 300 -d 10"

```

# Clean up the infrastructure 

```bash
pulumi destroy --yes

```
