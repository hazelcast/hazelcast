terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 3.2"
    }
    tls = {
      source  = "hashicorp/tls"
      version = ">= 3.3.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.1.0"
    }
  }
  required_version = ">= 0.13"
}

provider "aws" {
  region = var.aws_region
}

data "aws_ami" "image" {
  most_recent = true

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-bionic-18.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  owners = ["099720109477"]
}

resource "random_pet" "prefix" {
  prefix    = "test"
  length    = 2
  separator = "_"
}

data "template_file" "hazelcast" {
  template = file("${path.module}/hazelcast.yaml")
  vars = {
    PREFIX    = random_pet.prefix.id
    REGION    = var.aws_region
    TAG_KEY   = var.aws_tag_key
    TAG_VALUE = var.aws_tag_value

  }
}

data "template_file" "hazelcast_client" {
  template = file("${path.module}/hazelcast-client.yaml")
  vars = {
    PREFIX    = random_pet.prefix.id
    REGION    = var.aws_region
    TAG_KEY   = var.aws_tag_key
    TAG_VALUE = var.aws_tag_value
  }
}

data "template_file" "user_data" {
  template = file("${path.module}/cloud-init.yaml")
}

# IAM Role required for Hazelcast AWS Discovery
resource "aws_iam_role" "discovery_role" {
  name = "${random_pet.prefix.id}_discovery_role"

  assume_role_policy = <<-EOF
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Action": "sts:AssumeRole",
        "Principal": {
          "Service": "ec2.amazonaws.com"
        },
        "Effect": "Allow",
        "Sid": ""
      }
    ]
  }
  EOF
}

resource "aws_iam_role_policy" "discovery_policy" {
  name = "${random_pet.prefix.id}_discovery_policy"
  role = aws_iam_role.discovery_role.id

  policy = <<-EOF
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Action": [
          "ec2:DescribeInstances"
        ],
        "Effect": "Allow",
        "Resource": "*"
      }
    ]
  }
  EOF
}


resource "aws_iam_instance_profile" "discovery_instance_profile" {
  name = "${random_pet.prefix.id}_discovery_instance_profile"
  role = aws_iam_role.discovery_role.name
}

resource "aws_security_group" "sg" {
  name = "${random_pet.prefix.id}_sg"

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 5701
    to_port     = 5707
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }


  # Allow outgoing traffic to anywhere.
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "tls_private_key" "ssh" {
  algorithm = "RSA"
  rsa_bits  = "4096"
}

resource "local_file" "private_key" {
  content         = tls_private_key.ssh.private_key_pem
  filename        = "private_key.pem"
  file_permission = "0600"
}

resource "aws_key_pair" "keypair" {
  key_name   = "${random_pet.prefix.id}_aws_key"
  public_key = tls_private_key.ssh.public_key_openssh
}

resource "aws_instance" "hazelcast_member" {
  count                = var.member_count
  ami                  = data.aws_ami.image.id
  instance_type        = var.aws_instance_type
  iam_instance_profile = aws_iam_instance_profile.discovery_instance_profile.name
  security_groups      = [aws_security_group.sg.name]
  key_name             = aws_key_pair.keypair.key_name
  user_data            = data.template_file.user_data.rendered
  tags = {
    Name                 = "${random_pet.prefix.id}-AWS-Member-${count.index}"
    "${var.aws_tag_key}" = var.aws_tag_value
  }
  connection {
    type        = "ssh"
    user        = var.username
    host        = self.public_ip
    timeout     = "180s"
    agent       = false
    private_key = tls_private_key.ssh.private_key_pem
  }

  provisioner "remote-exec" {
    inline = [
      "mkdir -p /home/${var.username}/jars",
      "mkdir -p /home/${var.username}/logs",
      "while [ ! -f /var/lib/cloud/instance/boot-finished ]; do echo 'Waiting for cloud-init...'; sleep 1; done",
    ]
  }

  provisioner "file" {
    source      = "scripts/start_aws_hazelcast_member.sh"
    destination = "/home/${var.username}/start_aws_hazelcast_member.sh"
  }

  provisioner "file" {
    source      = "scripts/verify_member_count.sh"
    destination = "/home/${var.username}/verify_member_count.sh"
  }

  provisioner "file" {
    source      = var.hazelcast_path
    destination = "/home/${var.username}/jars/hazelcast.jar"
  }

  provisioner "file" {
    content     = data.template_file.hazelcast.rendered
    destination = "/home/${var.username}/hazelcast.yaml"
  }

  provisioner "remote-exec" {
    inline = [
      "cd /home/${var.username}",
      "chmod 0755 start_aws_hazelcast_member.sh",
      "./start_aws_hazelcast_member.sh",
    ]
  }
}

resource "null_resource" "verify_members" {
  count      = var.member_count
  depends_on = [aws_instance.hazelcast_member]
  connection {
    type        = "ssh"
    user        = var.username
    host        = aws_instance.hazelcast_member[count.index].public_ip
    timeout     = "180s"
    agent       = false
    private_key = tls_private_key.ssh.private_key_pem
  }


  provisioner "remote-exec" {
    inline = [
      "cd /home/${var.username}",
      "tail -n 20 ./logs/hazelcast.stdout.log",
      "chmod 0755 verify_member_count.sh",
      "./verify_member_count.sh  ${var.member_count}",
    ]
  }
}


resource "aws_instance" "hazelcast_mancenter" {
  ami                  = data.aws_ami.image.id
  instance_type        = var.aws_instance_type
  iam_instance_profile = aws_iam_instance_profile.discovery_instance_profile.name
  security_groups      = [aws_security_group.sg.name]
  key_name             = aws_key_pair.keypair.key_name
  user_data            = data.template_file.user_data.rendered
  tags = {
    Name                 = "${random_pet.prefix.id}-AWS-Management-Center"
    "${var.aws_tag_key}" = var.aws_tag_value
  }

  connection {
    type        = "ssh"
    user        = var.username
    host        = self.public_ip
    timeout     = "180s"
    agent       = false
    private_key = tls_private_key.ssh.private_key_pem
  }

  provisioner "file" {
    source      = "scripts/start_aws_hazelcast_management_center.sh"
    destination = "/home/${var.username}/start_aws_hazelcast_management_center.sh"
  }

  provisioner "file" {
    source      = "scripts/verify_mancenter.sh"
    destination = "/home/${var.username}/verify_mancenter.sh"
  }

  provisioner "file" {
    content     = data.template_file.hazelcast_client.rendered
    destination = "/home/${var.username}/hazelcast-client.yaml"
  }

  provisioner "remote-exec" {
    inline = [
      "while [ ! -f /var/lib/cloud/instance/boot-finished ]; do echo 'Waiting for cloud-init...'; sleep 1; done",
    ]
  }

  provisioner "remote-exec" {
    inline = [
      "cd /home/${var.username}",
      "chmod 0755 start_aws_hazelcast_management_center.sh",
      "./start_aws_hazelcast_management_center.sh ${var.hazelcast_mancenter_version}",
    ]
  }
}

resource "null_resource" "verify_mancenter" {

  depends_on = [aws_instance.hazelcast_member, aws_instance.hazelcast_mancenter]
  connection {
    type        = "ssh"
    user        = var.username
    host        = aws_instance.hazelcast_mancenter.public_ip
    timeout     = "180s"
    agent       = false
    private_key = tls_private_key.ssh.private_key_pem
  }


  provisioner "remote-exec" {
    inline = [
      "cd /home/${var.username}",
      "tail -n 20 ./logs/mancenter.stdout.log",
      "chmod 0755 verify_mancenter.sh",
      "./verify_mancenter.sh  ${var.member_count}",
    ]
  }
}
