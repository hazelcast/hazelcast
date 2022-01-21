# existing key pair name to be assigned to EC2 instance
variable "aws_key_name" {
  type    = string
  default = "id_rsa"
}

# local path of pem file for SSH connection - local_key_path/aws_key_name.pem
variable "local_key_path" {
  type    = string
  default = "~/.ssh/"
}

variable "username" {
  default = "ubuntu"
}

variable "member_count" {
  default = "2"
}

variable "aws_instance_type" {
  type    = string
  default = "t2.micro"
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "aws_tag_key" {
  type    = string
  default = "Category"
}

variable "aws_tag_value" {
  type    = string
  default = "hazelcast-aws-discovery"
}

variable "hazelcast_mancenter_version" {
  type    = string
  default = "4.2020.08"
}

variable "prefix" {
  type = string
}
