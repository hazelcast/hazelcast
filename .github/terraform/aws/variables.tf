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
}

variable "hazelcast_path" {
  type    = string
}
