variable "location" {
  type    = string
  default = "central us"
}

variable "member_count" {
  type    = number
  default = "2"
}

variable "hazelcast_mancenter_version" {
  type    = string
}

variable "azure_ssh_user" {
  type    = string
  default = "ubuntu"
}

variable "azure_instance_type" {
  type    = string
  default = "Standard_B1ms"
}

variable "azure_tag_key" {
  type    = string
  default = "integration-test"
}

variable "azure_tag_value" {
  type    = string
  default = "terraform"
}

variable "hazelcast_path" {
  type    = string
}
