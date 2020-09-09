variable "prefix" {
  type    = string
}

# key name to be assigned to Azure Compute instances
variable "azure_key_name" {
  type    = string
  default = "id_rsa"
}

# local path of private and public key file for SSH connection - local_key_path/azure_key_name
variable "local_key_path" {
  type    = string
  default = "~/.ssh"
}

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
  default = "4.2020.08"
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
