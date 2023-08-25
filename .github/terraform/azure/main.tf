terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "= 2.23.0"
    }
  }
  required_version = ">= 0.13"
}

# Configure the Microsoft Azure Provider.
provider "azurerm" {
  features {}
}

resource "random_pet" "prefix" {
  prefix    = "test"
  length    = 2
  separator = "-"
}

data "template_file" "hazelcast" {
  template = file("${path.module}/hazelcast.yaml")
  vars = {
    TAG_KEY   = var.azure_tag_key
    TAG_VALUE = var.azure_tag_value
  }
}

data "template_file" "hazelcast_client" {
  template = file("${path.module}/hazelcast-client.yaml")
  vars = {
    TAG_KEY   = var.azure_tag_key
    TAG_VALUE = var.azure_tag_value
  }
}

data "template_file" "cloud_init" {
  template = file("${path.module}/cloud-init.yaml")
}
data "template_cloudinit_config" "config" {
  gzip          = true
  base64_encode = true
  part {
    content_type = "text/cloud-config"
    content      = "${data.template_file.cloud_init.rendered}"
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

# Create a resource group
resource "azurerm_resource_group" "rg" {
  name     = "${random_pet.prefix.id}_rg"
  location = var.location
}

# Create virtual network
resource "azurerm_virtual_network" "vnet" {
  name                = "${random_pet.prefix.id}_vnet"
  address_space       = ["10.0.0.0/16"]
  location            = var.location
  resource_group_name = azurerm_resource_group.rg.name
}

# Create subnet
resource "azurerm_subnet" "subnet" {
  name                 = "${random_pet.prefix.id}_subnet"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.vnet.name
  address_prefixes     = ["10.0.1.0/24"]
}

# Create public IP(s)
resource "azurerm_public_ip" "publicip" {
  count               = var.member_count + 1
  name                = "${random_pet.prefix.id}_publicip_${count.index}"
  location            = var.location
  resource_group_name = azurerm_resource_group.rg.name
  allocation_method   = "Static"
}


data "azurerm_subscription" "primary" {}

# Create user assigned managed identity
resource "azurerm_user_assigned_identity" "hazelcast_reader" {
  resource_group_name = azurerm_resource_group.rg.name
  location            = var.location
  name                = "${random_pet.prefix.id}_reader_identity"
}


resource "azurerm_role_definition" "reader" {
  name  = "${random_pet.prefix.id}_reader_role_definition"
  scope = data.azurerm_subscription.primary.id

  permissions {
    actions = ["Microsoft.Network/networkInterfaces/Read",
      "Microsoft.Network/publicIPAddresses/Read",
    ]
    not_actions = []
  }

  assignable_scopes = [
    data.azurerm_subscription.primary.id
  ]
}


#Assign role to the user assigned managed identity
resource "azurerm_role_assignment" "reader" {
  scope              = data.azurerm_subscription.primary.id
  principal_id       = azurerm_user_assigned_identity.hazelcast_reader.principal_id
  role_definition_id = azurerm_role_definition.reader.id
}

# Create network interface(s)
resource "azurerm_network_interface" "nic" {
  count               = var.member_count + 1
  name                = "${random_pet.prefix.id}_nic_${count.index}"
  location            = var.location
  resource_group_name = azurerm_resource_group.rg.name

  tags = {
    "${var.azure_tag_key}" = var.azure_tag_value
  }
  ip_configuration {
    name                          = "${random_pet.prefix.id}_nicconfig_${count.index}"
    subnet_id                     = azurerm_subnet.subnet.id
    private_ip_address_allocation = "Static"
    private_ip_address            = "10.0.1.${count.index + 10}"
    public_ip_address_id          = azurerm_public_ip.publicip[count.index].id
  }
}

# Create Hazelcast member instances
resource "azurerm_linux_virtual_machine" "hazelcast_member" {
  count                 = var.member_count
  name                  = "${random_pet.prefix.id}-member-${count.index}"
  location              = var.location
  resource_group_name   = azurerm_resource_group.rg.name
  network_interface_ids = [azurerm_network_interface.nic[count.index].id]
  size                  = var.azure_instance_type
  admin_username        = var.azure_ssh_user

  os_disk {
    name                 = "OsDisk_${count.index}"
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
  }

  admin_ssh_key {
    username   = var.azure_ssh_user
    public_key = tls_private_key.ssh.public_key_openssh
  }

  custom_data = data.template_cloudinit_config.config.rendered

  source_image_reference {
    publisher = "Canonical"
    offer     = "UbuntuServer"
    sku       = "18.04-LTS"
    version   = "latest"
  }


  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.hazelcast_reader.id]
  }


  connection {
    host        = azurerm_public_ip.publicip[count.index].ip_address
    user        = var.azure_ssh_user
    type        = "ssh"
    private_key = tls_private_key.ssh.private_key_pem
    timeout     = "120"
    agent       = false
  }

  provisioner "remote-exec" {
    inline = [
      "mkdir -p /home/${var.azure_ssh_user}/jars",
      "mkdir -p /home/${var.azure_ssh_user}/logs",
      "while [ ! -f /var/lib/cloud/instance/boot-finished ]; do echo 'Waiting for cloud-init...'; sleep 10; done",
    ]
  }

  provisioner "file" {
    source      = "scripts/start_azure_hazelcast_member.sh"
    destination = "/home/${var.azure_ssh_user}/start_azure_hazelcast_member.sh"
  }

  provisioner "file" {
    source      = "scripts/verify_member_count.sh"
    destination = "/home/${var.azure_ssh_user}/verify_member_count.sh"
  }

  provisioner "file" {
    source      = var.hazelcast_path
    destination = "/home/${var.azure_ssh_user}/jars/hazelcast.jar"
  }

  provisioner "file" {
    content     = data.template_file.hazelcast.rendered
    destination = "/home/${var.azure_ssh_user}/hazelcast.yaml"
  }

  provisioner "remote-exec" {
    inline = [
      "cd /home/${var.azure_ssh_user}",
      "chmod 0755 start_azure_hazelcast_member.sh",
      "./start_azure_hazelcast_member.sh  ${var.azure_tag_key} ${var.azure_tag_value} ",
      "sleep 5",
    ]
  }
}

resource "null_resource" "verify_members" {
  count      = var.member_count
  depends_on = [azurerm_linux_virtual_machine.hazelcast_member]

  connection {
    type        = "ssh"
    user        = var.azure_ssh_user
    host        = azurerm_linux_virtual_machine.hazelcast_member[count.index].public_ip_address
    timeout     = "180s"
    agent       = false
    private_key = tls_private_key.ssh.private_key_pem
  }

  provisioner "remote-exec" {
    inline = [
      "cd /home/${var.azure_ssh_user}",
      "tail -n 20 ./logs/hazelcast.logs",
      "chmod 0755 verify_member_count.sh",
      "./verify_member_count.sh  ${var.member_count}",
    ]
  }
}


# Create Hazelcast Management Center
resource "azurerm_linux_virtual_machine" "hazelcast_mancenter" {
  name                  = "${random_pet.prefix.id}-mancenter"
  location              = var.location
  resource_group_name   = azurerm_resource_group.rg.name
  network_interface_ids = [azurerm_network_interface.nic[var.member_count].id]
  size                  = "Standard_B1ms"
  admin_username        = var.azure_ssh_user

  os_disk {
    name                 = "OsDisk"
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
  }

  admin_ssh_key {
    username   = var.azure_ssh_user
    public_key = tls_private_key.ssh.public_key_openssh
  }

  custom_data = data.template_cloudinit_config.config.rendered

  source_image_reference {
    publisher = "Canonical"
    offer     = "UbuntuServer"
    sku       = "18.04-LTS"
    version   = "latest"
  }


  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.hazelcast_reader.id]
  }


  connection {
    host        = azurerm_public_ip.publicip[var.member_count].ip_address
    user        = var.azure_ssh_user
    type        = "ssh"
    private_key = tls_private_key.ssh.private_key_pem
    timeout     = "120"
    agent       = false
  }

  provisioner "file" {
    source      = "scripts/start_azure_hazelcast_management_center.sh"
    destination = "/home/${var.azure_ssh_user}/start_azure_hazelcast_management_center.sh"
  }

  provisioner "file" {
    source      = "scripts/verify_mancenter.sh"
    destination = "/home/${var.azure_ssh_user}/verify_mancenter.sh"
  }

  provisioner "file" {
    content     = data.template_file.hazelcast_client.rendered
    destination = "/home/${var.azure_ssh_user}/hazelcast-client.yaml"
  }

  provisioner "remote-exec" {
    inline = [
      "while [ ! -f /var/lib/cloud/instance/boot-finished ]; do echo 'Waiting for cloud-init...'; sleep 10; done",
    ]
  }

  provisioner "remote-exec" {
    inline = [
      "cd /home/${var.azure_ssh_user}",
      "chmod 0755 start_azure_hazelcast_management_center.sh",
      "./start_azure_hazelcast_management_center.sh ${var.hazelcast_mancenter_version} ${var.azure_tag_key} ${var.azure_tag_value} ",
    ]
  }
}


resource "null_resource" "verify_mancenter" {

  depends_on = [azurerm_linux_virtual_machine.hazelcast_member, azurerm_linux_virtual_machine.hazelcast_mancenter]
  connection {
    type        = "ssh"
    user        = var.azure_ssh_user
    host        = azurerm_linux_virtual_machine.hazelcast_mancenter.public_ip_address
    timeout     = "180s"
    agent       = false
    private_key = tls_private_key.ssh.private_key_pem
  }


  provisioner "remote-exec" {
    inline = [
      "cd /home/${var.azure_ssh_user}",
      "tail -n 20 ./logs/mancenter.stdout.log",
      "chmod 0755 verify_mancenter.sh",
      "./verify_mancenter.sh  ${var.member_count}",
    ]
  }
}
