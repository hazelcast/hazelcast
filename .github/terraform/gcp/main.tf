terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "= 3.35.0"
    }
  }
  required_version = ">= 0.13"
}

provider "google" {
  credentials = file(var.gcp_key_file)
  batching {
    enable_batching = "false"
  }
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

resource "random_pet" "prefix" {
  prefix    = "test"
  length    = 2
  separator = "-"
}

data "template_file" "hazelcast" {
  template = file("${path.module}/hazelcast.yaml")
  vars = {
    LABEL_KEY   = var.gcp_label_key
    LABEL_VALUE = var.gcp_label_value
  }
}

data "template_file" "hazelcast_client" {
  template = file("${path.module}/hazelcast-client.yaml")
  vars = {
    LABEL_KEY   = var.gcp_label_key
    LABEL_VALUE = var.gcp_label_value
  }
}

data "template_file" "user_data" {
  template = file("${path.module}/cloud-init.yaml")
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

#################### SERVICE ACCOUNT ####################

resource "google_service_account" "service_account" {
  account_id   = "${random_pet.prefix.id}-sa"
  display_name = "Service Account for Hazelcast Integration Test"
}

resource "random_id" "id" {
  byte_length = 8
}
resource "google_project_iam_custom_role" "discovery_role" {
  role_id     = "HazelcastGcpIntegrationTest${random_id.id.hex}"
  title       = "Discovery Role for hazelcast Integration tests"
  permissions = ["compute.instances.list", "compute.zones.list", "compute.regions.get"]
}

resource "google_project_iam_member" "project" {
  depends_on = [google_service_account.service_account]
  project    = var.project_id
  role       = google_project_iam_custom_role.discovery_role.name
  member     = "serviceAccount:${google_service_account.service_account.email}"
}


########## NETWORK - SUBNETWORK - FIREWALL - PUBLIC IP ##################

resource "google_compute_network" "vpc" {
  name                    = "${random_pet.prefix.id}-vpc"
  auto_create_subnetworks = false
}


resource "google_compute_subnetwork" "vpc_subnet" {
  name          = "${random_pet.prefix.id}-subnet"
  ip_cidr_range = "10.0.10.0/24"
  region        = var.region
  network       = google_compute_network.vpc.id
}

resource "google_compute_firewall" "firewall" {
  name    = "${random_pet.prefix.id}-firewall"
  network = google_compute_network.vpc.name

  # Allow SSH, Hazelcast member communication and Hazelcat Management Center website
  allow {
    protocol = "tcp"
    ports    = ["22", "5701-5707", "8080"]
  }

  allow {
    protocol = "icmp"
  }
}

resource "google_compute_address" "public_ip" {
  count = var.member_count + 1
  name  = "${random_pet.prefix.id}-ip-${count.index}"
}

############## HAZELCAST MEMBERS #####################

resource "google_compute_instance" "hazelcast_member" {
  count                     = var.member_count
  name                      = "${random_pet.prefix.id}-instance-${count.index}"
  machine_type              = var.gcp_instance_type
  allow_stopping_for_update = "true"
  zone                      = var.zone
  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-1804-lts"
    }
  }

  labels = {
    "${var.gcp_label_key}" = var.gcp_label_value
  }

  network_interface {
    subnetwork = google_compute_subnetwork.vpc_subnet.self_link
    access_config {
      nat_ip = google_compute_address.public_ip[count.index].address
    }
  }

  service_account {
    email  = google_service_account.service_account.email
    scopes = ["cloud-platform"]
  }

  metadata = {
    ssh-keys  = "${var.gcp_ssh_user}:${tls_private_key.ssh.public_key_openssh}"
    user-data = "${data.template_file.user_data.rendered}"
  }

  connection {
    host        = self.network_interface[0].access_config[0].nat_ip
    user        = var.gcp_ssh_user
    type        = "ssh"
    private_key = tls_private_key.ssh.private_key_pem
    timeout     = "120s"
    agent       = false
  }
  provisioner "remote-exec" {
    inline = [
      "mkdir -p /home/${var.gcp_ssh_user}/jars",
      "mkdir -p /home/${var.gcp_ssh_user}/logs",
      "while [ ! -f /var/lib/cloud/instance/boot-finished ]; do echo 'Waiting for cloud-init...'; sleep 10; done",
    ]
  }

  provisioner "file" {
    source      = "scripts/start_gcp_hazelcast_member.sh"
    destination = "/home/${var.gcp_ssh_user}/start_gcp_hazelcast_member.sh"
  }

  provisioner "file" {
    source      = "scripts/verify_member_count.sh"
    destination = "/home/${var.gcp_ssh_user}/verify_member_count.sh"
  }

  provisioner "file" {
    source      = var.hazelcast_path
    destination = "/home/${var.gcp_ssh_user}/jars/hazelcast.jar"
  }

  provisioner "file" {
    content     = data.template_file.hazelcast.rendered
    destination = "/home/${var.gcp_ssh_user}/hazelcast.yaml"
  }

  provisioner "remote-exec" {
    inline = [
      "cd /home/${var.gcp_ssh_user}",
      "chmod 0755 start_gcp_hazelcast_member.sh",
      "./start_gcp_hazelcast_member.sh  ${var.gcp_label_key} ${var.gcp_label_value} ",
      "sleep 5",
    ]
  }
}

resource "null_resource" "verify_members" {
  count      = var.member_count
  depends_on = [google_compute_instance.hazelcast_member]
  connection {
    type        = "ssh"
    user        = var.gcp_ssh_user
    host        = google_compute_instance.hazelcast_member[count.index].network_interface.0.access_config.0.nat_ip
    timeout     = "180s"
    agent       = false
    private_key = tls_private_key.ssh.private_key_pem
  }


  provisioner "remote-exec" {
    inline = [
      "cd /home/${var.gcp_ssh_user}",
      "tail -n 20 ./logs/hazelcast.logs",
      "chmod 0755 verify_member_count.sh",
      "./verify_member_count.sh  ${var.member_count}",
    ]
  }

}

############## HAZELCAST MANAGEMENT CENTER #######################

resource "google_compute_instance" "hazelcast_mancenter" {
  name                      = "${random_pet.prefix.id}-mancenter"
  machine_type              = var.gcp_instance_type
  allow_stopping_for_update = "true"
  zone                      = var.zone
  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-1804-lts"
    }
  }

  labels = {
    "${var.gcp_label_key}" = var.gcp_label_value
  }

  network_interface {
    subnetwork = google_compute_subnetwork.vpc_subnet.self_link
    access_config {
      nat_ip = google_compute_address.public_ip[var.member_count].address
    }
  }

  service_account {
    email  = google_service_account.service_account.email
    scopes = ["cloud-platform"]
  }

  metadata = {
    ssh-keys  = "${var.gcp_ssh_user}:${tls_private_key.ssh.public_key_openssh}"
    user-data = "${data.template_file.user_data.rendered}"
  }

  connection {
    host        = self.network_interface[0].access_config[0].nat_ip
    user        = var.gcp_ssh_user
    type        = "ssh"
    private_key = tls_private_key.ssh.private_key_pem
    timeout     = "120s"
    agent       = false
  }

  provisioner "file" {
    source      = "scripts/start_gcp_hazelcast_management_center.sh"
    destination = "/home/${var.gcp_ssh_user}/start_gcp_hazelcast_management_center.sh"
  }

  provisioner "file" {
    source      = "scripts/verify_mancenter.sh"
    destination = "/home/${var.gcp_ssh_user}/verify_mancenter.sh"
  }

  provisioner "file" {
    content     = data.template_file.hazelcast_client.rendered
    destination = "/home/${var.gcp_ssh_user}/hazelcast-client.yaml"
  }

  provisioner "remote-exec" {
    inline = [
      "while [ ! -f /var/lib/cloud/instance/boot-finished ]; do echo 'Waiting for cloud-init...'; sleep 10; done",
    ]
  }
  provisioner "remote-exec" {
    inline = [
      "cd /home/${var.gcp_ssh_user}",
      "chmod 0755 start_gcp_hazelcast_management_center.sh",
      "./start_gcp_hazelcast_management_center.sh ${var.hazelcast_mancenter_version} ${var.gcp_label_key} ${var.gcp_label_value} ",
      "sleep 5",
    ]
  }
}

resource "null_resource" "verify_mancenter" {

  depends_on = [google_compute_instance.hazelcast_member, google_compute_instance.hazelcast_mancenter]

  connection {
    type        = "ssh"
    user        = var.gcp_ssh_user
    host        = google_compute_instance.hazelcast_mancenter.network_interface.0.access_config.0.nat_ip
    timeout     = "180s"
    agent       = false
    private_key = tls_private_key.ssh.private_key_pem
  }

  provisioner "remote-exec" {
    inline = [
      "cd /home/${var.gcp_ssh_user}",
      "tail -n 20 ./logs/mancenter.stdout.log",
      "chmod 0755 verify_mancenter.sh",
      "./verify_mancenter.sh  ${var.member_count}",
    ]
  }
}
