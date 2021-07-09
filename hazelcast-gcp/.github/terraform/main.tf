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

  credentials = file("gcp_key_file.json")
  batching {
    enable_batching = "false"
  }
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

#################### SERVICE ACCOUNT ####################

resource "google_service_account" "service_account" {
  account_id   = "${var.prefix}-sa"
  display_name = "Service Account for Hazelcast-gcp Integration Test"
}

resource "random_id" "id" {
  byte_length = 8
}
resource "google_project_iam_custom_role" "discovery_role" {
  role_id     = "HazelcastGcpIntegrationTest${random_id.id.hex}"
  title       = "Discovery Role for hazelcast-gcp Integration tests"
  permissions = ["compute.instances.list", "compute.zones.list", "compute.regions.get"]
}

resource "google_project_iam_member" "project" {
  depends_on = [google_service_account.service_account]
  project = var.project_id
  role    = google_project_iam_custom_role.discovery_role.name
  member  = "serviceAccount:${google_service_account.service_account.email}"
}


########## NETWORK - SUBNETWORK - FIREWALL - PUBLIC IP ##################

resource "google_compute_network" "vpc" {
  name                    = "${var.prefix}-vpc"
  auto_create_subnetworks = false
}


resource "google_compute_subnetwork" "vpc_subnet" {
  name          = "${var.prefix}-subnet"
  ip_cidr_range = "10.0.10.0/24"
  region        = var.region
  network       = google_compute_network.vpc.id
}

resource "google_compute_firewall" "firewall" {
  name    = "${var.prefix}-firewall"
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
  name  = "${var.prefix}-ip-${count.index}"
}

############## HAZELCAST MEMBERS #####################

resource "google_compute_instance" "hazelcast_member" {
  count                     = var.member_count
  name                      = "${var.prefix}-instance-${count.index}"
  machine_type              = var.gcp_instance_type
  allow_stopping_for_update = "true"
  zone                      = var.zone
  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-9"
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
    ssh-keys = "${var.gcp_ssh_user}:${file("${var.local_key_path}/${var.gcp_key_name}.pub")}"
  }

  connection {
    host        = self.network_interface[0].access_config[0].nat_ip
    user        = var.gcp_ssh_user
    type        = "ssh"
    private_key = file("${var.local_key_path}/${var.gcp_key_name}")
    timeout     = "120s"
    agent       = false
  }
  provisioner "remote-exec" {
    inline = [
      "mkdir -p /home/${var.gcp_ssh_user}/jars",
      "mkdir -p /home/${var.gcp_ssh_user}/logs",
      "sudo apt-get update",
      "sudo apt-get -y install openjdk-8-jdk wget",
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
    source      = "~/lib/hazelcast-gcp.jar"
    destination = "/home/${var.gcp_ssh_user}/jars/hazelcast-gcp.jar"
  }

  provisioner "file" {
    source      = "~/lib/hazelcast.jar"
    destination = "/home/${var.gcp_ssh_user}/jars/hazelcast.jar"
  }

  provisioner "file" {
    source      = "hazelcast.yaml"
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
    private_key = file("${var.local_key_path}/${var.gcp_key_name}")
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
  name                      = "${var.prefix}-mancenter"
  machine_type              = var.gcp_instance_type
  allow_stopping_for_update = "true"
  zone                      = var.zone
  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-9"
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
    ssh-keys = "${var.gcp_ssh_user}:${file("${var.local_key_path}/${var.gcp_key_name}.pub")}"
  }

  connection {
    host        = self.network_interface[0].access_config[0].nat_ip
    user        = var.gcp_ssh_user
    type        = "ssh"
    private_key = file("${var.local_key_path}/${var.gcp_key_name}")
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
    source      = "hazelcast-client.yaml"
    destination = "/home/${var.gcp_ssh_user}/hazelcast-client.yaml"
  }

  provisioner "remote-exec" {
    inline = [
      "sudo apt-get update",
      "sudo apt-get -y install openjdk-8-jdk wget unzip",
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
    private_key = file("${var.local_key_path}/${var.gcp_key_name}")
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
