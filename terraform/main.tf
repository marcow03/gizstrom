terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.31.0"
    }
  }
}

# Variables
variable "project" {}
variable "region" { default = "europe-west1" }
variable "zone" { default = "europe-west1-b" }
variable "aws_access_key" { }
variable "aws_secret_key" { }
variable "gcp_service_account_email" { }

# Provider
provider "google" {
  project = var.project
  region  = var.region
}

resource "google_compute_global_address" "lb_ip" {
  name = "lb-static-ip"
}

# Compute Instance (VM)
resource "google_compute_instance" "app_server" {
  name         = "docker-host"
  machine_type = "e2-standard-4"
  zone         = var.zone
  tags         = ["lb-backend"]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
    }
  }

  network_interface {
    network = "default"
    access_config {}
  }

  service_account {
    email  = var.gcp_service_account_email
    scopes = ["cloud-platform"]
  }

  metadata_startup_script = <<-EOT
    #!/bin/bash
    cd /opt

    # Install docker, if not already installed
    if ! command -v docker &> /dev/null; then
      echo "Docker not found. Installing Docker..."
      curl -fsSL https://get.docker.com -o get-docker.sh
      sudo sh ./get-docker.sh
    else
      echo "Docker is already installed."
    fi

    # Switch to the non-root user and run commands
    echo 'Starting application setup...'
    if [ ! -d 'gizstrom' ]; then
      echo 'Cloning repository...'
      git clone https://github.com/marcow03/gizstrom.git
      cd gizstrom
      echo 'Setting up environment variables...'
      cp example.env .env
      sed -i -e 's/AWS_ACCESS_KEY_ID=/AWS_ACCESS_KEY_ID=${var.aws_access_key}/g' .env
      sed -i -e 's/AWS_SECRET_ACCESS_KEY=/AWS_SECRET_ACCESS_KEY=${var.aws_secret_key}/g' .env
    else
      echo 'Repository already exists. Skipping clone.'
      cd gizstrom
    fi

    echo 'Starting Docker containers...'
    docker compose up -d
  EOT
}

# Instance Group & Port Mapping
resource "google_compute_instance_group" "app_group" {
  name      = "multi-port-group"
  zone      = var.zone
  instances = [google_compute_instance.app_server.self_link]

  named_port {
    name = "app-port"
    port = 80
  }
  named_port {
    name = "airflow-port"
    port = 8080
  }
  named_port {
    name = "feast-port"
    port = 8088
  }
  named_port {
    name = "rustfs-port"
    port = 9001
  }
  named_port {
    name = "mlflow-port"
    port = 5001
  }
  named_port {
    name = "inference-port"
    port = 8001
  }
}

# Health Check
resource "google_compute_health_check" "default" {
  name = "standard-health-check"
  http_health_check {
    port = 80 # Using the main app port for health verification
  }
}

# Routing Logic (Locals)
locals {
  services = {
    "app"       = "app-port"
    "airflow"   = "airflow-port"
    "feast-ui"  = "feast-port"
    "rustfs"    = "rustfs-port"
    "mlflow"    = "mlflow-port"
    "inference" = "inference-port"
  }
}

# Backend Services
resource "google_compute_backend_service" "backends" {
  for_each      = local.services
  name          = "${each.key}-backend"
  port_name     = each.value
  protocol      = "HTTP"
  health_checks = [google_compute_health_check.default.id]
  backend {
    group = google_compute_instance_group.app_group.id
  }
}

# URL Map (Router)
resource "google_compute_url_map" "default" {
  name            = "subdomain-router"
  default_service = google_compute_backend_service.backends["app"].id

  dynamic "host_rule" {
    for_each = local.services
    content {
      hosts        = ["${host_rule.key}.${replace(google_compute_global_address.lb_ip.address, ".", "-")}.nip.io"]
      path_matcher = host_rule.key
    }
  }

  dynamic "path_matcher" {
    for_each = local.services
    content {
      name            = path_matcher.key
      default_service = google_compute_backend_service.backends[path_matcher.key].id
    }
  }
}

# Load Balancer Frontend
resource "google_compute_target_http_proxy" "http_proxy" {
  name    = "http-proxy"
  url_map = google_compute_url_map.default.id
}

resource "google_compute_global_forwarding_rule" "default" {
  name       = "global-rule"
  ip_address = google_compute_global_address.lb_ip.address
  target     = google_compute_target_http_proxy.http_proxy.id
  port_range = "80"
}

# Firewall
resource "google_compute_firewall" "allow_lb" {
  name    = "allow-google-lb-multi"
  network = "default"
  allow {
    protocol = "tcp"
    ports    = ["80", "8080", "8088", "9001", "5001", "8001"]
  }
  source_ranges = ["130.211.0.0/22", "35.191.0.0/16"]
  target_tags   = ["lb-backend"]
}

# Outputs
output "instructions" {
  value = "Wait 5-10 minutes for the Load Balancer to initialize and the VM to finish the startup script."
}

output "urls" {
  value = {
    for name, _ in local.services :
    name => "http://${name}.${replace(google_compute_global_address.lb_ip.address, ".", "-")}.nip.io"
  }
}
