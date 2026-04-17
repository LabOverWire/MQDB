variable "aws_profile" {
  description = "AWS CLI profile name"
  type        = string
  default     = "laboverwire"
}

variable "region" {
  description = "AWS region"
  type        = string
  default     = "ca-west-1"
}

variable "instance_type" {
  description = "EC2 instance type (ARM Graviton)"
  type        = string
  default     = "c7g.xlarge"
}

variable "ssh_public_key_path" {
  description = "Path to SSH public key for instance access"
  type        = string
  default     = "~/.ssh/id_ed25519.pub"
}

variable "allowed_ssh_cidr" {
  description = "CIDR block allowed to SSH (tighten to your IP)"
  type        = string
  default     = "0.0.0.0/0"
}

variable "vpc_id" {
  description = "VPC ID (uses default VPC if not set)"
  type        = string
  default     = null
}
