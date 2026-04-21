output "instance_id" {
  description = "EC2 instance ID"
  value       = aws_instance.bench.id
}

output "public_dns" {
  description = "Public DNS of the benchmark instance"
  value       = aws_instance.bench.public_dns
}

output "region" {
  description = "AWS region"
  value       = var.region
}

output "ami_id" {
  description = "AMI ID used for the instance"
  value       = data.aws_ami.ubuntu.id
}

output "ssh_command" {
  description = "SSH command to connect to the instance"
  value       = "ssh -i ~/.ssh/id_ed25519 ubuntu@${aws_instance.bench.public_dns}"
}
