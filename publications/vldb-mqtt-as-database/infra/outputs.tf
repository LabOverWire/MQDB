output "instance_ip" {
  description = "Public IP of the benchmark instance"
  value       = aws_instance.bench.public_ip
}

output "instance_id" {
  description = "EC2 instance ID"
  value       = aws_instance.bench.id
}

output "ssh_command" {
  description = "SSH command to connect to the instance"
  value       = "ssh -i ~/.ssh/id_ed25519 ubuntu@${aws_instance.bench.public_ip}"
}
