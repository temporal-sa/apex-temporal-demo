resource "aws_instance" "web" {
  # Floci's image catalog maps this to the amazonlinux:2023 container image.
  # (It ignores the name filter in data.aws_ami, so pin it here.)
  ami = "ami-amazonlinux2023"

  # With this set at all, even to false, the provider sends the subnet and
  # security groups inside a network interface spec, which Floci ignores: the
  # instance lands in the default group and port 80 is never published.
  # Floci reports 127.0.0.1 as the public address regardless.
  associate_public_ip_address = null

  # Floci instances are containers without systemd, so start nginx directly.
  # Floci's metadata proxy already holds 169.254.169.254:80 inside them, which
  # blocks nginx's default listen-on-everything, so listen on each of the
  # instance's own IPv4 addresses instead (Floci's port forward targets one of
  # them, which one depends on the Docker network Floci runs in).
  user_data = <<-EOF
    #!/bin/bash
    dnf install -y nginx
    echo "<h1>Apex demo: ${var.env_name} ${var.app_version} healthy</h1>" > /usr/share/nginx/html/index.html
    LISTEN=""
    for ip in $(awk '/32 host LOCAL/ {print last} {last = $2}' /proc/net/fib_trie | sort -u | grep -v -e '^127\.' -e '^169\.254\.'); do
      LISTEN="$LISTEN listen $ip:80;"
    done
    sed -i "s/listen       80;/$LISTEN/; /listen       \[::\]:80;/d" /etc/nginx/nginx.conf
    nginx
  EOF
}

# Floci forwards the instance's port 80 to a host port it picks (30000+) and
# reports 127.0.0.1 as the public IP, so name the instance instead. The local
# worker's health check resolves <instance-id>.floci.localhost to the
# forwarded port (activities/floci.py).
output "public_ip" {
  value = "${aws_instance.web.id}.floci.localhost"
}
