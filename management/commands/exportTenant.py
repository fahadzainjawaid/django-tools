import os
import sys
import subprocess
from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = "Runs exportTenant.py with the provided arguments (--tenant or --remote-bucket)."

    def add_arguments(self, parser):
        parser.add_argument('--tenant', help='Tenant name for local export.')
        parser.add_argument('--remote-bucket', help='S3 bucket name for remote export.')

    def handle(self, *args, **options):
        tenant = options.get('tenant')
        remote_bucket = options.get('remote_bucket')

        if not tenant and not remote_bucket:
            raise CommandError("You must provide either --tenant or --remote-bucket.")

        cmd = ['python3', 'exportTenant.py']
        if tenant:
            cmd += ['--tenant', tenant]
        if remote_bucket:
            cmd += ['--remote_bucket', remote_bucket]

        self.stdout.write(self.style.HTTP_INFO(f"Running command: {' '.join(cmd)}"))

        try:
            subprocess.run(cmd, check=True)
            self.stdout.write(self.style.SUCCESS("exportTenant.py executed successfully."))
        except subprocess.CalledProcessError as e:
            raise CommandError(f"exportTenant.py failed with return code {e.returncode}")
