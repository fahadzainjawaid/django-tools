# your_app/management/commands/init.py

from django.core.management.base import BaseCommand

def init_environment(sosRulesEngine="basic", catalogue="itsg-33", tenant="test-tenant", ai_kb="all", dry_run=False, remote_bucket=None):
    print("=== Environment Initialization ===")
#    print(f"- Tenant: {tenant}")
    print(f"- Dry Run: {'Yes' if dry_run else 'No'}")
    print(f"- Remote Bucket: {remote_bucket if remote_bucket else 'Not specified'}")
    print(f"---- Ignoring tenant: {tenant} remote bucket is specified ----" if remote_bucket else "")

    commands_array = [
        'python3 manage.py reset_db',
        'python3 manage.py migrate',
        'python3 loaders/loadAllFixtures.py',
    ]

    import os
    print(f"Current Directory: {os.getcwd()}")
    print(f"SECRET_STRING is: {os.system('echo $SECRET_STRING')}")
    
    if dry_run:
        print("[DRY RUN] No changes will be made.")

        # print current directory 
        
        #loop thru commands_array and print each command, and put command in a green color. The entire line is prefixed by [DRY RUN]
        for command in commands_array:
            print(f"[DRY RUN] \033[92m{command}\033[0m")
            
        return
    



    print("Executing commands:")
    for command in commands_array:
        print(f"Executing: \033[92m{command}\033[0m")
        os.system(command)
        print("\033[92m---------------------------------------------------------------------------\033[0m")


class Command(BaseCommand):
    help = 'Initializes the environment with specified configuration.'

    def add_arguments(self, parser):
        parser.add_argument('--tenant', default='test-tenant', help='Tenant name')
        parser.add_argument('--dry-run', action='store_true', help='Simulate the command without making changes')
        parser.add_argument('--remote-bucket', default=None, help='Load tenant from remote bucket. Tenant argument is ignored.')

    def handle(self, *args, **options):
        init_environment(
            sosRulesEngine=options['sosRulesEngine'],
            catalogue=options['catalogue'],
            tenant=options['tenant'],
            ai_kb=options['ai_kb'],
            dry_run=options['dry_run'],
            remote_bucket=options.get('remote_bucket', None)
        )

