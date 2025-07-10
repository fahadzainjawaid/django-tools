import os, sys
import django
from django.core.management import call_command

# 1. Set up Django environment first
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'core.settings')  # Replace with your settings module
django.setup()

# 2. Now import other components
from django.db.models.signals import pre_save, post_save, post_delete
from core.signals import pre_save_handler, post_save_handler, post_delete_handler

# 3. Directory containing the fixtures
fixtures_dir = 'fixtures'


# 4. List of fixtures to skip
skip_fixtures = [
    'blueprint', 'colors', 'permissions', 'ato_status_choices',
    'months', 'risk_level_choices', 'controlset_types', 'audittrail', 'widgets', 'support_case_request_types'
] 



def load_fixtures():
    # Disconnect audit trail signals
    pre_save.disconnect(pre_save_handler)
    post_save.disconnect(post_save_handler)
    post_delete.disconnect(post_delete_handler)

    try:
        # Build and load fixtures
        fixture_files = [
            f for f in os.listdir(fixtures_dir)
            if f.endswith('.json') and
            f.split('.')[0] not in skip_fixtures
        ]
              
       
        fixture_files.sort()

        print('Loading fixtures in the following order:')
        print(fixture_files)

        for fixture_file in fixture_files:
            print(f"Loading {fixture_file}...")
            # Use Django's internal call_command instead of subprocess
            call_command('loaddata', os.path.join(fixtures_dir, fixture_file))

    finally:
        # Reconnect signals after loading
        pre_save.connect(pre_save_handler)
        post_save.connect(post_save_handler)
        post_delete.connect(post_delete_handler)

if __name__ == '__main__':
    load_fixtures()