#!/usr/bin/env python3
"""
Django Tenant Loader Script - Enhanced for Complete ATO Workflow

This script loads tenant-specific fixtures for the ATO (Authorization to Operate) tool.
It sets up new tenants by loading their complete ATO workflow configuration from JSON fixture files.

Usage: python scripts/tenant_management/loadTenant.py --tenant="transport-canada"

The script looks for fixtures in: fixtures/tenants/<tenant_name>/
Compatible with the Complete ATO Workflow Generator (create_new_tenant.py)
"""

import os
import sys
import json
import argparse
from pathlib import Path

# Django setup
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'core.settings')
import django
django.setup()

# Import Django components
from django.core.management import call_command
from django.db.models.signals import pre_save, post_save, post_delete
from core.signals import pre_save_handler, post_save_handler, post_delete_handler


def check_ato_workflow_completion(tenant_name, remote_bucket=None):
    """Check if the ATO workflow generation was completed properly."""

    tenant_dir = Path(f'fixtures/tenants/{tenant_name}')
    
    if not tenant_dir.exists():
        print(f"❌ Tenant directory not found: {tenant_dir}")
        print(f"Attempting to locate tenant content remotely from bucket {remote_bucket}...")
        if not remote_bucket:
            print(f"Unable to continue. Remote Bucket not defined.")
            print(f"💡 Please ensure the either a local tenant is provided or a remote bucket is provided.")
            return False
        
        #run a subcommand getRemoteTenant.py {bucketname}
        print(f"💡 Running: python getRemoteTenant.py --s3-bucket=\"{remote_bucket}\"")
        
        os.system(f"python getRemoteTenant.py --s3-bucket=\"{remote_bucket}\"")
        tenant_dir = Path(f'fixtures/tenants/.temp')

        return tenant_dir.exists()
    
    
    # Check if we have fixture files
    json_files = list(tenant_dir.glob('*.json'))
    if not json_files:
        print(f"⚠️ No JSON fixture files found in {tenant_dir}")
        print(f"💡 Please review tenant configuration --tenant=\"{tenant_name}\"")
        return False
    
    print(f"✅ ATO workflow directory ready with {len(json_files)} fixture files")
    return True


def validate_ato_fixture_file(file_path):
    """Validate an ATO fixture file is properly populated."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Check if it's a valid Django fixture
        if not isinstance(data, list):
            return False, "Not a valid Django fixture format (should be a list)"
        
        if not data:
            return False, "Empty fixture file"
        
        # Check for empty required fields that likely need population
        empty_string_count = 0
        required_fields_empty = []
        
        for record in data:
            if isinstance(record, dict) and 'fields' in record:
                for field_name, field_value in record['fields'].items():
                    # Count empty strings in important fields
                    if field_value == "" and field_name not in [
                        'description', 'bio', 'notes', 'comments', 'logo', 'avatar', 
                        'profile_picture', 'website', 'linkedin_profile', 'github_profile',
                        'fax', 'mobile', 'last_login', 'otp_code', 'user_token'
                    ]:
                        empty_string_count += 1
                        if field_name in ['name', 'company_name', 'email', 'username', 'first_name', 'last_name']:
                            required_fields_empty.append(field_name)
        
        # Warning for many empty fields (but don't fail)
        if empty_string_count > 5:
            return True, f"Warning: {empty_string_count} empty fields (may need population)"
        
        # Fail only if critical required fields are empty
        if required_fields_empty:
            return False, f"Critical fields empty: {', '.join(required_fields_empty[:3])}"
        
        return True, f"Valid fixture with {len(data)} records"
        
    except json.JSONDecodeError as e:
        return False, f"Invalid JSON: {e}"
    except Exception as e:
        return False, f"Error reading file: {e}"


def get_ato_fixture_files(tenant_fixtures_dir):
    """Get and validate ATO fixture files for loading."""
    if not os.path.exists(tenant_fixtures_dir):
        return [], f"Tenant directory does not exist: {tenant_fixtures_dir}"
    
    # Get all JSON files
    all_json_files = [
        f for f in os.listdir(tenant_fixtures_dir)
        if f.endswith('.json')
    ]
    
    if not all_json_files:
        return [], f"No JSON fixture files found in {tenant_fixtures_dir}"
    
    # Validate all fixture files
    fixture_files = []
    skipped_files = []
    invalid_files = []
    warning_files = []
    
    for json_file in all_json_files:
        file_path = os.path.join(tenant_fixtures_dir, json_file)
        is_valid, message = validate_ato_fixture_file(file_path)
        
        if is_valid:
            fixture_files.append(json_file)
            if "Warning:" in message:
                warning_files.append((json_file, message))
        else:
            invalid_files.append((json_file, message))
    
    # Sort fixture files for proper loading order (alphabetical ensures dependencies)
    fixture_files.sort()
    
    return fixture_files, skipped_files, invalid_files, warning_files


def load_ato_tenant_fixtures(tenant_name=None, verbose=False, remote_bucket=None):
    """Load complete ATO tenant fixtures with enhanced error handling."""

    tenant_dir = tenant_name if tenant_name else '.temp'
    tenant_desc = f"tenant: {tenant_name}" if tenant_name else f"remote bucket: {remote_bucket}"

    tenant_fixtures_dir = f'fixtures/tenants/{tenant_dir}'
    
    print(f"🎯 Loading COMPLETE ATO workflow using {tenant_desc}")
    print(f"📁 Fixture Source: {tenant_fixtures_dir}")
    print("="*70)
    
    # Check ATO workflow completion
    if not check_ato_workflow_completion(tenant_name, remote_bucket):
        print(f"\n❌ ATO workflow not properly generated!")
        return False
    
    # Get and validate fixture files
    result = get_ato_fixture_files(tenant_fixtures_dir)
    if len(result) == 2:
        fixture_files, error_message = result
        print(f"❌ {error_message}")
        return False
    
    fixture_files, skipped_files, invalid_files, warning_files = result
    
    # Report file status
    if skipped_files:
        print(f"⏭️  Skipped files ({len(skipped_files)}):")
        for filename, reason in skipped_files:
            print(f"   • {filename} - {reason}")
    
    if warning_files:
        print(f"⚠️  Files with warnings ({len(warning_files)}):")
        for filename, reason in warning_files:
            print(f"   • {filename} - {reason}")
        print(f"   💡 These will load but may need more data population")
    
    if invalid_files:
        print(f"\n❌ Invalid files found ({len(invalid_files)}):")
        for filename, reason in invalid_files:
            print(f"   • {filename} - {reason}")
        print(f"\n💡 Please fix these files before loading!")
        return False
    
    if not fixture_files:
        print(f"\n❌ No valid fixture files found to load!")
        print(f"💡 Run: python create_new_tenant.py --tenant=\"{tenant_name}\"")
        return False
    
    print(f"\n📋 Loading ATO fixtures in dependency order ({len(fixture_files)} files):")
    for fixture_file in fixture_files:
        print(f"   • {fixture_file}")
    
    # Disconnect audit trail signals
    print(f"\n🔇 Disconnecting audit trail signals...")
    pre_save.disconnect(pre_save_handler)
    post_save.disconnect(post_save_handler)
    post_delete.disconnect(post_delete_handler)
    
    loaded_files = []
    failed_files = []
    
    try:
        print(f"\n🚀 Starting ATO workflow loading...")
        
        for fixture_file in fixture_files:
            print(f"\n📄 Loading {fixture_file}...")
            
            try:
                # Use Django's loaddata command to import fixture
                if verbose:
                    call_command('loaddata', os.path.join(tenant_fixtures_dir, fixture_file), verbosity=2)
                else:
                    call_command('loaddata', os.path.join(tenant_fixtures_dir, fixture_file), verbosity=1)
                
                print(f"   ✅ Successfully loaded {fixture_file}")
                loaded_files.append(fixture_file)
                
            except Exception as e:
                error_msg = str(e)
                print(f"   ❌ Error loading {fixture_file}: {error_msg}")
                
                # Provide helpful error guidance
                if "duplicate key" in error_msg.lower() or "unique constraint" in error_msg.lower():
                    print("      💡 Tip: Duplicate primary keys or unique fields")
                elif "foreign key" in error_msg.lower() or "constraint" in error_msg.lower():
                    print("      💡 Tip: Foreign key reference doesn't exist")
                elif "does not exist" in error_msg.lower():
                    print("      💡 Tip: Referenced record not found - check loading order")
                elif "json" in error_msg.lower():
                    print("      💡 Tip: Invalid JSON syntax")
                elif "required" in error_msg.lower() or "null" in error_msg.lower():
                    print("      💡 Tip: Required field is empty or null")
                else:
                    print("      💡 Tip: Check ATO_WORKFLOW_GUIDE.md for help")
                
                failed_files.append((fixture_file, error_msg))
                
                if not verbose:
                    print(f"      💡 Use --verbose for detailed error info")
        
        # Report results
        print(f"\n" + "="*70)
        print(f"📊 ATO WORKFLOW LOADING SUMMARY")
        print("="*70)
        print(f"✅ Successfully loaded: {len(loaded_files)} files")
        print(f"❌ Failed to load: {len(failed_files)} files")
        print(f"⚠️  Files with warnings: {len(warning_files)}")
        print(f"⏭️  Skipped files: {len(skipped_files)}")
        
        if loaded_files:
            print(f"\n🎉 Successfully loaded ATO components:")
            for filename in loaded_files:
                # Show component category
                if filename.startswith('a'):
                    category = "Foundation"
                elif filename.startswith('b'):
                    category = "User Management" 
                elif filename.startswith('c'):
                    category = "Organization"
                elif filename.startswith('d'):
                    category = "Environments"
                elif filename.startswith('e'):
                    category = "Applications"
                elif filename.startswith('f'):
                    category = "Documents"
                elif filename.startswith('g'):
                    category = "Controls"
                elif filename.startswith('h'):
                    category = "SOS Workflow"
                elif filename.startswith('i'):
                    category = "Security Assessment"
                elif filename.startswith('j'):
                    category = "Supporting Systems"
                else:
                    category = "Other"
                
                print(f"   ✅ {filename} ({category})")
        
        if failed_files:
            print(f"\n💥 Failed ATO components:")
            for filename, error in failed_files:
                print(f"   ❌ {filename} - {error[:80]}{'...' if len(error) > 80 else ''}")
            print(f"\n💡 Fix the errors and run again to load remaining components")
            return False
        
        if loaded_files:
            print(f"\n🎯 COMPLETE ATO WORKFLOW LOADED!")
            print("="*70)
            print(f"🏢 Tenant: {tenant_name}")
            print(f"📊 Components: {len(loaded_files)} loaded successfully")
            print(f"✅ Status: Ready for Authorization to Operate workflow!")
            print(f"\n💡 Next steps:")
            print(f"   • Users can now login and access the system")
            print(f"   • Applications are registered and ready for assessment")
            print(f"   • SOS and SA workflows are available")
            print(f"   • Complete ATO process can begin!")
            return True
        else:
            print(f"\n⚠️ No ATO components were loaded successfully")
            return False
            
    except Exception as e:
        print(f"\n💥 Fatal error during ATO workflow loading: {str(e)}")
        return False
    finally:
        # Reconnect signals after loading
        print(f"\n🔊 Reconnecting audit trail signals...")
        pre_save.connect(pre_save_handler)
        post_save.connect(post_save_handler)
        post_delete.connect(post_delete_handler)


def list_available_tenants():
    """List available tenants with ATO workflow status."""
    tenants_base_dir = 'fixtures/tenants'
    if os.path.exists(tenants_base_dir):
        available_tenants = [
            d for d in os.listdir(tenants_base_dir) 
            if os.path.isdir(os.path.join(tenants_base_dir, d))
        ]
        if available_tenants:
            print('\n📋 Available ATO tenant workflows:')
            for tenant in sorted(available_tenants):
                tenant_dir = Path(tenants_base_dir) / tenant
                json_files = len(list(tenant_dir.glob('*.json')))
                ato_guide = tenant_dir / 'ATO_WORKFLOW_GUIDE.md'
                status = "✅ Ready" if ato_guide.exists() and json_files > 0 else "⚠️ Incomplete"
                print(f'   • {tenant} ({json_files} fixtures) {status}')
        else:
            print('\n📋 No ATO tenant workflows found')
    else:
        print('\n📋 No tenants directory found')


def main():
    parser = argparse.ArgumentParser(
        description='Load complete ATO workflow fixtures for tenant setup',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/tenant_management/loadTenant.py --tenant="transport-canada"
  python scripts/tenant_management/loadTenant.py --tenant="health-canada" --verbose
  python scripts/tenant_management/loadTenant.py --list
  python scripts/tenant_management/loadTenant.py --remote-bucket="aws-s3-bucket-name"

 This loads COMPLETE ATO workflows generated by create_new_tenant.py
        """
    )
    
    parser.add_argument('--tenant', type=str,
                       help='(Optional) Tenant name (e.g., transport-canada)')
    parser.add_argument('--remote-bucket', type=str,
                       help='(Optional) Remote Bucket name to load tenant fixtures (e.g., s3-bucket-name)')    
    parser.add_argument('--list', action='store_true',
                       help='List available ATO tenant workflows')
    parser.add_argument('--verbose', action='store_true',
                       help='Enable verbose output')
    
    # Support legacy usage: python loadTenant.py tenant-name
#    if len(sys.argv) == 2 and not sys.argv[1].startswith('--'):
#        # Legacy usage
#        tenant_name = sys.argv[1]
#        return load_ato_tenant_fixtures(tenant_name, verbose=False)
    
    args = parser.parse_args()

    remote_bucket = args.remote_bucket
   
    if args.list:
        list_available_tenants()
        return True
    
    if not args.tenant and not remote_bucket:
        print("❌ Tenant name or Remote bucket is required!")
        parser.print_help()
        list_available_tenants()
        return False
    
    print(f"\n🔍 Starting to load ATO workflow for tenant: {args.tenant} or remote bucket: {remote_bucket}")
    return load_ato_tenant_fixtures(args.tenant, args.verbose, args.remote_bucket)


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)