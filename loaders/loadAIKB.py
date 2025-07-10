import os
import sys
import json
import boto3
import django
import time
import re
from urllib.parse import urlparse
from django.core.management import call_command
from django.db.models.signals import pre_save, post_save, post_delete

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'core.settings')
django.setup()

from apps.ai_chat.classes.config_manager import BedrockConfigManager
from core.signals import pre_save_handler, post_save_handler, post_delete_handler
from core import lnineConnectors
from apps.ai_chat.models import ComplianceFramework

class AIKnowledgeBaseLoader:
    
    def __init__(self, catalogue_name):
        self.catalogue_name = catalogue_name
        self.ai_kb_fixtures_dir = f'fixtures/ai_kb/{catalogue_name}'
        self.base_fixtures_dir = 'fixtures/ai_kb/base'
        self.bedrock_agent_client = self._init_bedrock_agent_client()
        self.knowledge_base_id = lnineConnectors.getBedrockKnowledgeBaseId()
        self.data_source_id = None
        
    def _init_bedrock_agent_client(self):
        config = BedrockConfigManager()
        endpoint_url = config.get_endpoint_url()
        agent_endpoint = endpoint_url.replace("bedrock-agent-runtime", "bedrock-agent")
        
        client_params = {
            'service_name': 'bedrock-agent',
            'region_name': config.get_region(),
            'endpoint_url': agent_endpoint
        }
        
        aws_access_key = config.get_aws_access_key()
        aws_secret_access_key = config.get_aws_secret_key()
        
        if aws_access_key and aws_secret_access_key:
            client_params.update({
                'aws_access_key_id': aws_access_key,
                'aws_secret_access_key': aws_secret_access_key
            })
        
        return boto3.client(**client_params)

    def load_catalogue(self):
        print(f'🚀 Loading AI Knowledge Base for {self.catalogue_name} catalogue from {self.ai_kb_fixtures_dir}...')
        print(f'📌 REMINDER: Each catalogue ingestion can take 20-40 minutes')
        print(f'📌 REMINDER: 3000 character limit for all URLs combined (your tested working limit)')
        print(f'📌 REMINDER: Each run replaces existing URLs in the same data source')
        print(f'📌 REMINDER: Ensure URL quality - broken/slow URLs will affect entire knowledge base performance')
        
        if not os.path.exists(self.ai_kb_fixtures_dir):
            print(f"❌ Catalogue directory not found: {self.ai_kb_fixtures_dir}")
            print("Available catalogues:")
            self._list_available_catalogues()
            return False
        
        print("🔧 Getting or creating catalogue data source...")
        self.data_source_id = self._get_or_create_catalogue_data_source()
        
        # Load base knowledge first (available to all customers)
        all_urls = []
        if os.path.exists(self.base_fixtures_dir) and self.catalogue_name != 'base':
            print("📚 Loading base knowledge (available to all customers)...")
            base_urls = self._load_catalogue_urls(self.base_fixtures_dir)
            all_urls.extend(base_urls)
            print(f"  ✅ Added {len(base_urls)} base URLs")
        
        # Load catalogue-specific knowledge
        catalogue_urls = self._load_catalogue_urls(self.ai_kb_fixtures_dir)
        all_urls.extend(catalogue_urls)
        print(f"  ✅ Added {len(catalogue_urls)} catalogue-specific URLs")
        
        if all_urls:
            # Validate and clean URLs
            validated_urls = self._validate_and_clean_urls(all_urls)
            
            # Remove duplicates while preserving order
            final_urls = self._deduplicate_urls(validated_urls)
            
            # Check character limit (your proven working limit)
            total_chars = sum(len(url) for url in final_urls)
            print(f"📊 Final URL Stats:")
            print(f"   • Total URLs: {len(final_urls)}")
            print(f"   • Total characters: {total_chars}")
            print(f"   • Average URL length: {total_chars // len(final_urls) if final_urls else 0}")
            
            if total_chars > 3000:
                print(f"⚠️  WARNING: URLs exceed 3000 character limit ({total_chars} chars)")
                print(f"💡 Consider reducing URLs in your JSON files")
                return False
            
            print(f"✅ URL count and character limit OK")
            
            self._update_data_source_urls(final_urls)
            job_id = self._start_ingestion_job()
            
            # Wait for ingestion to complete
            print(f"⏳ Waiting for AWS Bedrock ingestion to complete...")
            ingestion_success = self._wait_for_ingestion_completion(job_id)
            
            if ingestion_success:
                self._load_django_fixtures()
                print(f"\n🎉 Successfully configured AI Knowledge Base!")
                print(f"📊 Final URLs loaded: {len(final_urls)}")
                print(f"🔧 Catalogue: {self.catalogue_name}")
                print(f"⚡ Ingestion Job ID: {job_id}")
                print(f"🔍 Knowledge Base ID: {self.knowledge_base_id}")
                print(f"📡 Data Source ID: {self.data_source_id}")
                print(f"📌 REMINDER: This data source now contains ONLY {self.catalogue_name} + base knowledge")
            else:
                print(f"❌ Ingestion job failed or timed out!")
                return False
            
            return True
        else:
            print("❌ No URLs found to load!")
            return False
    
    def _validate_and_clean_urls(self, urls):
        """Validate URL quality and format"""
        print("🔍 Validating URL quality...")
        
        valid_urls = []
        issues_found = []
        
        for i, url in enumerate(urls):
            # Clean whitespace
            clean_url = url.strip()
            
            # Basic validation
            if not clean_url:
                issues_found.append(f"Empty URL at position {i+1}")
                continue
                
            # URL format validation
            parsed = urlparse(clean_url)
            if not parsed.scheme or not parsed.netloc:
                issues_found.append(f"Invalid URL format: {clean_url[:50]}...")
                continue
            
            # Check for common issues
            if len(clean_url) > 200:
                issues_found.append(f"Very long URL ({len(clean_url)} chars): {clean_url[:50]}...")
            
            # Check for suspicious patterns
            suspicious_patterns = [
                r'localhost',
                r'127\.0\.0\.1',
                r'\.local',
                r'test\.',
                r'staging\.',
                r'dev\.'
            ]
            
            if any(re.search(pattern, clean_url, re.IGNORECASE) for pattern in suspicious_patterns):
                issues_found.append(f"Suspicious URL (dev/test/local): {clean_url[:50]}...")
            
            valid_urls.append(clean_url)
        
        # Report issues
        if issues_found:
            print(f"⚠️  Found {len(issues_found)} URL quality issues:")
            for issue in issues_found[:10]:  # Show first 10 issues
                print(f"   • {issue}")
            if len(issues_found) > 10:
                print(f"   • ... and {len(issues_found) - 10} more issues")
            print(f"📊 Valid URLs: {len(valid_urls)}/{len(urls)}")
        else:
            print(f"✅ All {len(valid_urls)} URLs passed validation")
        
        return valid_urls
    
    def _deduplicate_urls(self, all_urls):
        """Remove duplicate URLs while preserving order"""
        seen = set()
        deduplicated = []
        
        for url in all_urls:
            if url not in seen:
                seen.add(url)
                deduplicated.append(url)
        
        removed_count = len(all_urls) - len(deduplicated)
        if removed_count > 0:
            print(f"🔄 Removed {removed_count} duplicate URLs")
        
        return deduplicated
    
    def _wait_for_ingestion_completion(self, job_id, timeout_minutes=20):
        """Wait for AWS Bedrock ingestion job to complete"""
        print(f"⏳ Monitoring ingestion job {job_id}...")
        start_time = time.time()
        
        while time.time() - start_time < timeout_minutes * 60:
            try:
                response = self.bedrock_agent_client.get_ingestion_job(
                    knowledgeBaseId=self.knowledge_base_id,
                    dataSourceId=self.data_source_id,
                    ingestionJobId=job_id
                )
                
                status = response['ingestionJob']['status']
                
                if status == 'COMPLETE':
                    print(f"✅ Ingestion completed successfully!")
                    return True
                elif status == 'FAILED':
                    failure_reasons = response['ingestionJob'].get('failureReasons', [])
                    print(f"❌ Ingestion failed!")
                    for reason in failure_reasons:
                        print(f"   • {reason}")
                    return False
                elif status in ['IN_PROGRESS', 'STARTING']:
                    elapsed = int(time.time() - start_time)
                    print(f"📊 Status: {status} (elapsed: {elapsed}s)")
                    time.sleep(30)  # Check every 30 seconds
                else:
                    print(f"📊 Status: {status}")
                    time.sleep(30)
                    
            except Exception as e:
                print(f"⚠️ Error checking job status: {str(e)}")
                time.sleep(30)
        
        print(f"⏰ Timeout after {timeout_minutes} minutes - ingestion may still be in progress")
        return False

    def _get_or_create_catalogue_data_source(self):
        try:
            data_source_name = f"fixtures-{self.catalogue_name}-kb-source"
            existing_data_source = self._find_data_source_by_name(data_source_name)
            
            if existing_data_source:
                print(f"✅ Found existing data source: {existing_data_source['dataSourceId']}")
                print(f"📌 REMINDER: This will REPLACE all existing URLs in the data source (3000 char limit)")
                return existing_data_source['dataSourceId']
            else:
                print(f"🔧 Creating new data source for {self.catalogue_name} catalogue...")
                return self._create_catalogue_data_source()
                
        except Exception as e:
            print(f"❌ Error getting or creating catalogue data source: {str(e)}")
            raise
    
    def _find_data_source_by_name(self, data_source_name):
        try:
            response = self.bedrock_agent_client.list_data_sources(
                knowledgeBaseId=self.knowledge_base_id
            )
            
            for data_source in response.get('dataSourceSummaries', []):
                if data_source.get('name') == data_source_name:
                    return data_source
            
            return None
            
        except Exception as e:
            print(f"⚠️ Error listing data sources: {str(e)}")
            return None
    
    def _load_catalogue_urls(self, directory_path):
        all_urls = []
        
        if not os.path.exists(directory_path):
            print(f"⚠️ Directory not found: {directory_path}")
            return all_urls
        
        json_files = [
            f for f in os.listdir(directory_path)
            if f.endswith('.json') and f != 'catalogue_info.json'
        ]
        
        json_files.sort()
        
        print(f'Loading URL files from {os.path.basename(directory_path)} in the following order:')
        print(json_files)
        
        for json_file in json_files:
            file_path = os.path.join(directory_path, json_file)
            
            try:
                print(f"📄 Loading {json_file}...")
                
                with open(file_path, 'r') as f:
                    data = json.load(f)
                
                if isinstance(data, dict) and 'urls' in data:
                    urls = data['urls']
                    if isinstance(urls, list):
                        all_urls.extend(urls)
                        print(f"  ✅ Added {len(urls)} URLs from {json_file}")
                    else:
                        print(f"  ⚠️ 'urls' field is not a list in {json_file}")
                else:
                    print(f"  ⚠️ No 'urls' field found in {json_file}")
                    
            except Exception as e:
                print(f"  ❌ Error loading {json_file}: {str(e)}")
        
        return all_urls
    
    def _update_data_source_urls(self, urls):
        try:
            print(f"🔧 Updating data source with {len(urls)} URLs...")
            print(f"📌 REMINDER: AWS Bedrock will crawl and ingest ALL these URLs in one operation")
            
            current_ds = self.bedrock_agent_client.get_data_source(
                knowledgeBaseId=self.knowledge_base_id,
                dataSourceId=self.data_source_id
            )
            
            current_config = current_ds['dataSource']['dataSourceConfiguration'].copy()
            current_name = current_ds['dataSource'].get('name', f'fixtures-{self.catalogue_name}-kb-source')
            current_description = current_ds['dataSource'].get('description', 'Catalogue-specific data source')
            
            seed_urls = [{'url': url} for url in urls]
            
            if current_config.get('type') == 'WEB' and 'webConfiguration' in current_config:
                current_config['webConfiguration']['sourceConfiguration']['urlConfiguration']['seedUrls'] = seed_urls
                print("✅ Updated webConfiguration with new URLs")
            else:
                raise Exception(f"Data source is not a web crawler (type: {current_config.get('type')})")
            
            try:
                self.bedrock_agent_client.update_data_source(
                    knowledgeBaseId=self.knowledge_base_id,
                    dataSourceId=self.data_source_id,
                    name=current_name,
                    description=f"{current_description} - Updated {self.catalogue_name} URLs",
                    dataSourceConfiguration=current_config
                )
                print("✅ Data source URLs updated successfully")
                
            except Exception as update_error:
                if "vectorIngestionConfiguration" in str(update_error):
                    print("⚠️ Trying update without vectorIngestionConfiguration...")
                    try:
                        self.bedrock_agent_client.update_data_source(
                            knowledgeBaseId=self.knowledge_base_id,
                            dataSourceId=self.data_source_id,
                            name=current_name,
                            description=f"{current_description} - Updated {self.catalogue_name} URLs",
                            dataSourceConfiguration=current_config
                        )
                        print("✅ Data source URLs updated successfully (without vector config)")
                    except Exception as second_error:
                        print(f"❌ Still failed: {str(second_error)}")
                        print("🔄 Will only trigger ingestion job with existing URLs")
                else:
                    raise update_error
            
        except Exception as e:
            print(f"❌ Error updating data source URLs: {str(e)}")
            print("🔄 Continuing with ingestion job using existing configuration...")
    
    def _create_catalogue_data_source(self):

        
        try:
            # Load base knowledge first
            all_urls = []
            if os.path.exists(self.base_fixtures_dir) and self.catalogue_name != 'base':
                base_urls = self._load_catalogue_urls(self.base_fixtures_dir)
                all_urls.extend(base_urls)
            
            # Load catalogue-specific knowledge
            catalogue_urls = self._load_catalogue_urls(self.ai_kb_fixtures_dir)
            all_urls.extend(catalogue_urls)
            
            # Validate and clean URLs
            validated_urls = self._validate_and_clean_urls(all_urls)
            # Remove duplicates while preserving order
            final_urls = self._deduplicate_urls(validated_urls)
            
            # Check character limit (your proven working limit)
            if len(final_urls) > 50:  # Reasonable URL count safety check
                print(f"⚠️  WARNING: Very large URL count ({len(final_urls)}) - may slow down ingestion")
            
            total_chars = sum(len(url) for url in final_urls)
            if total_chars > 3000:
                print(f"❌ ERROR: Total URLs exceed 3000 char limit ({total_chars} chars)")
                print(f"💡 Please reduce URLs in your JSON files before creating data source")
                raise Exception(f"URL character limit exceeded: {total_chars} > 3000")
            
            seed_urls = [{'url': url} for url in final_urls]
            print(f"✅ Creating data source with {len(final_urls)} URLs ({total_chars} chars)")
            
            claude_model_id = lnineConnectors.getBedrockModelId()
            region = lnineConnectors.getRegionName() or 'ca-central-1'
            
            response = self.bedrock_agent_client.create_data_source(
                knowledgeBaseId=self.knowledge_base_id,
                name=f"fixtures-{self.catalogue_name}-kb-source",
                description=f"Fixture-managed data source for {self.catalogue_name} catalogue with Claude parser",
                dataSourceConfiguration={
                    'type': 'WEB',
                    'webConfiguration': {
                        'sourceConfiguration': {
                            'urlConfiguration': {
                                'seedUrls': seed_urls
                            }
                        },
                        'crawlerConfiguration': {
                            'crawlerLimits': {
                                'rateLimit': 300
                            },
                            'inclusionFilters': [
                                '.*'
                            ]
                        }
                    }
                },
                vectorIngestionConfiguration={
                    'parsingConfiguration': {
                        'parsingStrategy': 'BEDROCK_FOUNDATION_MODEL',
                        'bedrockFoundationModelConfiguration': {
                            'modelArn': f"arn:aws:bedrock:{region}::foundation-model/{claude_model_id}"
                        }
                    }
                }
            )
            
            data_source_id = response['dataSource']['dataSourceId']
            print(f"✅ Created new catalogue data source: {data_source_id}")
            print(f"🤖 Using Claude ({claude_model_id}) as foundation model parser")
            print(f"💡 This data source is specifically for {self.catalogue_name} fixtures")
            
            return data_source_id
            
        except Exception as e:
            print(f"❌ Error creating catalogue data source: {str(e)}")
            raise
    
    def _load_django_fixtures(self):
        try:
            # Load base Django fixtures first
            if os.path.exists(self.base_fixtures_dir) and self.catalogue_name != 'base':
                self._load_django_fixtures_from_dir(self.base_fixtures_dir, "base")
            
            # Load catalogue-specific Django fixtures
            self._load_django_fixtures_from_dir(self.ai_kb_fixtures_dir, self.catalogue_name)
            
            self._update_compliance_framework()
            
        except Exception as e:
            print(f"⚠️ Error loading Django fixtures: {str(e)}")
    
    def _load_django_fixtures_from_dir(self, directory, dir_name):
        try:
            django_fixtures = [
                f for f in os.listdir(directory)
                if f.endswith('.json') and f.startswith('django_')
            ]
            
            if django_fixtures:
                print(f"📋 Loading Django fixtures from {dir_name}...")
                
                pre_save.disconnect(pre_save_handler)
                post_save.disconnect(post_save_handler)
                post_delete.disconnect(post_delete_handler)
                
                try:
                    django_fixtures.sort()
                    
                    for fixture_file in django_fixtures:
                        print(f"  📄 Loading Django fixture: {fixture_file}")
                        call_command('loaddata', os.path.join(directory, fixture_file))
                        
                finally:
                    pre_save.connect(pre_save_handler)
                    post_save.connect(post_save_handler)
                    post_delete.connect(post_delete_handler)
                    
                print(f"  ✅ Django fixtures from {dir_name} loaded successfully")
            
        except Exception as e:
            print(f"⚠️ Error loading Django fixtures from {dir_name}: {str(e)}")
    
    def _update_compliance_framework(self):
        try:
            catalogue_info_path = os.path.join(self.ai_kb_fixtures_dir, 'catalogue_info.json')
            
            if os.path.exists(catalogue_info_path):
                with open(catalogue_info_path, 'r') as f:
                    catalogue_info = json.load(f)
                
                framework, created = ComplianceFramework.objects.get_or_create(
                    name=catalogue_info.get('name', self.catalogue_name.upper().replace('-', ' ')),
                    defaults={
                        'description': catalogue_info.get('description', f'Catalogue loaded from AI Knowledge Base'),
                        'version': catalogue_info.get('version', '1.0')
                    }
                )
                
                if created:
                    print(f"✅ Created compliance framework: {framework.name}")
                else:
                    print(f"📋 Compliance framework already exists: {framework.name}")
            
        except Exception as e:
            print(f"⚠️ Error updating compliance framework: {str(e)}")
    
    def _start_ingestion_job(self):
        try:
            print("⚡ Starting AWS Bedrock ingestion job...")
            print("📌 REMINDER: Bedrock will now crawl all URLs and vectorize the content")
            
            response = self.bedrock_agent_client.start_ingestion_job(
                knowledgeBaseId=self.knowledge_base_id,
                dataSourceId=self.data_source_id,
                description=f"LNine AI Knowledge Base ingestion - {self.catalogue_name} catalogue"
            )
            
            job_id = response['ingestionJob']['ingestionJobId']
            print(f"✅ Ingestion job started: {job_id}")
            
            return job_id
            
        except Exception as e:
            print(f"❌ Error starting ingestion job: {str(e)}")
            raise
    
    def _list_available_catalogues(self):
        base_path = 'fixtures/ai_kb'
        if os.path.exists(base_path):
            catalogues = [
                d for d in os.listdir(base_path)
                if os.path.isdir(os.path.join(base_path, d)) and not d.startswith('.')
            ]
            
            for catalogue in sorted(catalogues):
                print(f"  - {catalogue}")
        else:
            print("  (No ai_kb fixtures directory found)")


def load_ai_knowledge_base():
    print("🔧 AI Knowledge Base Loader")
    print("📌 REMINDER: This updates the SAME AWS Bedrock data source each time")
    print("📌 REMINDER: Each run completely replaces the previous content")
    print("=" * 60)
    
    # Get available catalogues dynamically
    base_path = 'fixtures/ai_kb'
    available_catalogues = []
    if os.path.exists(base_path):
        available_catalogues = [
            d for d in os.listdir(base_path)
            if os.path.isdir(os.path.join(base_path, d)) and not d.startswith('.')
        ]
        available_catalogues.sort()

    if len(sys.argv) > 1:
        catalogue_name = sys.argv[1].lower().strip()
        
        if catalogue_name == 'base':
            print(f'📚 Loading base AI knowledge base fixtures (available to all customers)...')
        elif catalogue_name == 'nist-800-53':
            print(f'🛡️ Loading NIST 800-53 knowledge base fixtures...')
        elif catalogue_name == 'itsg-33':
            print(f'🇨🇦 Loading ITSG-33 knowledge base fixtures...')
        elif catalogue_name == 'fedramp':
            print(f'🏛️ Loading FedRAMP knowledge base fixtures...')
        elif catalogue_name == 'general':
            print(f'🌐 Loading general cybersecurity knowledge base fixtures...')
        elif catalogue_name == 'all':
            print(f'🚀 Loading all available catalogues...')
        elif catalogue_name in available_catalogues:
            print(f'📋 Loading {catalogue_name} knowledge base fixtures...')
        else:
            print(f'❌ Unknown catalogue: {catalogue_name}')
            print(f"\nAvailable catalogues: {', '.join(available_catalogues)}")
            sys.exit(1)

    else:
        print("❌ Usage: python3 loadAIKB.py <catalogue-name>")
        print("\nExamples:")
        print("  python3 loadAIKB.py \"base\"")
        print("  python3 loadAIKB.py \"nist-800-53\"")
        print("  python3 loadAIKB.py \"itsg-33\"")
        print("  python3 loadAIKB.py \"fedramp\"")
        print("  python3 loadAIKB.py \"general\"")
        print("  python3 loadAIKB.py \"all\"")
        print(f"\nAvailable catalogues: {', '.join(available_catalogues)}")
        sys.exit(1)
    
    try:
        if catalogue_name != "all":
            loader = AIKnowledgeBaseLoader(catalogue_name)
            success = loader.load_catalogue()
        else:
            print("⚠️  WARNING: 'all' mode loads each catalogue SEPARATELY to avoid 3000 char limit")
            print("📌 REMINDER: Each catalogue will REPLACE the previous one in the data source")
            print("💡 Consider running individual catalogues instead of 'all' for production use")
            
            success = True
            for catalogue in available_catalogues:
                print(f"\n{'='*60}")
                print(f"📋 Loading catalogue: {catalogue} (will replace previous)")
                print(f"{'='*60}")
                
                loader = AIKnowledgeBaseLoader(catalogue)
                catalogue_success = loader.load_catalogue()
                
                if not catalogue_success:
                    print(f"❌ Failed to load catalogue: {catalogue}")
                    success = False
                else:
                    print(f"✅ Successfully loaded catalogue: {catalogue}")
                
                # Small delay between catalogues when loading all
                if catalogue != available_catalogues[-1]:
                    print("⏳ Waiting 60 seconds before next catalogue...")
                    time.sleep(60)
        
        if success:
            if catalogue_name == "all":
                print(f"\n🎉 AI Knowledge Base 'all' mode completed!")
                print(f"📌 FINAL STATE: Data source contains the LAST catalogue ({available_catalogues[-1]}) + base knowledge")
                print(f"💡 Each catalogue was loaded sequentially and replaced the previous one")
            else:
                print(f"\n🎉 AI Knowledge Base loading completed successfully!")
                print(f"📌 REMINDER: The data source now contains {catalogue_name} + base knowledge")
        else:
            print(f"\n❌ AI Knowledge Base loading failed!")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Error loading AI Knowledge Base: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    load_ai_knowledge_base()