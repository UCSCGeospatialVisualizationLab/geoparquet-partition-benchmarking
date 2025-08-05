#!/usr/bin/env python3
"""
S3 Bucket Structure Setup Script
Creates and configures S3 bucket structure for partitioned datasets with proper HTTP access
"""

import boto3
import yaml
import json
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

# Load environment variables from .env file
load_dotenv()


def load_config():
    """Load S3 configuration"""
    config_path = Path(__file__).parent.parent.parent / "s3_config" / "s3_config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_s3_client():
    """Get S3 client with proper endpoint configuration"""
    endpoint_url = os.getenv("AWS_ENDPOINT_URL")
    if endpoint_url:
        print(f"🔗 Using Ceph S3 endpoint: {endpoint_url}")
        return boto3.client("s3", endpoint_url=endpoint_url)
    else:
        print("🔗 Using default AWS S3")
        return boto3.client("s3")


def create_bucket_if_not_exists(s3_client, bucket_name):
    """Create S3 bucket if it doesn't exist"""
    print(f"🪣 Setting up bucket: {bucket_name}")

    try:
        # Check if bucket exists
        s3_client.head_bucket(Bucket=bucket_name)
        print("   ✅ Bucket already exists")
        return True

    except Exception as e:
        # Handle both NoSuchBucket and 404 errors as "bucket doesn't exist"
        if "NoSuchBucket" in str(e) or "404" in str(e) or "Not Found" in str(e):
            print("   📦 Bucket doesn't exist, creating new bucket...")
            try:
                # Create bucket
                s3_client.create_bucket(Bucket=bucket_name)
                print("   ✅ Bucket created successfully")
                return True

            except Exception as create_error:
                print(f"   ❌ Failed to create bucket: {create_error}")
                return False
        else:
            print(f"   ❌ Error checking bucket: {e}")
            return False


def setup_bucket_policy(s3_client, bucket_name, config):
    """Set up bucket policy for public read access"""
    print("🔒 Configuring bucket policy for HTTP access...")

    # Create policy for public read access
    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PublicReadGetObject",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": f"arn:aws:s3:::{bucket_name}/*",
            }
        ],
    }

    try:
        s3_client.put_bucket_policy(
            Bucket=bucket_name, Policy=json.dumps(bucket_policy)
        )
        print("   ✅ Bucket policy set for public read access")
        return True

    except Exception as e:
        print(f"   ⚠️  Failed to set bucket policy: {e}")
        print("   ℹ️  This may be expected with some S3 providers")
        return False


def setup_cors_policy(s3_client, bucket_name, config):
    """Set up CORS policy for web access"""
    print("🌐 Configuring CORS policy...")

    if not config.get("http_config", {}).get("cors_enabled", False):
        print("   ⏭️  CORS disabled in config, skipping")
        return True

    cors_configuration = {
        "CORSRules": [
            {
                "AllowedHeaders": ["*"],
                "AllowedMethods": ["GET", "HEAD"],
                "AllowedOrigins": ["*"],
                "ExposeHeaders": ["ETag", "Content-Length", "Content-Type"],
                "MaxAgeSeconds": 3600,
            }
        ]
    }

    try:
        s3_client.put_bucket_cors(
            Bucket=bucket_name, CORSConfiguration=cors_configuration
        )
        print("   ✅ CORS policy configured")
        return True

    except Exception as e:
        print(f"   ⚠️  Failed to set CORS policy: {e}")
        print("   ℹ️  This may be expected with some S3 providers")
        return False


def create_directory_structure(s3_client, bucket_name, config):
    """Create directory structure for all partitioning strategies"""
    print("📁 Creating directory structure...")

    prefix = config["test_bucket"]["prefix"]
    strategies = config["partitioning_strategies"]

    # Create a small placeholder file to establish directories
    placeholder_content = b"# Placeholder file to establish directory structure\n"

    for strategy_name, strategy_config in strategies.items():
        strategy_path = strategy_config["path"]
        full_path = f"{prefix}{strategy_path}"

        print(f"   📂 Setting up: {full_path}")

        try:
            # Create placeholder file to establish directory
            placeholder_key = f"{full_path}.placeholder"
            s3_client.put_object(
                Bucket=bucket_name,
                Key=placeholder_key,
                Body=placeholder_content,
                ContentType="text/plain",
            )
            print("      ✅ Directory structure created")

        except Exception as e:
            print(f"      ❌ Failed to create directory: {e}")
            return False

    return True


def test_bucket_access(s3_client, bucket_name, config):
    """Test bucket access for read and write operations"""
    print("🧪 Testing bucket access...")

    prefix = config["test_bucket"]["prefix"]
    test_key = f"{prefix}test_access.txt"
    test_content = b"Test file for bucket access verification"

    try:
        # Test write access
        print("   📝 Testing write access...")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=test_key,
            Body=test_content,
            ContentType="text/plain",
        )
        print("      ✅ Write access OK")

        # Test read access
        print("   📖 Testing read access...")
        response = s3_client.get_object(Bucket=bucket_name, Key=test_key)
        retrieved_content = response["Body"].read()

        if retrieved_content == test_content:
            print("      ✅ Read access OK")
        else:
            print("      ⚠️  Read access issue: content mismatch")

        # Clean up test file
        s3_client.delete_object(Bucket=bucket_name, Key=test_key)
        print("      🧹 Test file cleaned up")

        return True

    except Exception as e:
        print(f"   ❌ Access test failed: {e}")
        return False


def generate_http_urls(bucket_name, config):
    """Generate HTTP URLs for accessing partitioned data"""
    print("🔗 Generating HTTP access URLs...")

    endpoint_url = os.getenv("AWS_ENDPOINT_URL", "https://s3.amazonaws.com")
    prefix = config["test_bucket"]["prefix"]
    strategies = config["partitioning_strategies"]

    urls = {}

    for strategy_name, strategy_config in strategies.items():
        strategy_path = strategy_config["path"]
        full_path = f"{prefix}{strategy_path}"

        # Generate HTTP URL
        if endpoint_url.endswith("/"):
            endpoint_url = endpoint_url[:-1]

        http_url = f"{endpoint_url}/{bucket_name}/{full_path}"
        urls[strategy_name] = {
            "http_url": http_url,
            "s3_url": f"s3://{bucket_name}/{full_path}",
            "description": strategy_config["description"],
        }

        print(f"   📋 {strategy_name.upper()}:")
        print(f"      S3: s3://{bucket_name}/{full_path}")
        print(f"      HTTP: {http_url}")

    return urls


def save_setup_results(bucket_name, config, urls):
    """Save setup results for reference"""
    results_dir = Path(__file__).parent.parent.parent / "results"
    results_dir.mkdir(exist_ok=True)

    setup_data = {
        "timestamp": __import__("time").strftime("%Y-%m-%d %H:%M:%S"),
        "bucket_name": bucket_name,
        "endpoint_url": os.getenv("AWS_ENDPOINT_URL"),
        "bucket_config": config["test_bucket"],
        "partitioning_strategies": config["partitioning_strategies"],
        "access_urls": urls,
    }

    output_file = results_dir / "s3_bucket_setup.json"
    with open(output_file, "w") as f:
        json.dump(setup_data, f, indent=2, default=str)

    print(f"\n💾 Setup results saved to: {output_file}")


def main():
    """Main setup workflow"""
    print("🚀 S3 Bucket Structure Setup")
    print("=" * 40)

    # Load configuration
    config = load_config()
    bucket_name = config["test_bucket"]["bucket"]

    print(f"📋 Target bucket: {bucket_name}")
    print(f"🔗 Endpoint: {os.getenv('AWS_ENDPOINT_URL', 'Default AWS S3')}")

    # Get S3 client
    s3_client = get_s3_client()
    
    # Verify bucket access (bucket should already exist)
    print("🔍 Verifying access to existing bucket...")
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print("   ✅ Bucket access confirmed")
    except Exception as e:
        print(f"   ❌ Cannot access bucket: {e}")
        print("   💡 Make sure you have proper credentials for the bucket")
        return 1
    
    # Skip bucket policy setup - assuming same configuration as source bucket
    print("🔒 Skipping bucket policy setup (using existing bucket configuration)")

    # Create directory structure
    if not create_directory_structure(s3_client, bucket_name, config):
        print("❌ Failed to create directory structure")
        return 1

    # Test access
    if not test_bucket_access(s3_client, bucket_name, config):
        print("❌ Bucket access test failed")
        return 1

    # Generate access URLs
    urls = generate_http_urls(bucket_name, config)

    # Save results
    save_setup_results(bucket_name, config, urls)

    print("\n✅ S3 Bucket Setup Complete!")
    print("\nNext steps:")
    print("1. Verify HTTP access to generated URLs")
    print("2. Proceed to Step 4: Partitioning Implementation")
    print("3. Use the saved URLs for HTTP-based data access")

    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
