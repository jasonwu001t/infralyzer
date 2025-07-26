"""
Shared AWS Authentication utilities for DE Polars
"""
import boto3
from typing import Optional, Dict, Any
from datetime import datetime, timezone


def check_credential_expiration(expiration: Optional[str] = None):
    """Check if temporary credentials are expired or expiring soon."""
    if not expiration:
        return
        
    try:
        import re
        
        # Parse expiration timestamp (handle different formats)
        if isinstance(expiration, str):
            # Try to parse ISO format: 2025-01-15T10:30:00Z or 2025-01-15T10:30:00+00:00
            expiration_dt = datetime.fromisoformat(expiration.replace('Z', '+00:00'))
        else:
            # Assume it's already a datetime object
            expiration_dt = expiration
            
        # Ensure timezone awareness
        if expiration_dt.tzinfo is None:
            expiration_dt = expiration_dt.replace(tzinfo=timezone.utc)
            
        # Check against current time
        now = datetime.now(timezone.utc)
        time_until_expiry = expiration_dt - now
        
        if time_until_expiry.total_seconds() <= 0:
            print(f"⚠️  WARNING: AWS credentials expired at {expiration_dt}")
            print("   You may encounter authentication errors. Please refresh your credentials.")
        elif time_until_expiry.total_seconds() <= 300:  # 5 minutes
            minutes_left = int(time_until_expiry.total_seconds() / 60)
            print(f"⚠️  WARNING: AWS credentials expire in {minutes_left} minutes at {expiration_dt}")
            print("   Consider refreshing your credentials soon.")
        elif time_until_expiry.total_seconds() <= 900:  # 15 minutes
            minutes_left = int(time_until_expiry.total_seconds() / 60)
            print(f"ℹ️  INFO: AWS credentials expire in {minutes_left} minutes at {expiration_dt}")
            
    except Exception as e:
        print(f"⚠️  Warning: Could not parse expiration timestamp '{expiration}': {e}")
        print("   Expected format: ISO 8601 (e.g., '2025-01-15T10:30:00Z')")


def get_boto3_client(service_name: str,
                     aws_region: Optional[str] = None,
                     aws_access_key_id: Optional[str] = None,
                     aws_secret_access_key: Optional[str] = None,
                     aws_session_token: Optional[str] = None,
                     aws_profile: Optional[str] = None,
                     role_arn: Optional[str] = None,
                     external_id: Optional[str] = None):
    """Create boto3 client with enhanced authentication support."""
    from botocore.exceptions import ClientError
    
    # Method 1: Use AWS profile if specified
    if aws_profile:
        session = boto3.Session(profile_name=aws_profile)
        return session.client(service_name, region_name=aws_region)
    
    # Method 2: Use role assumption if role_arn specified
    if role_arn:
        sts_client = boto3.client('sts')
        assume_role_kwargs = {
            'RoleArn': role_arn,
            'RoleSessionName': 'de-polars-session'
        }
        if external_id:
            assume_role_kwargs['ExternalId'] = external_id
            
        try:
            response = sts_client.assume_role(**assume_role_kwargs)
            credentials = response['Credentials']
            return boto3.client(
                service_name,
                region_name=aws_region,
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken']
            )
        except ClientError as e:
            raise ValueError(f"Failed to assume role {role_arn}: {e}")
    
    # Method 3: Use explicit credentials (including session token)
    client_kwargs = {}
    if aws_region:
        client_kwargs['region_name'] = aws_region
    if aws_access_key_id:
        client_kwargs['aws_access_key_id'] = aws_access_key_id
    if aws_secret_access_key:
        client_kwargs['aws_secret_access_key'] = aws_secret_access_key
    if aws_session_token:
        client_kwargs['aws_session_token'] = aws_session_token
        
    # Method 4: Fall back to default credential chain (environment, IAM role, etc.)
    return boto3.client(service_name, **client_kwargs)


def get_storage_options(aws_region: Optional[str] = None,
                       aws_access_key_id: Optional[str] = None,
                       aws_secret_access_key: Optional[str] = None,
                       aws_session_token: Optional[str] = None,
                       role_arn: Optional[str] = None,
                       external_id: Optional[str] = None) -> Dict[str, Any]:
    """Get storage options for S3 authentication in polars."""
    options = {}
    
    # Add AWS region if specified
    if aws_region:
        options['aws_region'] = aws_region
        
    # Add explicit credentials if provided
    if aws_access_key_id:
        options['aws_access_key_id'] = aws_access_key_id
    if aws_secret_access_key:
        options['aws_secret_access_key'] = aws_secret_access_key
    if aws_session_token:
        options['aws_session_token'] = aws_session_token
        
    # For role assumption, get temporary credentials
    if role_arn and not aws_access_key_id:
        try:
            import boto3
            sts_client = boto3.client('sts')
            assume_role_kwargs = {
                'RoleArn': role_arn,
                'RoleSessionName': 'de-polars-data-session'
            }
            if external_id:
                assume_role_kwargs['ExternalId'] = external_id
                
            response = sts_client.assume_role(**assume_role_kwargs)
            credentials = response['Credentials']
            options['aws_access_key_id'] = credentials['AccessKeyId']
            options['aws_secret_access_key'] = credentials['SecretAccessKey']
            options['aws_session_token'] = credentials['SessionToken']
        except Exception as e:
            print(f"⚠️  Warning: Failed to get role credentials for data access: {e}")
            print("    Falling back to default credential chain...")
    
    return options 