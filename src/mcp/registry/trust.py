"""
Trust and Security Manager for Service Registry

This module implements trust boundaries, consent management, and
security verification for services in the registry.
"""
import os
import json
import logging
import time
import hashlib
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Set
from dataclasses import dataclass, field
from pathlib import Path
import threading

logger = logging.getLogger(__name__)

@dataclass
class ConsentRecord:
    """Record of user consent for a service."""
    service_id: str
    permissions: List[str]
    granted_at: datetime = field(default_factory=datetime.now)
    expires_at: Optional[datetime] = None
    granted_by: str = "user"
    context: Dict[str, Any] = field(default_factory=dict)
    consent_id: str = field(default_factory=lambda: str(uuid.uuid4()))

@dataclass
class VerificationRecord:
    """Verification record for a service."""
    service_id: str
    verification_type: str  # "signature", "domain", "local", etc.
    verified_at: datetime = field(default_factory=datetime.now)
    expires_at: Optional[datetime] = None
    verification_data: Dict[str, Any] = field(default_factory=dict)
    verified: bool = False

class TrustManager:
    """
    Trust Manager for Service Registry.
    
    Handles service verification, consent management, and security boundaries.
    """
    
    def __init__(self, storage_path: Optional[str] = None):
        """Initialize the trust manager."""
        self.consents: Dict[str, ConsentRecord] = {}
        self.verifications: Dict[str, VerificationRecord] = {}
        self.trusted_sources: Set[str] = set()
        self.storage_path = storage_path
        self.lock = threading.RLock()
        
        # Configure default trusted sources
        self._configure_default_trusted_sources()
        
        # Load existing records if storage path provided
        if storage_path:
            self._load_records()
        
        logger.info("Trust Manager initialized")
    
    def _configure_default_trusted_sources(self):
        """Configure default trusted sources."""
        # Local trusted sources
        self.trusted_sources.add("localhost")
        self.trusted_sources.add("127.0.0.1")
        
        # Add any environment-specified trusted sources
        trusted_env = os.environ.get("TRUSTED_SOURCES", "")
        if trusted_env:
            for source in trusted_env.split(","):
                source = source.strip()
                if source:
                    self.trusted_sources.add(source)
    
    def _load_records(self):
        """Load consent and verification records from storage."""
        consent_path = self._get_consent_path()
        verification_path = self._get_verification_path()
        
        if os.path.exists(consent_path):
            try:
                with open(consent_path, 'r') as f:
                    consent_data = json.load(f)
                
                for record in consent_data.get("consents", []):
                    consent = ConsentRecord(
                        service_id=record["service_id"],
                        permissions=record["permissions"],
                        granted_at=datetime.fromisoformat(record["granted_at"]),
                        expires_at=datetime.fromisoformat(record["expires_at"]) if record.get("expires_at") else None,
                        granted_by=record.get("granted_by", "user"),
                        context=record.get("context", {}),
                        consent_id=record.get("consent_id", str(uuid.uuid4()))
                    )
                    self.consents[consent.service_id] = consent
                
                logger.info(f"Loaded {len(self.consents)} consent records")
            except Exception as e:
                logger.error(f"Error loading consent records: {str(e)}")
        
        if os.path.exists(verification_path):
            try:
                with open(verification_path, 'r') as f:
                    verification_data = json.load(f)
                
                for record in verification_data.get("verifications", []):
                    verification = VerificationRecord(
                        service_id=record["service_id"],
                        verification_type=record["verification_type"],
                        verified_at=datetime.fromisoformat(record["verified_at"]),
                        expires_at=datetime.fromisoformat(record["expires_at"]) if record.get("expires_at") else None,
                        verification_data=record.get("verification_data", {}),
                        verified=record.get("verified", False)
                    )
                    self.verifications[verification.service_id] = verification
                
                logger.info(f"Loaded {len(self.verifications)} verification records")
            except Exception as e:
                logger.error(f"Error loading verification records: {str(e)}")
    
    def _save_records(self):
        """Save consent and verification records to storage."""
        if not self.storage_path:
            return
        
        os.makedirs(self.storage_path, exist_ok=True)
        
        consent_path = self._get_consent_path()
        verification_path = self._get_verification_path()
        
        try:
            with open(consent_path, 'w') as f:
                consent_data = {
                    "consents": [
                        {
                            "service_id": c.service_id,
                            "permissions": c.permissions,
                            "granted_at": c.granted_at.isoformat(),
                            "expires_at": c.expires_at.isoformat() if c.expires_at else None,
                            "granted_by": c.granted_by,
                            "context": c.context,
                            "consent_id": c.consent_id
                        }
                        for c in self.consents.values()
                    ]
                }
                json.dump(consent_data, f, indent=2)
            
            with open(verification_path, 'w') as f:
                verification_data = {
                    "verifications": [
                        {
                            "service_id": v.service_id,
                            "verification_type": v.verification_type,
                            "verified_at": v.verified_at.isoformat(),
                            "expires_at": v.expires_at.isoformat() if v.expires_at else None,
                            "verification_data": v.verification_data,
                            "verified": v.verified
                        }
                        for v in self.verifications.values()
                    ]
                }
                json.dump(verification_data, f, indent=2)
            
            logger.info("Trust records saved")
        except Exception as e:
            logger.error(f"Error saving trust records: {str(e)}")
    
    def _get_consent_path(self) -> str:
        """Get the path to the consent record file."""
        return os.path.join(self.storage_path, "consents.json")
    
    def _get_verification_path(self) -> str:
        """Get the path to the verification record file."""
        return os.path.join(self.storage_path, "verifications.json")
    
    def verify_service(self, service_id: str, verification_data: Dict[str, Any]) -> bool:
        """
        Verify a service based on provided verification data.
        
        Args:
            service_id: ID of the service to verify
            verification_data: Data used for verification
                
        Returns:
            True if service is verified, False otherwise
        """
        with self.lock:
            # Determine verification type
            verification_type = verification_data.get("type", "unknown")
            
            result = False
            
            # Verify based on type
            if verification_type == "local":
                # Local services are trusted if they're running on localhost
                host = verification_data.get("host", "")
                result = host in self.trusted_sources
            
            elif verification_type == "domain":
                # Domain verification checks if the service URL matches a trusted domain
                domain = verification_data.get("domain", "")
                result = any(domain.endswith(trusted) for trusted in self.trusted_sources)
            
            elif verification_type == "signature":
                # Signature verification checks a cryptographic signature
                # This would involve more complex signature validation logic
                signature = verification_data.get("signature", "")
                public_key = verification_data.get("public_key", "")
                data = verification_data.get("data", "")
                
                # Placeholder for signature verification
                # In a real implementation, this would validate the signature
                result = len(signature) > 0 and len(public_key) > 0 and len(data) > 0
            
            # Create or update verification record
            verification = VerificationRecord(
                service_id=service_id,
                verification_type=verification_type,
                verified_at=datetime.now(),
                verification_data=verification_data,
                verified=result
            )
            
            # Set expiration if specified
            if verification_data.get("expires_in"):
                expires_in = verification_data["expires_in"]
                verification.expires_at = datetime.now() + timedelta(seconds=expires_in)
            
            self.verifications[service_id] = verification
            
            # Save records
            self._save_records()
            
            logger.info(f"Service {service_id} verification result: {result}")
            return result
    
    def request_consent(self, service_id: str, permissions: List[str], 
                      context: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """
        Request user consent for a service to access specific permissions.
        
        In a real application, this would show a UI prompt to the user.
        For our implementation, it automatically grants consent for demo purposes.
        
        Args:
            service_id: ID of the service requesting consent
            permissions: List of permissions requested
            context: Optional context information
                
        Returns:
            Consent ID if granted, None if denied
        """
        with self.lock:
            # In a real app, this would show a UI prompt
            # For demo purposes, we'll auto-grant consent
            
            context = context or {}
            granted = True  # In real app, this would be user decision
            
            if granted:
                consent = ConsentRecord(
                    service_id=service_id,
                    permissions=permissions,
                    granted_at=datetime.now(),
                    granted_by="auto",  # In real app, this would be user ID
                    context=context
                )
                
                # Set expiration if needed
                if context.get("expires_in"):
                    expires_in = context["expires_in"]
                    consent.expires_at = datetime.now() + timedelta(seconds=expires_in)
                
                self.consents[service_id] = consent
                
                # Save records
                self._save_records()
                
                logger.info(f"Consent granted for service {service_id}: {permissions}")
                return consent.consent_id
            else:
                logger.info(f"Consent denied for service {service_id}")
                return None
    
    def check_consent(self, service_id: str, permission: str) -> bool:
        """
        Check if consent has been granted for a specific permission.
        
        Args:
            service_id: ID of the service to check
            permission: Permission to check
                
        Returns:
            True if consent granted, False otherwise
        """
        with self.lock:
            if service_id not in self.consents:
                return False
            
            consent = self.consents[service_id]
            
            # Check if consent has expired
            if consent.expires_at and consent.expires_at < datetime.now():
                logger.info(f"Consent for service {service_id} has expired")
                return False
            
            # Check if permission is granted
            has_permission = permission in consent.permissions
            return has_permission
    
    def revoke_consent(self, service_id: str) -> bool:
        """
        Revoke consent for a service.
        
        Args:
            service_id: ID of the service
                
        Returns:
            True if consent was revoked, False if no consent existed
        """
        with self.lock:
            if service_id not in self.consents:
                return False
            
            del self.consents[service_id]
            
            # Save records
            self._save_records()
            
            logger.info(f"Consent revoked for service {service_id}")
            return True
    
    def is_service_verified(self, service_id: str) -> bool:
        """
        Check if a service has been verified.
        
        Args:
            service_id: ID of the service to check
                
        Returns:
            True if service is verified, False otherwise
        """
        with self.lock:
            if service_id not in self.verifications:
                return False
            
            verification = self.verifications[service_id]
            
            # Check if verification has expired
            if verification.expires_at and verification.expires_at < datetime.now():
                logger.info(f"Verification for service {service_id} has expired")
                return False
            
            return verification.verified
    
    def add_trusted_source(self, source: str) -> bool:
        """
        Add a trusted source for service verification.
        
        Args:
            source: Source to trust (domain, IP, etc.)
                
        Returns:
            True if source was added, False if already trusted
        """
        with self.lock:
            if source in self.trusted_sources:
                return False
            
            self.trusted_sources.add(source)
            logger.info(f"Added trusted source: {source}")
            return True
    
    def remove_trusted_source(self, source: str) -> bool:
        """
        Remove a trusted source.
        
        Args:
            source: Source to remove
                
        Returns:
            True if source was removed, False if not trusted
        """
        with self.lock:
            if source not in self.trusted_sources:
                return False
            
            self.trusted_sources.remove(source)
            logger.info(f"Removed trusted source: {source}")
            return True
    
    def get_trusted_sources(self) -> List[str]:
        """Get list of trusted sources."""
        with self.lock:
            return list(self.trusted_sources)


# Singleton instance
_trust_manager_instance = None

def get_trust_manager(storage_path: Optional[str] = None) -> TrustManager:
    """Get the singleton TrustManager instance."""
    global _trust_manager_instance
    if _trust_manager_instance is None:
        _trust_manager_instance = TrustManager(storage_path)
    return _trust_manager_instance