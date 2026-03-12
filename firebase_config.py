import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore import Client
import os
from typing import Optional

class FirebaseManager:
    """Central Firebase Firestore manager for the Curiosity system"""
    
    def __init__(self, credential_path: str = "firebase_credentials.json"):
        """
        Initialize Firebase connection with proper error handling
        
        Args:
            credential_path: Path to Firebase service account key
        """
        self.credential_path = credential_path
        self.app = None
        self.db: Optional[Client] = None
        self._initialize()
    
    def _initialize(self):
        """Initialize Firebase app with robust error handling"""
        try:
            if not os.path.exists(self.credential_path):
                raise FileNotFoundError(
                    f"Firebase credentials not found at {self.credential_path}. "
                    "Please create service account key from Firebase Console."
                )
            
            # Check if already initialized
            if not firebase_admin._apps:
                cred = credentials.Certificate(self.credential_path)
                self.app = firebase_admin.initialize_app(cred)
            
            self.db = firestore.client()
            print("✅ Firebase initialized successfully")
            
        except Exception as e:
            print(f"❌ Firebase initialization failed: {e}")
            raise
    
    def get_mission_state(self) -> dict:
        """Retrieve current mission state document"""
        if not self.db:
            raise ConnectionError("Firebase not initialized")
        
        try:
            doc_ref = self.db.collection("mission_state").document("current")
            doc = doc_ref.get()
            
            if doc.exists:
                return doc.to_dict()
            else:
                # Initialize default state
                default_state = {
                    "phase": "active",
                    "resource_pool": 100.00,
                    "hardware_fund": 0.00,
                    "upgrade_fund": 0.00,
                    "current_allocation": {
                        "trading_capital": 70.00,
                        "gig_reserve": 30.00,
                        "emergency_buffer": 0.00
                    },
                    "targets": {
                        "hardware_target": 250.00,
                        "next_upgrade_budget": 50.00
                    },
                    "performance_metrics": {
                        "trades_today": 0,
                        "win_rate_7d": 0.0,
                        "avg_gig_profit": 0.0,
                        "total_profit_generated": 0.0
                    },
                    "system_health": {
                        "last_trade_check": None,
                        "last_gig_scan": None,
                        "consecutive_losses": 0,
                        "trading_paused": False,
                        "last_error": None,
                        "uptime_hours": 0
                    }
                }
                doc_ref.set(default_state)
                return default_state
                
        except Exception as e:
            print(f"❌ Failed to get mission state: {e}")
            raise
    
    def update_field(self, collection: str, document: str, field: str, value):
        """Atomic field update with error handling"""
        try:
            doc_ref = self.db.collection(collection).document(document)
            doc_ref.update({field: value})
            return True
        except Exception as e:
            print(f"❌ Failed to update {collection}/{document}.{field}: {e}")
            return False
    
    def log_trade(self, trade_data: dict):
        """Log a trade with complete metadata"""
        try:
            trades_ref = self.db.collection("trades")
            trades_ref.add({
                **trade_data,
                "timestamp": firestore.SERVER_TIMESTAMP,
                "system_state": self.get_mission_state()["phase"]
            })
            print(f"📊 Trade logged: {trade_data.get('symbol', 'unknown')}")
        except Exception as e:
            print(f"❌ Failed to log trade: {e}")
    
    def log_performance(self, metric: str, value: float):
        """Update performance metrics with rolling average"""
        try:
            state = self.get_mission_state()
            current = state["performance_metrics"].get(metric, 0)
            
            # Simple exponential smoothing for metrics
            if metric.startswith("avg_"):
                new_value = (current * 0.7) + (value * 0.3)
            else:
                new_value = value
            
            self.update_field("mission_state", "current", 
                            f"performance_metrics.{metric}", new_value)
        except Exception as e:
            print(f"❌ Failed to log performance: {e}")

# Singleton instance
firebase_manager = FirebaseManager()