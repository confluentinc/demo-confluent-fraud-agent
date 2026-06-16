"""Data-model reference for the demo.

These Pydantic models document the shape of the events the producer emits and the
fraud alert the agent returns. The producer serializes with Avro (schemas in
producer/generate_events.py, matching the Flink-created tables); the Flink tables
and agent live in terraform/flink.tf. Nothing imports this module at runtime — it
is kept as the canonical description of the data model.
"""

from pydantic import BaseModel, Field
from typing import List, Optional


class Transaction(BaseModel):
    user_id: str
    transaction_id: str
    amount: float
    merchant: str
    merchant_category: str
    location: str
    timestamp: int


class UserLogin(BaseModel):
    user_id: str
    ip_address: str
    device_id: str
    location: str
    timestamp: int


class AccountChange(BaseModel):
    user_id: str
    field_changed: str
    old_value: str
    new_value: str
    timestamp: int


class UserActivityProfile(BaseModel):
    user_id: str
    transactions: List[dict] = Field(default_factory=list)
    logins: List[dict] = Field(default_factory=list)
    account_changes: List[dict] = Field(default_factory=list)
    window_start: int = 0
    window_end: int = 0


class FraudAlert(BaseModel):
    user_id: str
    risk_score: int
    reasoning: str
    actions_taken: List[str] = Field(default_factory=list)
    flagged_transaction_ids: List[str] = Field(default_factory=list)
