"""Tracking: persist token snapshots and milestone hits (e.g. market cap >= 50k).

This module is designed to be lightweight and safe to use in a live stream:
- never raises exceptions to callers (handled in history.py)
- uses SQLite (single file) in data/processed/
"""
