"""E2E test configuration.

These tests require local infrastructure (docker compose) and the control
plane running.  The pytestmark in each test module handles skipping when
services are unavailable.
"""
