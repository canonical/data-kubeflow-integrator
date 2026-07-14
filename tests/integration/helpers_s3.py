#!/usr/bin/env python3

# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Helpers for provisioning an S3-compatible backend for integration tests."""

import dataclasses
import json
import logging
import os
import socket
import subprocess

from tenacity import retry, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class S3ConnectionInfo:
    """Connection details for an S3-compatible endpoint."""

    endpoint: str
    access_key: str
    secret_key: str
    region: str
    tls_ca_chain: str


def host_ip() -> str:
    """The IP address of the host running these tests."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.connect(("1.1.1.1", 80))
        return s.getsockname()[0]


@retry(stop=stop_after_attempt(20), wait=wait_fixed(3), reraise=True)
def wait_for_rgw_ready():
    """Wait for RADOS Gateway to be ready by checking if account list command succeeds."""
    subprocess.run(
        ["sudo", "microceph.radosgw-admin", "account", "list"],
        capture_output=True,
        check=True,
        encoding="utf-8",
    )


def install_microceph():
    """Install and bootstrap microceph if not already installed."""
    if subprocess.run(["snap", "list", "microceph"], capture_output=True).returncode == 0:
        logger.info("microceph already installed, skipping install and bootstrap")
        return

    logger.info("Installing microceph")
    subprocess.run(["sudo", "snap", "install", "microceph"], check=True)
    try:
        subprocess.run(
            ["sudo", "microceph", "cluster", "bootstrap"],
            check=True,
            stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as ex:
        logger.error(ex.stderr.decode())
        raise
    try:
        subprocess.run(
            ["sudo", "microceph", "disk", "add", "loop,1G,3"],
            check=True,
            stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as ex:
        logger.error(ex.stderr.decode())
        raise


def setup_radosgw():
    """Enable the RADOS Gateway over plain HTTP (no TLS)."""
    logger.info("Enabling RADOS Gateway")
    try:
        subprocess.run(
            ["sudo", "microceph", "enable", "rgw"],
            check=True,
            stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as ex:
        logger.warning(
            "microceph enable rgw failed (may already be enabled): %s", ex.stderr.decode()
        )

    wait_for_rgw_ready()


def create_user(host_ip: str) -> S3ConnectionInfo:
    """Create an S3 user, reusing existing credentials if present."""
    uid = "kfp-integration-user"
    result = subprocess.run(
        ["sudo", "microceph.radosgw-admin", "user", "info", "--uid", uid],
        capture_output=True,
        encoding="utf-8",
    )
    if result.returncode == 0:
        logger.info("S3 user %s already exists, reusing credentials", uid)
        key = json.loads(result.stdout)["keys"][0]
    else:
        logger.info("Creating S3 user %s...", uid)
        output = subprocess.run(
            [
                "sudo",
                "microceph.radosgw-admin",
                "user",
                "create",
                "--uid",
                uid,
                "--display-name",
                uid,
                "--gen-access-key",
                "--gen-secret",
            ],
            capture_output=True,
            check=True,
            encoding="utf-8",
        ).stdout
        key = json.loads(output)["keys"][0]

    return S3ConnectionInfo(
        endpoint=f"http://{host_ip}",
        access_key=key["access_key"],
        secret_key=key["secret_key"],
        tls_ca_chain="",
        region="default",
    )


def setup_microceph() -> S3ConnectionInfo:
    """Set up microceph, radosgw, and an S3 user; return S3 connection info.

    If ``S3_ACCESS_KEY``, ``S3_SECRET_KEY`` and ``S3_ENDPOINT`` environment variables
    are set, the microceph setup is skipped entirely and the credentials are taken
    from the environment. ``S3_TLS_CA`` may optionally supply a CA certificate chain
    (in the format expected by the `s3-integrator` charm) and ``S3_REGION`` the region
    (defaulting to ``default``).
    """
    if (
        os.environ.get("S3_ACCESS_KEY")
        and os.environ.get("S3_SECRET_KEY")
        and os.environ.get("S3_ENDPOINT")
    ):
        logger.info("S3 credentials found in environment, skipping microceph setup")
        return S3ConnectionInfo(
            endpoint=os.environ["S3_ENDPOINT"],
            access_key=os.environ["S3_ACCESS_KEY"],
            secret_key=os.environ["S3_SECRET_KEY"],
            tls_ca_chain=os.environ.get("S3_TLS_CA", ""),
            region=os.environ.get("S3_REGION", "default"),
        )
    ip = host_ip()
    install_microceph()
    setup_radosgw()
    return create_user(ip)
