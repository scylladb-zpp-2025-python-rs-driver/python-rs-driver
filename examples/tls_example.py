"""
tls_example.py

Example showcasing how to configure and use TLS/SSL with the ScyllaDB Python driver.

This file demonstrates:
  1) Connecting securely using server-side certificate verification.
  2) Connecting using Mutual TLS (mTLS) with client certificates.
  3) Building the TLS context from in-memory PEM bytes (e.g., from a Secret Manager).
"""

import asyncio
import os

from scylla.session_builder import SessionBuilder
from scylla.tls import TlsContext


async def example_server_verification(host: str, port: int) -> None:
    print("\n=== 1) Connecting with Server-side TLS Verification ===")

    # Assuming you have a CA certificate downloaded locally
    ca_file_path = os.getenv("SCYLLA_CA_PATH", "./certs/ca.crt")

    if not os.path.exists(ca_file_path):
        print(f"Skipping: CA file not found at {ca_file_path}")
        return

    # Load from files using the convenience helper
    tls_config = TlsContext.from_files(ca_path=ca_file_path, verify_peer=True)

    try:
        session = await SessionBuilder().contact_points((host, port)).tls_context(tls_config).connect()  # pyright: ignore[reportArgumentType]
        print("Successfully connected with Server-side TLS!")

        # Verify it works
        result = await session.execute("SELECT cluster_name FROM system.local")
        row = await result.first_row()
        print(f"Cluster name: {row}")

    except Exception as e:
        print(f"Connection failed: {e}")


async def example_mutual_tls(host: str, port: int) -> None:
    print("\n=== 2) Connecting with Mutual TLS (mTLS) ===")

    # Mutual TLS requires the CA, the Client Cert, and the Client Key
    ca_path = os.getenv("SCYLLA_CA_PATH", "./certs/ca.crt")
    cert_path = os.getenv("SCYLLA_CLIENT_CERT", "./certs/client.crt")
    key_path = os.getenv("SCYLLA_CLIENT_KEY", "./certs/client.key")

    if not all(os.path.exists(p) for p in [ca_path, cert_path, key_path]):
        print("Skipping: mTLS certificates not found.")
        return

    # We can read the bytes manually to simulate pulling from a Secret Manager (AWS/Vault)
    with open(ca_path, "rb") as f:
        ca_bytes = f.read()
    with open(cert_path, "rb") as f:
        cert_bytes = f.read()
    with open(key_path, "rb") as f:
        key_bytes = f.read()

    # Load using the in-memory PEM builder
    tls_config = TlsContext.from_pem(ca_pem=ca_bytes, cert_pem=cert_bytes, key_pem=key_bytes, verify_peer=True)

    try:
        _session = await SessionBuilder().contact_points((host, port)).tls_context(tls_config).connect()  # pyright: ignore[reportArgumentType]
        print("Successfully connected with Mutual TLS (mTLS)!")
    except Exception as e:
        print(f"Connection failed: {e}")


async def main() -> None:
    # Use standard TLS port 9142 instead of 9042
    uri = os.getenv("SCYLLA_URI", "127.0.0.1:9142")
    host, port_str = uri.split(":")
    port = int(port_str)

    print(f"Attempting TLS connections to {host}:{port} ...")

    await example_server_verification(host, port)
    await example_mutual_tls(host, port)

    print("\nOk.")


if __name__ == "__main__":
    asyncio.run(main())
