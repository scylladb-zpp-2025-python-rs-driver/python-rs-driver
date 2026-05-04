"""
tls_example.py

Example showcasing how to configure and use TLS/SSL with the Python driver.

This file demonstrates:
  1) Connecting securely using server-side certificate verification (from disk).
  2) Connecting using Mutual TLS (mTLS) entirely from in-memory PEM bytes.
  3) Mixing and matching file-based CA validation with in-memory client credentials.
"""

import asyncio
import os

from scylla.session_builder import SessionBuilder
from scylla.tls import TlsContextBuilder


async def example_server_verification(host: str, port: int) -> None:
    print("\n=== 1) Connecting with Server-side TLS Verification (Files) ===")

    # Assuming you have a CA certificate downloaded locally
    ca_file_path = os.getenv("SCYLLA_CA_PATH", "./certs/ca.crt")

    if not os.path.exists(ca_file_path):
        print(f"Skipping: CA file not found at {ca_file_path}")
        return

    # 1. Use the Python builder to load the file
    tls_config = TlsContextBuilder().set_verify_peer(True).load_verify_locations(cafile=ca_file_path)

    # 2. Compile into the native Rust context
    # Note: This method can also be chained with the previous load_verify_locations() call.
    native_tls_context = tls_config.build()

    try:
        session = await SessionBuilder().contact_points((host, port)).tls_context(native_tls_context).connect()
        print("Successfully connected with Server-side TLS!")

        # Verify it works
        result = await session.execute("SELECT cluster_name FROM system.local")
        row = await result.first_row()
        print(f"Cluster name: {row}")

    except Exception as e:
        print(f"Connection failed: {e}")


async def example_mutual_tls_memory(host: str, port: int) -> None:
    print("\n=== 2) Connecting with Mutual TLS (In-Memory Bytes) ===")

    # Mutual TLS requires the CA, the Client Cert, and the Client Key
    ca_path = os.getenv("SCYLLA_CA_PATH", "./certs/ca.crt")
    cert_path = os.getenv("SCYLLA_CLIENT_CERT", "./certs/client.crt")
    key_path = os.getenv("SCYLLA_CLIENT_KEY", "./certs/client.key")

    if not all(os.path.exists(p) for p in [ca_path, cert_path, key_path]):
        print("Skipping: mTLS certificates not found.")
        return

    # Simulate pulling secrets dynamically from AWS Secrets Manager or HashiCorp Vault
    with open(ca_path, "rb") as f:
        ca_bytes = f.read()
    with open(cert_path, "rb") as f:
        cert_bytes = f.read()
    with open(key_path, "rb") as f:
        key_bytes = f.read()

    # 1. Load using the in-memory cadata/certdata arguments
    tls_config = (
        TlsContextBuilder()
        .set_verify_peer(True)
        .load_verify_locations(cadata=ca_bytes)
        .load_cert_chain(certdata=cert_bytes, keydata=key_bytes)
    )

    # 2. Compile into the native Rust context
    # Note: This method can also be chained with the previous load_verify_locations() and load_cert_chain() calls.
    native_tls_context = tls_config.build()

    try:
        _session = await SessionBuilder().contact_points((host, port)).tls_context(native_tls_context).connect()
        print("Successfully connected with Mutual TLS (Memory)!")
    except Exception as e:
        print(f"Connection failed: {e}")


async def example_mixed_loading(host: str, port: int) -> None:
    print("\n=== 3) Connecting with Mixed Loading (Disk CA + Memory Certs) ===")

    ca_path = os.getenv("SCYLLA_CA_PATH", "./certs/ca.crt")
    cert_path = os.getenv("SCYLLA_CLIENT_CERT", "./certs/client.crt")
    key_path = os.getenv("SCYLLA_CLIENT_KEY", "./certs/client.key")

    if not all(os.path.exists(p) for p in [ca_path, cert_path, key_path]):
        print("Skipping: mTLS certificates not found.")
        return

    # The CA is safe to keep on disk, but the private key must be loaded from a secure vault in memory
    with open(cert_path, "rb") as f:
        cert_bytes = f.read()
    with open(key_path, "rb") as f:
        key_bytes = f.read()

    # 1. Mix cafile (disk) and certdata/keydata (memory) seamlessly!
    tls_config = (
        TlsContextBuilder()
        .set_verify_peer(True)
        .load_verify_locations(cafile=ca_path)
        .load_cert_chain(certdata=cert_bytes, keydata=key_bytes)
        .build()
    )

    try:
        _session = await SessionBuilder().contact_points((host, port)).tls_context(tls_config).connect()
        print("Successfully connected with Mixed Loading!")
    except Exception as e:
        print(f"Connection failed: {e}")


async def main() -> None:
    # Use standard TLS port 9142 instead of 9042
    uri = os.getenv("SCYLLA_URI", "127.0.0.1:9142")
    host, port_str = uri.split(":")
    port = int(port_str)

    print(f"Attempting TLS connections to {host}:{port} ...")

    await example_server_verification(host, port)
    await example_mutual_tls_memory(host, port)
    await example_mixed_loading(host, port)

    print("\nOk.")


if __name__ == "__main__":
    asyncio.run(main())
