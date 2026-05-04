use openssl::pkey::PKey;
use openssl::ssl::{SslConnector, SslContext, SslMethod, SslVerifyMode};
use openssl::x509::{X509, store::X509StoreBuilder};
use pyo3::prelude::*;

use crate::errors::DriverTlsError;

#[derive(Clone)]
#[pyclass(name = "TlsContext", frozen, skip_from_py_object)]
pub(crate) struct PyTlsContext {
    pub(crate) inner: SslContext,
}

/// Internal Rust-side builder used by the public Python `TlsContextBuilder`.
#[pyclass(name = "TlsContextBuilder")]
pub(crate) struct PyTlsContextBuilder {
    verify_peer: bool,
    ca_pem: Option<Vec<u8>>,
    client_identity: Option<(Vec<u8>, Vec<u8>)>,
}

#[pymethods]
impl PyTlsContextBuilder {
    #[new]
    fn new() -> Self {
        Self {
            verify_peer: true,
            ca_pem: None,
            client_identity: None,
        }
    }

    fn set_verify_peer(&mut self, verify_peer: bool) {
        self.verify_peer = verify_peer;
    }

    fn set_ca_pem(&mut self, ca_pem: &[u8]) {
        self.ca_pem = Some(ca_pem.to_vec());
    }

    fn set_cert_chain_pem(&mut self, cert_pem: &[u8], key_pem: &[u8]) {
        self.client_identity = Some((cert_pem.to_vec(), key_pem.to_vec()));
    }

    fn build(&self) -> Result<PyTlsContext, DriverTlsError> {
        let mut builder =
            SslConnector::builder(SslMethod::tls()).map_err(DriverTlsError::openssl_error)?;

        if self.verify_peer {
            builder.set_verify(SslVerifyMode::PEER);
        } else {
            builder.set_verify(SslVerifyMode::NONE);
        }

        if let Some(ca_bytes) = self.ca_pem.as_deref() {
            let ca_certs = X509::stack_from_pem(ca_bytes).map_err(|error| {
                DriverTlsError::invalid_parameters(format!("Invalid CA PEM: {error}"))
            })?;

            let mut cert_store = X509StoreBuilder::new().map_err(DriverTlsError::openssl_error)?;

            for cert in ca_certs {
                cert_store
                    .add_cert(cert)
                    .map_err(DriverTlsError::openssl_error)?;
            }

            // SslConnector loads the system trust store by default. Replace it
            // when a custom CA bundle is supplied, preserving the existing API
            // semantics where only the explicitly supplied CAs are trusted.
            builder.set_cert_store(cert_store.build());
        }

        if let Some((cert_bytes, key_bytes)) = &self.client_identity {
            // The cert PEM may contain a chain of certificates.
            // The first one is the leaf, and the rest are intermediates.
            let mut cert_chain = X509::stack_from_pem(cert_bytes)
                .map_err(|error| {
                    DriverTlsError::invalid_parameters(format!("Invalid certificate PEM: {error}"))
                })?
                .into_iter();

            // The leaf certificate is required.
            let leaf_cert = cert_chain.next().ok_or_else(|| {
                DriverTlsError::invalid_parameters(
                    "Certificate PEM must contain at least one certificate",
                )
            })?;

            builder
                .set_certificate(&leaf_cert)
                .map_err(DriverTlsError::openssl_error)?;

            // Any remaining certificates are intermediates.
            for intermediate in cert_chain {
                builder
                    .add_extra_chain_cert(intermediate)
                    .map_err(DriverTlsError::openssl_error)?;
            }

            let key = PKey::private_key_from_pem(key_bytes).map_err(|error| {
                DriverTlsError::invalid_parameters(format!("Invalid private key PEM: {error}"))
            })?;

            builder
                .set_private_key(&key)
                .map_err(DriverTlsError::openssl_error)?;

            builder
                .check_private_key()
                .map_err(DriverTlsError::openssl_error)?;
        }

        let connector = builder.build();

        Ok(PyTlsContext {
            inner: connector.into_context(),
        })
    }
}

#[pymodule]
pub(crate) fn tls(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyTlsContext>()?;
    module.add_class::<PyTlsContextBuilder>()?;

    Ok(())
}
