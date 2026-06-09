use openssl::pkey::PKey;
use openssl::ssl::{SslContext, SslContextBuilder, SslMethod, SslVerifyMode};
use openssl::x509::X509;
use pyo3::prelude::*;

use crate::errors::DriverTlsError;

#[derive(Clone)]
#[pyclass(name = "TlsContext", frozen, skip_from_py_object)]
pub(crate) struct PyTlsContext {
    pub(crate) inner: SslContext,
}

#[pymethods]
impl PyTlsContext {
    #[new]
    #[pyo3(signature = (*, ca_pem=None, cert_pem=None, key_pem=None, verify_peer=true))]
    fn new(
        ca_pem: Option<&[u8]>,
        cert_pem: Option<&[u8]>,
        key_pem: Option<&[u8]>,
        verify_peer: bool,
    ) -> Result<Self, DriverTlsError> {
        let mut builder = SslContextBuilder::new(SslMethod::tls_client())
            .map_err(DriverTlsError::openssl_error)?;

        if verify_peer {
            builder.set_verify(SslVerifyMode::PEER);
        } else {
            builder.set_verify(SslVerifyMode::NONE);
        }

        if let Some(ca_bytes) = ca_pem {
            let ca_certs = X509::stack_from_pem(ca_bytes).map_err(|e| {
                DriverTlsError::invalid_parameters(format!("Invalid CA PEM: {}", e))
            })?;
            for cert in ca_certs {
                builder
                    .cert_store_mut()
                    .add_cert(cert)
                    .map_err(DriverTlsError::openssl_error)?;
            }
        } else if verify_peer {
            // Only load system default certificates if no custom CA is provided AND
            // we are actually verifying the peer. This prevents unexpected failures
            // on systems using vendored OpenSSL without populated system CA paths.
            builder
                .set_default_verify_paths()
                .map_err(DriverTlsError::openssl_error)?;
        }

        match (cert_pem, key_pem) {
            (Some(cert_bytes), Some(key_bytes)) => {
                // The cert PEM may contain a chain of certificates. The first one is the leaf, and the rest are intermediates.
                let mut cert_chain = X509::stack_from_pem(cert_bytes)
                    .map_err(|e| {
                        DriverTlsError::invalid_parameters(format!("Invalid Cert PEM: {}", e))
                    })?
                    .into_iter();

                // The leaf certificate is required
                let leaf_cert = cert_chain.next().ok_or_else(|| {
                    DriverTlsError::invalid_parameters(
                        "Certificate PEM must contain at least one certificate",
                    )
                })?;

                builder
                    .set_certificate(&leaf_cert)
                    .map_err(DriverTlsError::openssl_error)?;

                // Any remaining certificates are intermediates
                for intermediate in cert_chain {
                    builder
                        .add_extra_chain_cert(intermediate)
                        .map_err(DriverTlsError::openssl_error)?;
                }

                let key = PKey::private_key_from_pem(key_bytes).map_err(|e| {
                    DriverTlsError::invalid_parameters(format!("Invalid Key PEM: {}", e))
                })?;

                builder
                    .set_private_key(&key)
                    .map_err(DriverTlsError::openssl_error)?;

                builder
                    .check_private_key()
                    .map_err(DriverTlsError::openssl_error)?;
            }
            (None, None) => {}
            _ => {
                return Err(DriverTlsError::invalid_parameters(
                    "cert_pem and key_pem must be provided together for mTLS",
                ));
            }
        }

        Ok(Self {
            inner: builder.build(),
        })
    }
}

#[pymodule]
pub(crate) fn tls(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyTlsContext>()?;
    Ok(())
}
