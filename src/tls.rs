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
        let mut builder =
            SslContextBuilder::new(SslMethod::tls()).map_err(DriverTlsError::openssl_error)?;

        if verify_peer {
            builder.set_verify(SslVerifyMode::PEER);
        } else {
            builder.set_verify(SslVerifyMode::NONE);
        }

        if let Some(ca_bytes) = ca_pem {
            let ca_cert = X509::from_pem(ca_bytes).map_err(|e| {
                DriverTlsError::invalid_parameters(format!("Invalid CA PEM: {}", e))
            })?;
            builder
                .cert_store_mut()
                .add_cert(ca_cert)
                .map_err(DriverTlsError::openssl_error)?;
        }

        match (cert_pem, key_pem) {
            (Some(cert_bytes), Some(key_bytes)) => {
                let cert = X509::from_pem(cert_bytes).map_err(|e| {
                    DriverTlsError::invalid_parameters(format!("Invalid Cert PEM: {}", e))
                })?;
                builder
                    .set_certificate(&cert)
                    .map_err(DriverTlsError::openssl_error)?;

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
