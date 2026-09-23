use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use std::{path::Path, sync::Arc};
use tokio_rustls::TlsConnector;

pub fn install_crypto_provider() -> std::io::Result<()> {
    #[cfg(target_vendor = "apple")]
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                "Error on install crypto provider for tls",
            )
        })?;

    #[cfg(any(target_os = "linux", target_os = "windows"))]
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                "Error on install crypto provider for tls",
            )
        })?;

    Ok(())
}

pub fn build_root_store(root_ca_cert: Option<&Path>) -> std::io::Result<RootCertStore> {
    let mut root_store = RootCertStore::empty();
    if let Some(root_ca_cert) = root_ca_cert {
        let certs = CertificateDer::pem_file_iter(root_ca_cert)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        for cert in certs {
            let cert = cert.map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            root_store
                .add(cert)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        }
    } else {
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    Ok(root_store)
}

pub fn build_client_certificates<'a>(
    client_cert: &Path,
) -> std::io::Result<Vec<CertificateDer<'a>>> {
    let certs = CertificateDer::pem_file_iter(client_cert)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    certs
        .map(|res| res.map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)))
        .collect()
}

pub fn build_client_private_keys<'a>(
    client_private_key: &Path,
) -> std::io::Result<Vec<PrivateKeyDer<'a>>> {
    let key = PrivateKeyDer::from_pem_file(client_private_key)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(vec![key])
}

pub fn with_client_auth(
    ca_path: Option<&Path>,
    cert_path: &Path,
    key_path: &Path,
    domain: String,
) -> std::io::Result<(TlsConnector, String)> {
    let root_cert_store: RootCertStore = build_root_store(ca_path)?;
    let client_certs: Vec<CertificateDer> = build_client_certificates(cert_path)?;
    let client_keys: Vec<PrivateKeyDer> = build_client_private_keys(key_path)?;
    let config = ClientConfig::builder()
        .with_root_certificates(root_cert_store)
        .with_client_auth_cert(
            client_certs,
            client_keys.into_iter().next().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "No client private key found",
                )
            })?,
        )
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let connector = TlsConnector::from(Arc::new(config));

    Ok((connector, domain))
}
