use std::{fs::File, io::BufReader, path::Path, sync::Arc};
use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsConnector;

pub fn install_crypto_provider() -> std::io::Result<()> {
    #[cfg(target_vendor = "apple")]
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Error on install crypto provider for tls"))?;
        
    #[cfg(any(target_os = "linux", target_os = "windows"))]
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Error on install crypto provider for tls"))?;
        
    Ok(())
}

pub fn build_root_store(root_ca_cert: Option<&Path>) -> std::io::Result<RootCertStore> {
    let mut root_store = RootCertStore::empty();
    if let Some(root_ca_cert) = root_ca_cert {
        let mut pem = BufReader::new(File::open(root_ca_cert)?);
        let certs = rustls_pemfile::certs(&mut pem);

        let trust_anchors = certs
            .into_iter()
            .map(|cert| {
                cert.map(|cert| {
                    let anchor = webpki::anchor_from_trusted_cert(&cert).unwrap().to_owned();
                    rustls_pki_types::TrustAnchor {
                        subject: anchor.subject,
                        subject_public_key_info: anchor.subject_public_key_info,
                        name_constraints: anchor.name_constraints,
                    }
                })
            })
            .collect::<std::io::Result<Vec<rustls_pki_types::TrustAnchor>>>()?;

        root_store.roots.extend(trust_anchors);
    } else {
        root_store
            .roots
            .extend(webpki_roots::TLS_SERVER_ROOTS.iter().map(|ta| {
                rustls_pki_types::TrustAnchor {
                    subject: ta.subject.clone(),
                    subject_public_key_info: ta.subject_public_key_info.clone(),
                    name_constraints: ta.name_constraints.clone(),
                }
            }));
    }
    Ok(root_store)
}

pub fn build_client_certificates<'a>(
    client_cert: &Path,
) -> std::io::Result<Vec<CertificateDer<'a>>> {
    let file = File::open(client_cert)?;
    let mut pem = BufReader::new(file);
    let raw_certs = rustls_pemfile::certs(&mut pem);

    raw_certs.into_iter().collect::<std::io::Result<Vec<CertificateDer>>>()
}

pub fn build_client_private_keys<'a>(
    client_private_key: &Path,
) -> std::io::Result<Vec<PrivateKeyDer<'a>>> {
    let mut pem = BufReader::new(File::open(client_private_key)?);
    let keys = read_private_keys_from_pem(&mut pem)?;
    
    keys.into_iter()
        .map(|c| {
            PrivateKeyDer::try_from(c)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })
        .collect::<std::io::Result<Vec<PrivateKeyDer>>>()
}

pub fn read_private_keys_from_pem(
    rd: &mut dyn std::io::BufRead,
) -> std::io::Result<Vec<Vec<u8>>> {
    let mut keys = Vec::new();

    loop {
        match rustls_pemfile::read_one(rd)? {
            None => return Ok(keys),
            Some(rustls_pemfile::Item::Pkcs1Key(key)) => keys.push(key.secret_pkcs1_der().to_vec()), //PKCS1/RSA
            Some(rustls_pemfile::Item::Pkcs8Key(key)) => keys.push(key.secret_pkcs8_der().to_vec()), //PKCS8
            Some(rustls_pemfile::Item::Sec1Key(key)) => keys.push(key.secret_sec1_der().to_vec()), //SEC1/EC
            _ => {}
        };
    }
}

pub fn with_client_auth(
    ca_path: Option<&Path>,
    cert_path: &Path,
    key_path: &Path,
    domain: String,
) -> std::io::Result<(TlsConnector, String)> {
    let root_cert_store: RootCertStore = build_root_store(ca_path)?;
    let client_certs: Vec<CertificateDer> = build_client_certificates(&cert_path)?;
    let client_keys: Vec<PrivateKeyDer> = build_client_private_keys(&key_path)?;
    let config = ClientConfig::builder()
        .with_root_certificates(root_cert_store)
        .with_client_auth_cert(client_certs, client_keys.into_iter().next().ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "No client private key found"))?)
        .unwrap();
    let connector = TlsConnector::from(Arc::new(config));

    Ok((connector, domain))
}