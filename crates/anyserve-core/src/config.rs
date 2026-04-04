#[derive(Clone, Debug)]
pub struct HttpConfig {
    pub host: String,
    pub port: u16,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 8848,
        }
    }
}

#[derive(Clone, Debug)]
pub struct GrpcConfig {
    pub host: String,
    pub port: u16,
    pub tls_enabled: bool,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 50_052,
            tls_enabled: false,
            cert_file: None,
            key_file: None,
        }
    }
}
