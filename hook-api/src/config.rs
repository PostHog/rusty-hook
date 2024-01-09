use envconfig::Envconfig;

#[derive(Envconfig)]
pub struct Config {
    #[envconfig(from = "BIND_HOST", default = "0.0.0.0")]
    pub host: String,

    #[envconfig(from = "BIND_PORT", default = "8000")]
    pub port: u16,

    #[envconfig(default = "postgres://posthog:posthog@localhost:15432/test_database")]
    pub database_url: String,

    #[envconfig(default = "default")]
    pub queue_name: String,
}

impl Config {
    pub fn bind(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
