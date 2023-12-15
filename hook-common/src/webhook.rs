use std::collections;
use std::fmt;
use std::str::FromStr;

use serde::{de::Visitor, Deserialize, Serialize};

use crate::pgqueue::PgQueueError;

/// Supported HTTP methods for webhooks.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum HttpMethod {
    DELETE,
    GET,
    PATCH,
    POST,
    PUT,
}

/// Allow casting `HttpMethod` from strings.
impl FromStr for HttpMethod {
    type Err = PgQueueError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_ref() {
            "DELETE" => Ok(HttpMethod::DELETE),
            "GET" => Ok(HttpMethod::GET),
            "PATCH" => Ok(HttpMethod::PATCH),
            "POST" => Ok(HttpMethod::POST),
            "PUT" => Ok(HttpMethod::PUT),
            invalid => Err(PgQueueError::ParseHttpMethodError(invalid.to_owned())),
        }
    }
}

/// Implement `std::fmt::Display` to convert HttpMethod to string.
impl fmt::Display for HttpMethod {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            HttpMethod::DELETE => write!(f, "DELETE"),
            HttpMethod::GET => write!(f, "GET"),
            HttpMethod::PATCH => write!(f, "PATCH"),
            HttpMethod::POST => write!(f, "POST"),
            HttpMethod::PUT => write!(f, "PUT"),
        }
    }
}

struct HttpMethodVisitor;

impl<'de> Visitor<'de> for HttpMethodVisitor {
    type Value = HttpMethod;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "the string representation of HttpMethod")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        match HttpMethod::from_str(s) {
            Ok(method) => Ok(method),
            Err(_) => Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Str(s),
                &self,
            )),
        }
    }
}

/// Deserialize required to read `HttpMethod` from database.
impl<'de> Deserialize<'de> for HttpMethod {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(HttpMethodVisitor)
    }
}

/// Serialize required to write `HttpMethod` to database.
impl Serialize for HttpMethod {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

/// Convenience to cast `HttpMethod` to `http::Method`.
/// Not all `http::Method` variants are valid `HttpMethod` variants, hence why we
/// can't just use the former or implement `From<HttpMethod>`.
impl From<HttpMethod> for http::Method {
    fn from(val: HttpMethod) -> Self {
        match val {
            HttpMethod::DELETE => http::Method::DELETE,
            HttpMethod::GET => http::Method::GET,
            HttpMethod::PATCH => http::Method::PATCH,
            HttpMethod::POST => http::Method::POST,
            HttpMethod::PUT => http::Method::PUT,
        }
    }
}

impl From<&HttpMethod> for http::Method {
    fn from(val: &HttpMethod) -> Self {
        match val {
            HttpMethod::DELETE => http::Method::DELETE,
            HttpMethod::GET => http::Method::GET,
            HttpMethod::PATCH => http::Method::PATCH,
            HttpMethod::POST => http::Method::POST,
            HttpMethod::PUT => http::Method::PUT,
        }
    }
}

/// `JobParameters` required for the `WebhookConsumer` to execute a webhook.
/// These parameters should match the exported Webhook interface that PostHog plugins.
/// implement. See: https://github.com/PostHog/plugin-scaffold/blob/main/src/types.ts#L15.
#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct WebhookJobParameters {
    pub body: String,
    pub headers: collections::HashMap<String, String>,
    pub method: HttpMethod,
    pub url: String,

    // These should be set if the Webhook is associated with a plugin `composeWebhook` invocation.
    pub team_id: Option<i32>,
    pub plugin_id: Option<i32>,
    pub plugin_config_id: Option<i32>,

    #[serde(default = "default_max_attempts")]
    pub max_attempts: i32,
}

fn default_max_attempts() -> i32 {
    3
}