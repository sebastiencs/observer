use std::{collections::HashMap, fmt};

/// Failure while building a static token directory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthConfigError {
    EmptyDirectory,
    EmptyToken,
    EmptyTenant,
    DuplicateToken,
}

impl fmt::Display for AuthConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyDirectory => formatter.write_str("token directory must not be empty"),
            Self::EmptyToken => formatter.write_str("bearer token must not be empty"),
            Self::EmptyTenant => formatter.write_str("tenant id must not be empty"),
            Self::DuplicateToken => formatter.write_str("duplicate bearer token"),
        }
    }
}

impl std::error::Error for AuthConfigError {}

/// Authentication failed before a batch is admitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthError {
    Unauthenticated,
}

impl fmt::Display for AuthError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("unauthenticated")
    }
}

impl std::error::Error for AuthError {}

/// Static bearer-token to tenant mapping.
#[derive(Clone)]
pub struct TokenDirectory {
    tokens: HashMap<String, String>,
}

impl TokenDirectory {
    pub fn new<I, Token, Tenant>(bindings: I) -> Result<Self, AuthConfigError>
    where
        I: IntoIterator<Item = (Token, Tenant)>,
        Token: Into<String>,
        Tenant: Into<String>,
    {
        let mut directory = Self {
            tokens: HashMap::new(),
        };
        for (token, tenant_id) in bindings {
            directory.insert(token.into(), tenant_id.into())?;
        }
        if directory.tokens.is_empty() {
            return Err(AuthConfigError::EmptyDirectory);
        }
        Ok(directory)
    }

    fn insert(&mut self, token: String, tenant_id: String) -> Result<(), AuthConfigError> {
        if token.is_empty() {
            return Err(AuthConfigError::EmptyToken);
        }
        if tenant_id.is_empty() {
            return Err(AuthConfigError::EmptyTenant);
        }
        if self.tokens.contains_key(&token) {
            return Err(AuthConfigError::DuplicateToken);
        }
        self.tokens.insert(token, tenant_id);
        Ok(())
    }

    /// Resolve `Authorization` to a tenant. Never includes the token in the error.
    pub fn authenticate(&self, authorization: Option<&str>) -> Result<String, AuthError> {
        let header = authorization.ok_or(AuthError::Unauthenticated)?;
        let token = parse_bearer(header)?;
        self.tokens
            .get(token)
            .cloned()
            .ok_or(AuthError::Unauthenticated)
    }
}

impl fmt::Debug for TokenDirectory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TokenDirectory")
            .field("tenants", &self.tokens.values().collect::<Vec<_>>())
            .finish()
    }
}

fn parse_bearer(value: &str) -> Result<&str, AuthError> {
    let value = value.trim();
    let Some((scheme, rest)) = value.split_once(' ') else {
        return Err(AuthError::Unauthenticated);
    };
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return Err(AuthError::Unauthenticated);
    }
    let token = rest.trim();
    if token.is_empty() || token.contains(char::is_whitespace) {
        return Err(AuthError::Unauthenticated);
    }
    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn directory() -> TokenDirectory {
        TokenDirectory::new([("secret-a", "tenant-a"), ("secret-b", "tenant-b")])
            .expect("directory")
    }

    #[test]
    fn authenticates_bearer_token() {
        let tokens = directory();
        assert_eq!(
            tokens.authenticate(Some("Bearer secret-a")).expect("a"),
            "tenant-a"
        );
        assert_eq!(
            tokens.authenticate(Some("bearer secret-b")).expect("b"),
            "tenant-b"
        );
    }

    #[test]
    fn missing_malformed_and_unknown_are_unauthenticated() {
        let tokens = directory();
        for header in [
            None,
            Some(""),
            Some("secret-a"),
            Some("Basic secret-a"),
            Some("Bearer"),
            Some("Bearer "),
            Some("Bearer secret-a extra"),
            Some("Bearer unknown"),
        ] {
            assert_eq!(
                tokens.authenticate(header),
                Err(AuthError::Unauthenticated),
                "{header:?}"
            );
        }
    }

    #[test]
    fn errors_and_debug_omit_the_token() {
        let tokens = directory();
        let error = tokens
            .authenticate(Some("Bearer super-secret-token"))
            .expect_err("unknown");
        let rendered = format!("{error} {error:?} {tokens:?}");
        assert!(!rendered.contains("super-secret-token"));
        assert!(!rendered.contains("secret-a"));
        assert!(!rendered.contains("secret-b"));
        assert!(rendered.contains("unauthenticated"));
        assert!(rendered.contains("tenant-a"));
    }

    #[test]
    fn rejects_invalid_directory() {
        assert_eq!(
            TokenDirectory::new::<[(&str, &str); 0], _, _>([]).expect_err("empty"),
            AuthConfigError::EmptyDirectory
        );
        assert_eq!(
            TokenDirectory::new([("", "tenant-a")]).expect_err("token"),
            AuthConfigError::EmptyToken
        );
        assert_eq!(
            TokenDirectory::new([("secret-a", "")]).expect_err("tenant"),
            AuthConfigError::EmptyTenant
        );
        assert_eq!(
            TokenDirectory::new([("secret-a", "tenant-a"), ("secret-a", "tenant-b")])
                .expect_err("duplicate"),
            AuthConfigError::DuplicateToken
        );
    }
}
