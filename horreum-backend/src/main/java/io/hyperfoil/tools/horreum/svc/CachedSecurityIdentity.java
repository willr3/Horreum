package io.hyperfoil.tools.horreum.svc;

import java.security.Permission;
import java.security.Principal;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import io.quarkus.security.credential.Credential;
import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.mutiny.Uni;

public class CachedSecurityIdentity implements SecurityIdentity {
    public static final CachedSecurityIdentity ANONYMOUS = new CachedSecurityIdentity(null, Collections.emptySet(),
            Collections.emptySet(), Collections.emptyMap());
    private final Principal principal;
    private final Set<String> roles;
    private final Set<Credential> credentials;
    private final Map<String, Object> attributes;

    public CachedSecurityIdentity(Principal principal, Set<String> roles, Set<Credential> credentials,
            Map<String, Object> attributes) {
        this.principal = principal;
        this.roles = roles;
        this.credentials = credentials;
        this.attributes = attributes;
    }

    public CachedSecurityIdentity(SecurityIdentity other) {
        this(other.getPrincipal(), other.getRoles(), other.getCredentials(), other.getAttributes());
    }

    @Override
    public Principal getPrincipal() {
        return principal;
    }

    @Override
    public boolean isAnonymous() {
        return false;
    }

    @Override
    public Set<String> getRoles() {
        return roles;
    }

    @Override
    public boolean hasRole(String s) {
        return roles.contains(s);
    }

    @Override
    public <T extends Credential> T getCredential(Class<T> type) {
        return credentials.stream().filter(type::isInstance).map(type::cast).findAny().orElse(null);
    }

    @Override
    public Set<Credential> getCredentials() {
        return credentials;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getAttribute(String s) {
        return (T) attributes.get(s);
    }

    @Override
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    @Override
    public Uni<Boolean> checkPermission(Permission permission) {
        throw new UnsupportedOperationException();
    }
}
