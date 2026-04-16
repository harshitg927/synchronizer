package authentication

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/core"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthenticationMiddleware_ClusterUID(t *testing.T) {
	var capturedID domain.ClientIdentifier

	handler := AuthenticationServerMiddleware(nil, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedID = utils.ClientIdentifierFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(core.AccessKeyHeader, "test-key")
	req.Header.Set(core.AccountHeader, "test-account")
	req.Header.Set(core.ClusterNameHeader, "test-cluster")
	req.Header.Set(core.HelmVersionHeader, "v1.0.0")
	req.Header.Set(core.VersionHeader, "v0.0.1")
	req.Header.Set(core.GitVersionHeader, "v1.28.0")
	req.Header.Set(core.CloudProviderHeader, "azure")
	req.Header.Set(core.ClusterUIDHeader, "kube-system-uid-abc123")

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "test-account", capturedID.Account)
	assert.Equal(t, "test-cluster", capturedID.Cluster)
	assert.Equal(t, "kube-system-uid-abc123", capturedID.ClusterUID)
	assert.Equal(t, "azure", capturedID.CloudProvider)
	assert.Equal(t, "v1.28.0", capturedID.GitVersion)
}

func TestAuthenticationMiddleware_ClusterUID_Empty(t *testing.T) {
	var capturedID domain.ClientIdentifier

	handler := AuthenticationServerMiddleware(nil, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedID = utils.ClientIdentifierFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(core.AccessKeyHeader, "test-key")
	req.Header.Set(core.AccountHeader, "test-account")
	req.Header.Set(core.ClusterNameHeader, "test-cluster")

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "", capturedID.ClusterUID)
}

func TestAuthenticationMiddleware_WithAuthServer_ClusterUID(t *testing.T) {
	authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer authServer.Close()

	var capturedID domain.ClientIdentifier

	authConfig := &config.AuthenticationServerConfig{
		Url: authServer.URL,
	}

	handler := AuthenticationServerMiddleware(authConfig, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedID = utils.ClientIdentifierFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(core.AccessKeyHeader, "test-key")
	req.Header.Set(core.AccountHeader, "test-account")
	req.Header.Set(core.ClusterNameHeader, "test-cluster")
	req.Header.Set(core.ClusterUIDHeader, "uid-with-auth")

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "uid-with-auth", capturedID.ClusterUID)
}
