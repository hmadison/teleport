// Copyright 2021 Gravitational, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package handler

import (
	"context"

	"github.com/gravitational/trace"

	api "github.com/gravitational/teleport/lib/teleterm/api/protogen/golang/v1"
	"github.com/gravitational/teleport/lib/teleterm/daemon"
)

// Lists all existing clusters
func (s *Handler) ListClusters(ctx context.Context, r *api.ListClustersRequest) (*api.ListClustersResponse, error) {
	clusters, err := s.DaemonService.GetClusters(ctx)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	result := []*api.Cluster{}
	for _, cluster := range clusters {
		result = append(result, newAPICluster(cluster))
	}

	return &api.ListClustersResponse{
		Clusters: result,
	}, nil
}

// AddCluster creates a new cluster
func (s *Handler) AddCluster(ctx context.Context, req *api.AddClusterRequest) (*api.Cluster, error) {
	cluster, err := s.DaemonService.AddCluster(ctx, req.Name)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	return newAPICluster(cluster), nil
}

// RemoveCluster removes a cluster from local system
func (s *Handler) RemoveCluster(ctx context.Context, req *api.RemoveClusterRequest) (*api.EmptyResponse, error) {
	if err := s.DaemonService.RemoveCluster(ctx, req.ClusterUri); err != nil {
		return nil, trace.Wrap(err)
	}

	return &api.EmptyResponse{}, nil
}

// GetCluster returns a cluster
func (s *Handler) GetCluster(ctx context.Context, req *api.GetClusterRequest) (*api.Cluster, error) {
	cluster, err := s.DaemonService.GetCluster(req.ClusterUri)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	return newAPICluster(cluster), nil
}

func newAPICluster(cluster *daemon.Cluster) *api.Cluster {
	loggedInUser := cluster.GetLoggedInUser()
	return &api.Cluster{
		UriKind:   string(cluster.URIKind),
		Uri:       cluster.URI,
		Name:      cluster.Name,
		Connected: cluster.Connected(),
		LoggedInUser: &api.LoggedInUser{
			Name:      loggedInUser.Name,
			SshLogins: loggedInUser.SSHLogins,
			Roles:     loggedInUser.Roles,
		},
	}
}
