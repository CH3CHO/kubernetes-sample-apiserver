/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package apiserver

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/apis/example"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/sample-apiserver/pkg/registry"
	"path"
)

const GroupName = "higress.io"

var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1"}

var (
	// Scheme defines methods for serializing and deserializing API objects.
	Scheme = runtime.NewScheme()
	// Codecs provides methods for retrieving codecs and serializers for specific
	// versions and content types.
	Codecs = serializer.NewCodecFactory(Scheme)
)

func init() {
	Scheme.AddKnownTypes(SchemeGroupVersion,
		&corev1.ConfigMap{},
		&corev1.ConfigMapList{},
		&metav1.WatchEvent{},
	)

	// TODO fix the server code to avoid this
	metav1.AddToGroupVersion(Scheme, schema.GroupVersion{Version: "v1"})

	// TODO: keep the generic API server from wanting this
	unversioned := schema.GroupVersion{Group: "", Version: "v1"}
	Scheme.AddUnversionedTypes(unversioned,
		&metav1.Status{},
		&metav1.APIVersions{},
		&metav1.APIGroupList{},
		&metav1.APIGroup{},
		&metav1.APIResourceList{},
	)
}

// ExtraConfig holds custom apiserver config
type ExtraConfig struct {
	// Place you custom config here.
}

// Config defines the config for the apiserver
type Config struct {
	GenericConfig *genericapiserver.RecommendedConfig
	ExtraConfig   ExtraConfig
}

// HigressServer contains state for a Kubernetes cluster master/api server.
type HigressServer struct {
	GenericAPIServer *genericapiserver.GenericAPIServer
}

type completedConfig struct {
	GenericConfig genericapiserver.CompletedConfig
	ExtraConfig   *ExtraConfig
}

// CompletedConfig embeds a private pointer that cannot be instantiated outside of this package.
type CompletedConfig struct {
	*completedConfig
}

// Complete fills in any fields not set that are required to have valid data. It's mutating the receiver.
func (cfg *Config) Complete() CompletedConfig {
	c := completedConfig{
		cfg.GenericConfig.Complete(),
		&cfg.ExtraConfig,
	}

	c.GenericConfig.Version = &version.Info{
		Major: "1",
		Minor: "0",
	}

	return CompletedConfig{&c}
}

// New returns a new instance of HigressServer from the given config.
func (c completedConfig) New() (*HigressServer, error) {
	genericServer, err := c.GenericConfig.New("sample-apiserver", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return nil, err
	}

	s := &HigressServer{
		GenericAPIServer: genericServer,
	}

	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(GroupName, Scheme, metav1.ParameterCodec, Codecs)

	storages := map[string]rest.Storage{}
	groupVersionResource := SchemeGroupVersion.WithResource("configmaps").GroupResource()
	codec, _, err := serverstorage.NewStorageCodec(serverstorage.StorageCodecConfig{
		StorageMediaType:  runtime.ContentTypeYAML,
		StorageSerializer: serializer.NewCodecFactory(Scheme),
		StorageVersion:    Scheme.PrioritizedVersionsForGroup(groupVersionResource.Group)[0],
		MemoryVersion:     Scheme.PrioritizedVersionsForGroup(groupVersionResource.Group)[0],
		Config:            storagebackend.Config{}, // useless fields..
	})
	if err != nil {
		return nil, err
	}
	storages["configmaps"] = registry.NewFilepathREST(groupVersionResource, codec, "/tmp/k8s", true, "configmap", newConfigMap, newConfigMapList)
	apiGroupInfo.VersionedResourcesStorageMap[SchemeGroupVersion.Version] = storages

	// Install custom API group and add it to the list of registered groups
	if err := s.GenericAPIServer.InstallAPIGroup(&apiGroupInfo); err != nil {
		return nil, err
	}

	return s, nil
}

func newConfigMap() runtime.Object { return &corev1.ConfigMap{} }

func newConfigMapList() runtime.Object { return &corev1.ConfigMapList{} }

type namespacedResourceStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
	namespaceScoped          bool
	allowCreateOnUpdate      bool
	allowUnconditionalUpdate bool
}

func (s *namespacedResourceStrategy) NamespaceScoped() bool     { return s.namespaceScoped }
func (s *namespacedResourceStrategy) AllowCreateOnUpdate() bool { return s.allowCreateOnUpdate }
func (s *namespacedResourceStrategy) AllowUnconditionalUpdate() bool {
	return s.allowUnconditionalUpdate
}

func (s *namespacedResourceStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	metaObj, err := meta.Accessor(obj)
	if err != nil {
		panic(err.Error())
	}
	labels := metaObj.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels["prepare_create"] = "true"
	metaObj.SetLabels(labels)
}

func (s *namespacedResourceStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {}
func (s *namespacedResourceStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	return nil
}
func (s *namespacedResourceStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}
func (s *namespacedResourceStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return nil
}
func (s *namespacedResourceStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}
func (s *namespacedResourceStrategy) Canonicalize(obj runtime.Object) {}

var strategy = &namespacedResourceStrategy{Scheme, names.SimpleNameGenerator, true, false, true}

const configMapPrefix = "/configmaps"

var configMapStore = &genericregistry.Store{
	NewFunc:                   func() runtime.Object { return &corev1.ConfigMap{} },
	NewListFunc:               func() runtime.Object { return &corev1.ConfigMapList{} },
	DefaultQualifiedResource:  example.Resource("configmaps"),
	SingularQualifiedResource: example.Resource("configmap"),
	CreateStrategy:            strategy,
	UpdateStrategy:            strategy,
	DeleteStrategy:            strategy,
	KeyRootFunc: func(ctx context.Context) string {
		return configMapPrefix
	},
	KeyFunc: func(ctx context.Context, id string) (string, error) {
		if _, ok := genericapirequest.NamespaceFrom(ctx); !ok {
			return "", fmt.Errorf("namespace is required")
		}
		return path.Join(configMapPrefix, id), nil
	},
	ObjectNameFunc: func(obj runtime.Object) (string, error) { return obj.(*corev1.ConfigMap).Name, nil },
	PredicateFunc: func(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
		return storage.SelectionPredicate{
			Label: label,
			Field: field,
			GetAttrs: func(obj runtime.Object) (labels.Set, fields.Set, error) {
				cm, ok := obj.(*corev1.ConfigMap)
				if !ok {
					return nil, nil, fmt.Errorf("not a configmap")
				}
				return cm.ObjectMeta.Labels, generic.ObjectMetaFieldsSet(&cm.ObjectMeta, true), nil
			},
		}
	},
	Storage: genericregistry.DryRunnableStorage{Storage: &dummyStore{}},
}

type dummyStore struct {
}

var cache = map[string]runtime.Object{}

func (d *dummyStore) Versioner() storage.Versioner {
	return nil
}

func (d *dummyStore) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	return nil
}

func (d *dummyStore) Delete(ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions, validateDeletion storage.ValidateObjectFunc, cachedExistingObject runtime.Object) error {
	return nil
}

func (d *dummyStore) Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	return nil, nil
}

func (d *dummyStore) Get(ctx context.Context, key string, opts storage.GetOptions, objPtr runtime.Object) error {
	return nil
}

func (d *dummyStore) GetList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	return nil
}

func (d *dummyStore) GuaranteedUpdate(ctx context.Context, key string, destination runtime.Object, ignoreNotFound bool, preconditions *storage.Preconditions, tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object) error {
	return nil
}

func (d *dummyStore) Count(key string) (int64, error) {
	return int64(len(cache)), nil
}
