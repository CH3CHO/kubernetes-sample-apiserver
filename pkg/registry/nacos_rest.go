package registry

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"io"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/utils/strings/slices"
	"reflect"
	"strings"
	"sync"
	"time"
)

const namesItemKey = "__names__"
const newLine = "\n"
const newLineWindows = "\r\n"
const dataIdSeparator = "."

// ErrItemNotExists means the item doesn't actually exist.
var ErrItemNotExists = fmt.Errorf("item doesn't exist")

// ErrItemAlreadyExists means the item already exists.
var ErrItemAlreadyExists = fmt.Errorf("item already exists")

var _ rest.StandardStorage = &nacosREST{}
var _ rest.Scoper = &nacosREST{}
var _ rest.Storage = &nacosREST{}

// NewNacosREST instantiates a new REST storage.
func NewNacosREST(
	groupResource schema.GroupResource,
	codec runtime.Codec,
	configClient config_client.IConfigClient,
	isNamespaced bool,
	singularName string,
	newFunc func() runtime.Object,
	newListFunc func() runtime.Object,
	attrFunc storage.AttrFunc,
) rest.Storage {
	if attrFunc == nil {
		if isNamespaced {
			if isNamespaced {
				attrFunc = storage.DefaultNamespaceScopedAttr
			} else {
				attrFunc = storage.DefaultClusterScopedAttr
			}
		}
	}
	// file REST
	return &nacosREST{
		TableConvertor: rest.NewDefaultTableConvertor(groupResource),
		groupResource:  groupResource,
		codec:          codec,
		configClient:   configClient,
		isNamespaced:   isNamespaced,
		singularName:   singularName,
		dataIdPrefix:   strings.ToLower(groupResource.Resource),
		newFunc:        newFunc,
		newListFunc:    newListFunc,
		attrFunc:       attrFunc,
		watchers:       make(map[int]*nacosWatch, 10),
	}
}

type nacosREST struct {
	rest.TableConvertor
	groupResource schema.GroupResource
	codec         runtime.Codec
	configClient  config_client.IConfigClient
	isNamespaced  bool
	singularName  string
	dataIdPrefix  string

	muWatchers sync.RWMutex
	watchers   map[int]*nacosWatch

	newFunc     func() runtime.Object
	newListFunc func() runtime.Object
	attrFunc    storage.AttrFunc
}

func (f *nacosREST) GetSingularName() string {
	return f.singularName
}

func (f *nacosREST) Destroy() {
}

func (f *nacosREST) notifyWatchers(ev watch.Event) {
	f.muWatchers.RLock()
	for _, w := range f.watchers {
		w.ch <- ev
	}
	f.muWatchers.RUnlock()
}

func (f *nacosREST) New() runtime.Object {
	return f.newFunc()
}

func (f *nacosREST) NewList() runtime.Object {
	return f.newListFunc()
}

func (f *nacosREST) NamespaceScoped() bool {
	return f.isNamespaced
}

func (f *nacosREST) Get(
	ctx context.Context,
	name string,
	options *metav1.GetOptions,
) (runtime.Object, error) {
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	if ns == "" {
		ns = "higress-system"
	}
	obj, _, err := f.read(f.codec, ns, f.objectDataId(ctx, name), f.newFunc)
	if obj == nil && err == nil {
		requestInfo, ok := genericapirequest.RequestInfoFrom(ctx)
		var groupResource = schema.GroupResource{}
		if ok {
			groupResource.Group = requestInfo.APIGroup
			groupResource.Resource = requestInfo.Resource
		}
		fmt.Printf("%s %s/%s not found\n", f.groupResource, ns, name)
		return nil, apierrors.NewNotFound(groupResource, name)
	}
	fmt.Printf("%s %s/%s got\n", f.groupResource, ns, name)
	return obj, err
}

func (f *nacosREST) List(
	ctx context.Context,
	options *metainternalversion.ListOptions,
) (runtime.Object, error) {
	newListObj := f.NewList()
	v, err := getListPrt(newListObj)
	if err != nil {
		return nil, err
	}

	ns, _ := genericapirequest.NamespaceFrom(ctx)
	if ns == "" {
		ns = "higress-system"
	}
	list, err := f.configClient.GetConfig(vo.ConfigParam{
		DataId: f.objectNamesDataId(ctx),
		Group:  ns,
	})
	if err != nil {
		return nil, err
	}

	if len(list) != 0 {
		predicate := f.buildListPredicate(options)
		for _, name := range strings.Split(strings.ReplaceAll(list, newLineWindows, newLine), newLine) {
			obj, err := f.Get(ctx, name, nil)
			if obj == nil || err != nil {
				continue
			}
			if ok, err := predicate.Matches(obj); err == nil && ok {
				appendItem(v, obj)
			}
		}
	}
	fmt.Printf("%s %s list count=%d\n", f.groupResource, ns, len(list))
	return newListObj, nil
}

func (f *nacosREST) Create(
	ctx context.Context,
	obj runtime.Object,
	createValidation rest.ValidateObjectFunc,
	options *metav1.CreateOptions,
) (runtime.Object, error) {
	if createValidation != nil {
		if err := createValidation(ctx, obj); err != nil {
			return nil, err
		}
	}

	accessor, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}

	ns, _ := genericapirequest.NamespaceFrom(ctx)
	if ns == "" {
		ns = "higress-system"
	}
	namesDataId := f.objectNamesDataId(ctx)
	list, err := f.configClient.GetConfig(vo.ConfigParam{
		DataId: namesDataId,
		Group:  ns,
	})
	if err != nil {
		return nil, ErrFileNotExists
	}

	name := accessor.GetName()
	var newList, oldMd5 string
	if len(list) != 0 {
		if names := strings.Split(strings.ReplaceAll(list, newLineWindows, newLine), newLine); slices.Contains(names, name) {
			return nil, apierrors.NewConflict(f.groupResource, name, ErrItemAlreadyExists)
		}
		newList = strings.Join([]string{list, name}, newLine)
		oldMd5 = calculateMd5(list)
	} else {
		newList = name
		oldMd5 = ""
	}
	if err := f.writeRaw(ns, namesDataId, newList, oldMd5); err != nil {
		return nil, err
	}

	dataId := f.objectDataId(ctx, name)

	currentConfig, err := f.readRaw(ns, dataId)
	if currentConfig != "" && err == nil {
		return nil, apierrors.NewConflict(f.groupResource, name, ErrItemAlreadyExists)
	}

	accessor.SetCreationTimestamp(metav1.NewTime(time.Now()))
	if err := f.write(f.codec, ns, dataId, "", obj); err != nil {
		return nil, err
	}

	f.notifyWatchers(watch.Event{
		Type:   watch.Added,
		Object: obj,
	})

	return obj, nil
}

func (f *nacosREST) Update(
	ctx context.Context,
	name string,
	objInfo rest.UpdatedObjectInfo,
	createValidation rest.ValidateObjectFunc,
	updateValidation rest.ValidateObjectUpdateFunc,
	forceAllowCreate bool,
	options *metav1.UpdateOptions,
) (runtime.Object, bool, error) {
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	if ns == "" {
		ns = "higress-system"
	}
	dataId := f.objectDataId(ctx, name)

	isCreate := false
	oldObj, oldConfig, err := f.read(f.codec, ns, dataId, f.newFunc)
	if err != nil {
		return nil, false, err
	}
	if oldConfig == "" && err == nil {
		if !forceAllowCreate {
			return nil, false, err
		}
		isCreate = true
	}

	updatedObj, err := objInfo.UpdatedObject(ctx, oldObj)
	if err != nil {
		return nil, false, err
	}

	oldAccessor, err := meta.Accessor(oldObj)
	if err != nil {
		return nil, false, err
	}

	updatedAccessor, err := meta.Accessor(updatedObj)
	if err != nil {
		return nil, false, err
	}

	if isCreate {
		obj, err := f.Create(ctx, updatedObj, createValidation, nil)
		return obj, err == nil, err
	}

	if updateValidation != nil {
		if err := updateValidation(ctx, updatedObj, oldObj); err != nil {
			return nil, false, err
		}
	}

	if updatedAccessor.GetResourceVersion() != oldAccessor.GetResourceVersion() {
		return nil, false, apierrors.NewConflict(f.groupResource, name, nil)
	}

	if err := f.write(f.codec, ns, dataId, oldAccessor.GetResourceVersion(), updatedObj); err != nil {
		return nil, false, err
	}

	f.notifyWatchers(watch.Event{
		Type:   watch.Modified,
		Object: updatedObj,
	})
	return updatedObj, false, nil
}

func (f *nacosREST) Delete(
	ctx context.Context,
	name string,
	deleteValidation rest.ValidateObjectFunc,
	options *metav1.DeleteOptions) (runtime.Object, bool, error) {
	dataId := f.objectDataId(ctx, name)

	oldObj, err := f.Get(ctx, name, nil)
	if err != nil {
		return nil, false, err
	}
	if deleteValidation != nil {
		if err := deleteValidation(ctx, oldObj); err != nil {
			return nil, false, err
		}
	}

	ns, _ := genericapirequest.NamespaceFrom(ctx)
	if ns == "" {
		ns = "higress-system"
	}
	deleted, err := f.configClient.DeleteConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  ns,
	})
	if err != nil {
		return nil, false, err
	}
	if !deleted {
		return nil, false, errors.New("delete config failed: " + dataId)
	}

	// TODO: Delete from list

	f.notifyWatchers(watch.Event{
		Type:   watch.Deleted,
		Object: oldObj,
	})
	return oldObj, true, nil
}

func (f *nacosREST) DeleteCollection(
	ctx context.Context,
	deleteValidation rest.ValidateObjectFunc,
	options *metav1.DeleteOptions,
	listOptions *metainternalversion.ListOptions,
) (runtime.Object, error) {
	list, err := f.List(ctx, listOptions)
	if err != nil {
		return nil, err
	}

	deletedItems := f.NewList()
	v, err := getListPrt(deletedItems)
	if err != nil {
		return nil, err
	}

	for _, obj := range list.(*unstructured.UnstructuredList).Items {
		if deletedObj, deleted, err := f.Delete(ctx, obj.GetName(), deleteValidation, options); deleted && err == nil {
			appendItem(v, deletedObj)
		}
	}
	return deletedItems, nil
}

func (f *nacosREST) objectDataId(ctx context.Context, name string) string {
	//if f.isNamespaced {
	//	// FIXME: return error if namespace is not found
	//	ns, _ := genericapirequest.NamespaceFrom(ctx)
	//	return strings.Join([]string{f.dataIdPrefix, ns, name}, dataIdSeparator)
	//}
	return strings.Join([]string{f.dataIdPrefix, name}, dataIdSeparator)
}

func (f *nacosREST) objectNamesDataId(ctx context.Context) string {
	return strings.Join([]string{f.dataIdPrefix, namesItemKey}, dataIdSeparator)
}

func (f *nacosREST) Watch(ctx context.Context, options *metainternalversion.ListOptions) (watch.Interface, error) {
	nw := &nacosWatch{
		id: len(f.watchers),
		f:  f,
		ch: make(chan watch.Event, 10),
	}

	// On initial watch, send all the existing objects
	list, err := f.List(ctx, options)
	if err != nil {
		return nil, err
	}

	danger := reflect.ValueOf(list).Elem()
	items := danger.FieldByName("Items")

	for i := 0; i < items.Len(); i++ {
		obj := items.Index(i).Addr().Interface().(runtime.Object)
		nw.ch <- watch.Event{
			Type:   watch.Added,
			Object: obj,
		}
	}

	f.muWatchers.Lock()
	f.watchers[nw.id] = nw
	f.muWatchers.Unlock()

	return nw, nil
}
func (f *nacosREST) buildListPredicate(options *metainternalversion.ListOptions) storage.SelectionPredicate {
	label := labels.Everything()
	field := fields.Everything()
	if options != nil {
		if options.LabelSelector != nil {
			label = options.LabelSelector
		}
		if options.FieldSelector != nil {
			field = options.FieldSelector
		}
	}
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: f.attrFunc,
	}
}

func (f *nacosREST) readRaw(group, dataId string) (string, error) {
	return f.configClient.GetConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  group,
	})
}

func (f *nacosREST) read(decoder runtime.Decoder, group, dataId string, newFunc func() runtime.Object) (runtime.Object, string, error) {
	config, err := f.readRaw(group, dataId)
	if err != nil {
		return nil, "", err
	}
	if config == "" {
		return nil, "", nil
	}
	obj, _, err := decoder.Decode([]byte(config), nil, newFunc())
	if err != nil {
		return nil, "", err
	}
	accessor, err := meta.Accessor(obj)
	if err == nil {
		accessor.SetResourceVersion(calculateMd5(config))
	}
	return obj, config, nil
}

func (f *nacosREST) write(encoder runtime.Encoder, group, dataId, oldMd5 string, obj runtime.Object) error {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	// No resource version saved into nacos
	accessor.SetResourceVersion("")

	buf := new(bytes.Buffer)
	if err := encoder.Encode(obj, buf); err != nil {
		return err
	}
	content := buf.String()
	return f.writeRaw(group, dataId, content, oldMd5)
}

func (f *nacosREST) writeRaw(group, dataId, content, oldMd5 string) error {
	published, err := f.configClient.PublishConfig(vo.ConfigParam{
		DataId:  dataId,
		Group:   group,
		Content: content,
		CasMd5:  oldMd5,
	})
	if err != nil {
		return err
	} else if !published {
		return fmt.Errorf("failed to publish config %s", dataId)
	}
	return nil
}

func calculateMd5(str string) string {
	w := md5.New()
	_, _ = io.WriteString(w, str)
	return fmt.Sprintf("%x", w.Sum(nil))
}

type nacosWatch struct {
	f  *nacosREST
	id int
	ch chan watch.Event
}

func (w *nacosWatch) Stop() {
	w.f.muWatchers.Lock()
	delete(w.f.watchers, w.id)
	w.f.muWatchers.Unlock()
}

func (w *nacosWatch) ResultChan() <-chan watch.Event {
	return w.ch
}

// TODO: implement custom table printer optionally
// func (f *nacosREST) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
// 	return &metav1.Table{}, nil
// }
