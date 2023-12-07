// Package dnssdbrowser provides a DNS-SD browser.
package dnssdbrowser

import (
	"io"
	"net"
	"net/netip"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/miekg/dns"
	"github.com/rdleal/go-priorityq/kpq"
	"go.uber.org/zap"
)

// Service represents a service instance.
type Service struct {
	// Instance name e.g. MacBouk Pro._http._tcp.local.
	// Note that it is a fully qualified domain name (FQDN), so it includes a trailing period.
	InstanceName string

	// Service name e.g. _http._tcp.local.
	// Note that it is a fully qualified domain name (FQDN), so it includes a trailing period.
	ServiceName string

	// Hostname e.g. MacBouk-Pro.local.
	// Note that it is a fully qualified domain name (FQDN), so it includes a trailing period.
	Hostname string

	// Port
	Port uint16

	// Resolved addresses for hostname
	Addresses []netip.Addr

	// TXT record
	Metadata map[string]*string
}

// UserFriendlyName returns a user friendly name for the service.
func (s *Service) UserFriendlyName() string {
	name, _, _ := strings.Cut(s.InstanceName, ".")
	return name
}

// ChangeType represents the type of change to a service.
type ChangeType int

const (
	Added ChangeType = iota
	Removed
	Updated
)

// Change represents a change to a service.
type Change struct {
	Type    ChangeType
	Service Service
}

// Subscription represents a subscription to a service.
type Subscription struct {
	Stream <-chan Change
	close  chan<- struct{}
}

// Close closes the subscription.
func (s *Subscription) Close() {
	close(s.close)
}

// FullSubscription represents a subscription to a service.
type FullSubscription struct {
	Stream <-chan []Service
	close  chan<- struct{}
}

// Close closes the subscription.
func (s *FullSubscription) Close() {
	close(s.close)
}

// Browser represents a DNS-SD browser.
type Browser struct {
	mu sync.RWMutex

	services            map[string]map[string]*serviceInfo
	servicesByInstance  map[string]map[string]struct{}
	instances           map[string]*instanceInfo
	instancesByHostname map[string]map[string]struct{}
	addressesByHostname map[string]map[netip.Addr]*addressInfo
	activeQueries       map[string][]*query
	deadlineQueue       *kpq.KeyedPriorityQueue[string, deadlineItem]
	queryQueue          *kpq.KeyedPriorityQueue[string, time.Time]

	notify chan struct{}
	log    *zap.Logger
}

// NewBrowser creates a new DNS-SD browser.
func NewBrowser() *Browser {
	return &Browser{
		services:            make(map[string]map[string]*serviceInfo),
		servicesByInstance:  make(map[string]map[string]struct{}),
		instances:           make(map[string]*instanceInfo),
		instancesByHostname: make(map[string]map[string]struct{}),
		addressesByHostname: make(map[string]map[netip.Addr]*addressInfo),
		activeQueries:       make(map[string][]*query),
		deadlineQueue: kpq.NewKeyedPriorityQueue[string](func(a, b deadlineItem) bool {
			return a.QueueDeadline().Before(b.QueueDeadline())
		}),
		queryQueue: kpq.NewKeyedPriorityQueue[string](func(a, b time.Time) bool {
			return a.Before(b)
		}),
		notify: make(chan struct{}, 16),
		log:    zap.NewNop(),
	}
}

// SetLogger sets the logger.
func (b *Browser) SetLogger(log *zap.Logger) *Browser {
	b.log = log
	return b
}

// Subscribe subscribes to a service.
func (b *Browser) Subscribe(serviceName string) *Subscription {
	serviceName = dns.CanonicalName(serviceName)

	b.mu.Lock()
	defer b.mu.Unlock()

	existingServices := b.servicesByName(serviceName)
	stream := make(chan Change, len(existingServices)+16)
	for _, service := range existingServices {
		stream <- Change{
			Type:    Added,
			Service: service,
		}
	}
	close := make(chan struct{})
	sub := &Subscription{
		Stream: stream,
		close:  close,
	}

	listeners := b.activeQueries[serviceName]
	if len(listeners) == 0 {
		if err := b.queryQueue.Push(serviceName, time.Now()); err != nil {
			panic("unexpected error " + err.Error())
		}

		b.notify <- struct{}{}
	}
	b.activeQueries[serviceName] = append(listeners, &query{
		serviceName: serviceName,
		channel:     stream,
		close:       close,
	})

	return sub
}

// FullSubscribe subscribes to a complete view of the service.
// The stream will always start with the current state of the service.
func (b *Browser) FullSubscribe(serviceName string) *FullSubscription {
	serviceName = dns.CanonicalName(serviceName)

	b.mu.Lock()
	defer b.mu.Unlock()

	existingServices := b.servicesByName(serviceName)
	fullStream := make(chan []Service, 16)
	fullStream <- existingServices
	fullClose := make(chan struct{})
	sub := &FullSubscription{
		Stream: fullStream,
		close:  fullClose,
	}

	listeners := b.activeQueries[serviceName]
	if len(listeners) == 0 {
		if err := b.queryQueue.Push(serviceName, time.Now()); err != nil {
			panic("unexpected error " + err.Error())
		}

		b.notify <- struct{}{}
	}

	stream := make(chan Change, 16)
	closeChanges := make(chan struct{})

	go func() {
		defer close(closeChanges)
		for {
			select {
			case <-fullClose:
				return
			case <-stream:
				services := b.Services(serviceName)
				fullStream <- services
			}
		}
	}()

	b.activeQueries[serviceName] = append(listeners, &query{
		serviceName: serviceName,
		channel:     stream,
		close:       closeChanges,
	})

	return sub
}

// Listen listens for DNS-SD packets on the given socket.
// The socket will be closed when the function returns.
func (b *Browser) Listen(socket *net.UDPConn) {
	defer socket.Close()

	messages := make(chan *dns.Msg, 16)
	go func() {
		defer close(messages)
		buf := make([]byte, 9000)
		for {
			n, err := socket.Read(buf)
			if err == io.EOF {
				return
			} else if err != nil {
				continue
			}

			dnsMsg := &dns.Msg{}
			if err := dnsMsg.Unpack(buf[:n]); err != nil {
				continue
			}
			messages <- dnsMsg
		}
	}()

	timer := time.NewTimer(0)
	for {
		now := time.Now()
		deadline := now.Add(1 * time.Hour)
		v, ok := b.deadlineQueue.PeekValue()
		if ok && v.QueueDeadline().Before(deadline) {
			deadline = v.QueueDeadline()
		}
		v2, ok := b.queryQueue.PeekValue()
		if ok && v2.Before(deadline) {
			deadline = v2
		}

		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(deadline.Sub(now))

		select {
		case msg, ok := <-messages:
			if !ok {
				return
			}
			b.handlePacket(msg)
		case now := <-timer.C:
			b.mu.Lock()
			b.flushDeadlines(now)
			b.runQueries(now, socket)
			b.mu.Unlock()
		case <-b.notify:
			// recalculate deadline
		}
	}
}

var MULTICAST_ADDR = netip.AddrPortFrom(netip.AddrFrom4([...]byte{224, 0, 0, 251}), 5353)

const queryInterval = 15 * time.Second

func (b *Browser) runQueries(now time.Time, socket *net.UDPConn) {
	for k, v, ok := b.queryQueue.Peek(); ok && v.Before(now); k, v, ok = b.queryQueue.Peek() {
		b.queryQueue.Pop()

		b.log.Debug("sending query", zap.String("serviceName", k))

		// Filter out any listeners that are closed
		listeners := b.activeQueries[k]
		newListeners := listeners[:0]
		for _, listener := range listeners {
			select {
			case <-listener.close:
				close(listener.channel)
			default:
				newListeners = append(newListeners, listener)
			}
		}

		if len(newListeners) == 0 {
			delete(b.activeQueries, k)
			b.log.Debug("removed query", zap.String("serviceName", k))
			continue
		}

		b.activeQueries[k] = newListeners

		msg := &dns.Msg{}
		msg.SetQuestion(k, dns.TypePTR)
		data, err := msg.Pack()
		if err == nil {
			_, err = socket.WriteToUDPAddrPort(data, MULTICAST_ADDR)
			if err != nil {
				b.log.Error("failed to send query", zap.Error(err))
			}
		} else {
			b.log.Error("failed to pack query", zap.Error(err))
		}

		if err := b.queryQueue.Push(k, now.Add(queryInterval)); err != nil {
			panic("unexpected error " + err.Error())
		}
	}
}

func (b *Browser) flushAddresses(now time.Time, hostname string) {
	b.log.Debug("flushing addresses", zap.String("hostname", hostname))
	addresses, ok := b.addressesByHostname[hostname]
	if !ok {
		return
	}
	oneSecondAgo := now.Add(-1 * time.Second)
	oneSecondFromNow := now.Add(1 * time.Second)
	for address, info := range addresses {
		if info.Received.After(oneSecondAgo) || info.Deadline.Before(oneSecondFromNow) {
			continue
		}
		info.Deadline = oneSecondFromNow
		b.pushDeadline(&addressDeadlineItem{
			Deadline: info.Deadline,
			Hostname: hostname,
			Address:  address,
		})
	}
}

func (b *Browser) flushDeadlines(now time.Time) {
	for _, v, ok := b.deadlineQueue.Peek(); ok && v.QueueDeadline().Before(now); _, v, ok = b.deadlineQueue.Peek() {
		b.deadlineQueue.Pop()
		switch item := v.(type) {
		case *serviceDeadlineItem:
			b.log.Debug("flushing service", zap.String("serviceName", item.ServiceName), zap.String("instanceName", item.InstanceName))
			instances := b.services[item.ServiceName]
			delete(instances, item.InstanceName)
			if len(instances) == 0 {
				delete(b.services, item.ServiceName)
			}
			services := b.servicesByInstance[item.InstanceName]
			delete(services, item.ServiceName)
			if len(services) == 0 {
				delete(b.servicesByInstance, item.InstanceName)
			}

			instance, ok := b.instances[item.InstanceName]
			if ok {
				service := b.buildService(item.ServiceName, item.InstanceName, instance)
				b.streamChange(Removed, service)
			}
		case *instanceDeadlineItem:
			b.log.Debug("flushing instance", zap.String("instanceName", item.InstanceName))
			delete(b.instances, item.InstanceName)
			instances := b.instancesByHostname[item.InstanceName]
			delete(instances, item.InstanceName)
			if len(instances) == 0 {
				delete(b.instancesByHostname, item.InstanceName)
			}

			services := b.servicesByInstance[item.InstanceName]
			for serviceName := range services {
				service := b.buildService(serviceName, item.InstanceName, &instanceInfo{})
				b.streamChange(Removed, service)
			}
		case *addressDeadlineItem:
			b.log.Debug("flushing address", zap.String("hostname", item.Hostname), zap.Any("address", item.Address))
			addresses := b.addressesByHostname[item.Hostname]
			delete(addresses, item.Address)
			if len(addresses) == 0 {
				delete(b.addressesByHostname, item.Hostname)
			}

			instances := b.instancesByHostname[item.Hostname]
			for instanceName := range instances {
				instanceInfo, ok := b.instances[instanceName]
				if !ok {
					continue
				}
				services, ok := b.servicesByInstance[instanceName]
				if !ok {
					continue
				}
				for serviceName := range services {
					service := b.buildService(serviceName, instanceName, instanceInfo)
					b.streamChange(Updated, service)
				}
			}
		}
	}
}

func (b *Browser) pushDeadline(item deadlineItem) {
	key := item.Key()
	if b.deadlineQueue.Push(key, item) != nil {
		_ = b.deadlineQueue.Update(key, item)
	}
}

func (b *Browser) handlePacket(msg *dns.Msg) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	type serviceInstanceKey struct {
		serviceName  string
		instanceName string
	}

	newServiceInstance := make(map[serviceInstanceKey]struct{})
	updatedInstances := make(map[string]struct{})
	newInstances := make(map[string]struct{})
	updatedHostnames := make(map[string]struct{})
	for _, answer := range append(msg.Answer, msg.Extra...) {
		deleteRecord := answer.Header().Ttl == 0
		var deadline time.Time
		if deleteRecord {
			deadline = now.Add(1 * time.Second)
		} else {
			deadline = now.Add(time.Duration(answer.Header().Ttl) * time.Second)
		}

		switch rr := answer.(type) {
		case *dns.SRV:
			b.log.Debug("SRV record", zap.String("name", rr.Hdr.Name), zap.String("target", rr.Target), zap.Uint16("port", rr.Port))
			instanceName := rr.Hdr.Name
			hostName := rr.Target
			instance, ok := b.instances[instanceName]
			if ok {
				updated := false
				if instance.Hostname != hostName {
					updated = true
					delete(b.instancesByHostname[instance.Hostname], instanceName)
					if _, ok := b.instancesByHostname[hostName]; !ok {
						b.instancesByHostname[hostName] = make(map[string]struct{})
					}
					b.instancesByHostname[hostName][instanceName] = struct{}{}
					instance.Hostname = hostName
				}
				if instance.Port != rr.Port {
					updated = true
					instance.Port = rr.Port
				}
				instance.Deadline = deadline
				if updated {
					updatedInstances[instanceName] = struct{}{}
				}
			} else {
				if deleteRecord {
					continue
				}

				instance = &instanceInfo{
					Hostname: hostName,
					Port:     rr.Port,
					Deadline: deadline,
				}
				b.instances[instanceName] = instance
				if _, ok := b.instancesByHostname[hostName]; !ok {
					b.instancesByHostname[hostName] = make(map[string]struct{})
				}
				b.instancesByHostname[hostName][instanceName] = struct{}{}
				newInstances[instanceName] = struct{}{}
			}

			b.pushDeadline(&instanceDeadlineItem{
				Deadline:     deadline,
				InstanceName: instanceName,
			})
		case *dns.PTR:
			b.log.Debug("PTR record", zap.String("name", rr.Hdr.Name), zap.String("ptr", rr.Ptr))
			serviceName := rr.Hdr.Name
			instanceName := rr.Ptr
			service, ok := b.services[serviceName][instanceName]
			if ok {
				service.Deadline = deadline
			} else {
				if deleteRecord {
					continue
				}

				service = &serviceInfo{
					Deadline: deadline,
				}
				if _, ok := b.services[serviceName]; !ok {
					b.services[serviceName] = make(map[string]*serviceInfo)
				}
				b.services[serviceName][instanceName] = service
				if _, ok := b.servicesByInstance[instanceName]; !ok {
					b.servicesByInstance[instanceName] = make(map[string]struct{})
				}
				b.servicesByInstance[instanceName][serviceName] = struct{}{}
				newServiceInstance[serviceInstanceKey{
					serviceName:  serviceName,
					instanceName: instanceName,
				}] = struct{}{}
			}
			b.pushDeadline(&serviceDeadlineItem{
				Deadline:     deadline,
				ServiceName:  serviceName,
				InstanceName: instanceName,
			})
		case *dns.TXT:
			b.log.Debug("TXT record", zap.String("name", rr.Hdr.Name), zap.Any("txt", rr.Txt))
			instanceName := rr.Hdr.Name
			attributes := make(map[string]*string, len(rr.Txt))
			for _, txt := range rr.Txt {
				if len(txt) == 0 {
					continue
				}

				key, value, ok := strings.Cut(txt, "=")
				if ok {
					attributes[key] = &value
				} else {
					attributes[txt] = nil
				}
			}
			instance, ok := b.instances[instanceName]
			if ok {
				instance.Metadata = attributes
			} else {
				if deleteRecord {
					continue
				}

				instance = &instanceInfo{
					Deadline: deadline,
					Metadata: attributes,
				}
				b.instances[instanceName] = instance
				if _, ok := b.instancesByHostname[instance.Hostname]; !ok {
					b.instancesByHostname[instance.Hostname] = make(map[string]struct{})
				}
				b.instancesByHostname[instance.Hostname][instanceName] = struct{}{}
				newInstances[instanceName] = struct{}{}
			}
			b.pushDeadline(&instanceDeadlineItem{
				Deadline:     deadline,
				InstanceName: instanceName,
			})
		case *dns.A:
			b.log.Debug("A record", zap.String("name", rr.Hdr.Name), zap.Any("a", rr.A))
			hostname := rr.Hdr.Name
			if len(rr.A) != 4 {
				continue
			}

			var ip [4]byte
			copy(ip[:], rr.A)
			address := netip.AddrFrom4(ip)
			info, ok := b.addressesByHostname[hostname][address]
			if ok {
				info.Deadline = deadline
				info.Received = now
			} else {
				if deleteRecord {
					continue
				}

				info = &addressInfo{
					Deadline: deadline,
					Received: now,
				}
				if _, ok := b.addressesByHostname[hostname]; !ok {
					b.addressesByHostname[hostname] = make(map[netip.Addr]*addressInfo)
				}
				b.addressesByHostname[hostname][address] = info
				updatedHostnames[hostname] = struct{}{}
			}
			b.pushDeadline(&addressDeadlineItem{
				Deadline: deadline,
				Hostname: hostname,
				Address:  address,
			})

			if msg.Rcode != 0 {
				b.log.Debug("TODO implement cache flush", zap.Int("rcode", msg.Rcode))
			}
		case *dns.AAAA:
			b.log.Debug("AAAA record", zap.String("name", rr.Hdr.Name), zap.Any("aaaa", rr.AAAA))
			hostname := rr.Hdr.Name
			if len(rr.AAAA) != 16 {
				continue
			}
			var ip [16]byte
			copy(ip[:], rr.AAAA)
			address := netip.AddrFrom16(ip)
			info, ok := b.addressesByHostname[hostname][address]
			if ok {
				info.Deadline = deadline
				info.Received = now
			} else {
				if deleteRecord {
					continue
				}

				info = &addressInfo{
					Deadline: deadline,
					Received: now,
				}
				if _, ok := b.addressesByHostname[hostname]; !ok {
					b.addressesByHostname[hostname] = make(map[netip.Addr]*addressInfo)
				}
				b.addressesByHostname[hostname][address] = info
				updatedHostnames[hostname] = struct{}{}
			}
			b.pushDeadline(&addressDeadlineItem{
				Deadline: deadline,
				Hostname: hostname,
				Address:  address,
			})

			if msg.Rcode != 0 {
				b.log.Debug("TODO implement cache flush", zap.Int("rcode", msg.Rcode))
			}
		}
	}

	for hostname := range updatedHostnames {
		instances, ok := b.instancesByHostname[hostname]
		if ok {
			for instanceName := range instances {
				updatedInstances[instanceName] = struct{}{}
			}
		}
	}

	for instanceName := range newInstances {
		delete(updatedInstances, instanceName)
		services, ok := b.servicesByInstance[instanceName]
		if !ok {
			continue
		}
		instance, ok := b.instances[instanceName]
		if !ok {
			continue
		}

		for serviceName := range services {
			delete(newServiceInstance, serviceInstanceKey{
				serviceName:  serviceName,
				instanceName: instanceName,
			})
			services, ok := b.services[serviceName]
			if !ok {
				continue
			}
			serviceDeadline, ok := services[instanceName]
			if !ok {
				continue
			}
			if serviceDeadline.Deadline.Before(now) {
				continue
			}

			service := b.buildService(serviceName, instanceName, instance)
			b.streamChange(Added, service)
		}
	}

	for key := range newServiceInstance {
		serviceName := key.serviceName
		instanceName := key.instanceName
		delete(updatedInstances, instanceName)
		instance, ok := b.instances[instanceName]
		if !ok {
			continue
		}
		if instance.Deadline.Before(now) {
			continue
		}

		services, ok := b.services[serviceName]
		if !ok {
			continue
		}
		serviceDeadline, ok := services[instanceName]
		if !ok {
			continue
		}
		if serviceDeadline.Deadline.Before(now) {
			continue
		}

		service := b.buildService(serviceName, instanceName, instance)
		b.streamChange(Added, service)
	}

	for instanceName := range updatedInstances {
		services, ok := b.servicesByInstance[instanceName]
		if !ok {
			continue
		}
		instance, ok := b.instances[instanceName]
		if !ok {
			continue
		}

		if instance.Deadline.Before(now) {
			continue
		}

		for serviceName := range services {
			services, ok := b.services[serviceName]
			if !ok {
				continue
			}
			serviceDeadline, ok := services[instanceName]
			if !ok {
				continue
			}
			if serviceDeadline.Deadline.Before(now) {
				continue
			}

			service := b.buildService(serviceName, instanceName, instance)
			b.streamChange(Updated, service)
		}
	}
}

// Services returns the currently known services for the given service name.
func (b *Browser) Services(name string) []Service {
	name = dns.CanonicalName(name)

	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.servicesByName(name)
}

func (b *Browser) servicesByName(serviceName string) []Service {
	services := make([]Service, 0)
	for instanceName, svc := range b.services[serviceName] {
		if svc.Deadline.Before(time.Now()) {
			continue
		}
		instanceInfo := b.instances[instanceName]
		service := b.buildService(serviceName, instanceName, instanceInfo)
		services = append(services, service)
	}

	sort.Slice(services, func(i, j int) bool {
		return services[i].InstanceName < services[j].InstanceName
	})

	return services
}

func (b *Browser) buildService(serviceName string, instanceName string, instanceInfo *instanceInfo) Service {
	addresses := b.addresses(instanceInfo.Hostname)
	return Service{
		InstanceName: instanceName,
		ServiceName:  serviceName,
		Hostname:     instanceInfo.Hostname,
		Port:         instanceInfo.Port,
		Addresses:    addresses,
		Metadata:     instanceInfo.Metadata,
	}
}

func (b *Browser) addresses(hostname string) []netip.Addr {
	addresses := make([]netip.Addr, 0)
	for address, info := range b.addressesByHostname[hostname] {
		if info.Deadline.Before(time.Now()) {
			continue
		}
		addresses = append(addresses, address)
	}
	sort.Slice(addresses, func(i, j int) bool {
		return addresses[i].Compare(addresses[j]) < 0
	})
	return addresses
}

func (b *Browser) streamChange(changeType ChangeType, service Service) {
	name := service.ServiceName
	listeners := b.activeQueries[name]
	if len(listeners) == 0 {
		return
	}

	for _, listener := range listeners {
		listener.Push(Change{
			Type:    changeType,
			Service: service,
		})
	}
}

type serviceInfo struct {
	Deadline time.Time
}

type instanceInfo struct {
	Hostname string
	Port     uint16
	Deadline time.Time
	Metadata map[string]*string
}

type deadlineItem interface {
	QueueDeadline() time.Time
	Key() string
}

type serviceDeadlineItem struct {
	Deadline     time.Time
	ServiceName  string
	InstanceName string
}

func (i *serviceDeadlineItem) QueueDeadline() time.Time {
	return i.Deadline
}

func (i *serviceDeadlineItem) Key() string {
	return "service/" + i.ServiceName + "/" + i.InstanceName
}

type instanceDeadlineItem struct {
	Deadline     time.Time
	InstanceName string
}

func (i *instanceDeadlineItem) QueueDeadline() time.Time {
	return i.Deadline
}

func (i *instanceDeadlineItem) Key() string {
	return "instance/" + i.InstanceName
}

type addressDeadlineItem struct {
	Deadline time.Time
	Hostname string
	Address  netip.Addr
}

func (i *addressDeadlineItem) QueueDeadline() time.Time {
	return i.Deadline
}

func (i *addressDeadlineItem) Key() string {
	return "address/" + i.Hostname + "/" + i.Address.String()
}

type addressInfo struct {
	Received time.Time
	Deadline time.Time
}

type query struct {
	serviceName string
	close       <-chan struct{}
	channel     chan<- Change
}

func (q *query) Push(change Change) {
	select {
	case q.channel <- change:
	default:
	}
}
