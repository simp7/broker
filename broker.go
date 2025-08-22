package broker

import (
	"reflect"
	"sync"
)

// Broker is the structure for pub-sub pattern.
// A Broker is safe for concurrent use by multiple goroutines.

type Broker[T any] struct {
	pub                chan T
	sub                chan chan T
	unsub              chan chan T
	stop               chan struct{}
	last               T
	dismissDuplicates  bool
	publishOnSubscribe bool
	mutex              sync.RWMutex
}

type BrokerInitParams[T any] struct {
	InitialData        T    // Initial data
	BufferSize         int  // channel buffer size, default value is 10
	DismissDuplicates  bool // Whether to deliver to subscribers only when the existing data is not identical when publishing
	PublishOnSubscribe bool // Whether to receive data immediately when subscribing
}

func New[T any](params BrokerInitParams[T]) *Broker[T] {
	bufferSize := 0
	if params.BufferSize == 0 {
		bufferSize = params.BufferSize
	}
	b := &Broker[T]{
		pub:                make(chan T, bufferSize),
		sub:                make(chan chan T, bufferSize),
		unsub:              make(chan chan T, bufferSize),
		stop:               make(chan struct{}),
		last:               params.InitialData,
		dismissDuplicates:  params.DismissDuplicates,
		publishOnSubscribe: params.PublishOnSubscribe,
	}
	go b.run()
	return b
}

func (b *Broker[T]) run() {
	defer func() {
		if err := recover(); err != nil {
			go b.run()
		}
	}()
	subs := map[chan T]struct{}{}
	for {
		select {
		case <-b.stop:
			for ch := range subs {
				close(ch)
			}
			return
		case ch := <-b.sub:
			subs[ch] = struct{}{}
			if b.publishOnSubscribe {
				ch <- b.Current()
			}
		case ch := <-b.unsub:
			if _, ok := subs[ch]; ok {
				delete(subs, ch)
				close(ch)
			}
		case data := <-b.pub:
			for ch := range subs {
				select {
				case ch <- data:
				default:
				}
			}
		}
	}
}

// Stop stops broker and unsubscribe all subscribers automatically
func (b *Broker[T]) Stop() {
	close(b.stop)
}

// Subscribe returns channel that will send you data when publishing.
// When publishOnSubscribe on initPrams was true, this channel also gets last-published data(or initial data when not published) immediately
func (b *Broker[T]) Subscribe() chan T {
	ch := make(chan T, 1)
	b.sub <- ch
	return ch
}

// Unsubscribe unsubscribe channel and closes channel automatically.
// You should call this function when not using channel any more for preventing memory leak.
func (b *Broker[T]) Unsubscribe(ch chan T) {
	b.unsub <- ch
}

// Publish will send all subscribers data except dismissDuplicates on initPrams was true and new data is same as last one.
func (b *Broker[T]) Publish(data T) {
	if b.dismissDuplicates && reflect.DeepEqual(b.last, data) {
		return
	}
	b.mutex.Lock()
	b.last = data
	b.mutex.Unlock()
	b.pub <- data
}

// Current will return last published data(or initial data when not published any data yet).
func (b *Broker[T]) Current() T {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	return b.last
}
