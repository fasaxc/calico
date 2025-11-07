package packedmap

import (
	"fmt"
	"iter"
	"os"

	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

var bucketName = []byte("packedmap")

type DiskBacked[V any] struct {
	encoder Encoder[V, string]
	db      *bbolt.DB
}

func NewDiskBackedCompressedJSON[V any](path string) (*DiskBacked[V], error) {
	encoder := SnappyEncoderWrapper[V, JSONEncoder[V]]{
		encoder: JSONEncoder[V]{},
	}
	return NewDiskBacked[V](path, encoder)
}

func NewDiskBacked[V any](path string, encoder Encoder[V, string]) (*DiskBacked[V], error) {
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		log.WithError(err).Error("Error remoVg existing disk backed file.")
		// Try to open the database anyway...
	}
	// Open the bbolt database; we use NoSync mode for performance since we
	// don't need any durability guarantees.
	db, err := bbolt.Open(path, 0600, &bbolt.Options{NoSync: true})
	if err != nil {
		return nil, fmt.Errorf("failed to create bbolt database at %q: %w", path, err)
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		// In case we failed above, clean out the bucket.
		err = tx.DeleteBucket(bucketName)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucket(bucketName)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to init bbolt database at %q: %w", path, err)
	}
	return &DiskBacked[V]{
		encoder: encoder,
		db:      db,
	}, nil
}

func (p *DiskBacked[V]) Get(key string) (V, bool, error) {
	var packed []byte
	err := p.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		packed = bucket.Get([]byte(key))
		return nil
	})
	var zero V
	if err != nil {
		return zero, false, fmt.Errorf("failed to get key %q from bbolt database: %w", key, err)
	}
	if packed == nil {
		return zero, false, nil
	}
	val := p.encoder.Unpack(string(packed))
	return val, true, nil
}

func (p *DiskBacked[V]) Set(key string, val V) error {
	packed := p.encoder.Pack(val)
	err := p.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		return bucket.Put([]byte(key), []byte(packed))
	})
	if err != nil {
		return fmt.Errorf("failed to set key %q in bbolt database: %w", key, err)
	}
	return nil
}

func (p *DiskBacked[V]) SetSequence(kvPairs iter.Seq2[string, V]) error {
	err := p.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		for k, v := range kvPairs {
			packed := p.encoder.Pack(v)
			err := bucket.Put([]byte(k), []byte(packed))
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to update bbolt database: %w", err)
	}
	return nil
}

func (p *DiskBacked[V]) Close() error {
	return p.db.Close()
}

func (p *DiskBacked[V]) Delete(key string) error {
	err := p.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		return bucket.Delete([]byte(key)) // Returns nil if key not found
	})
	if err != nil {
		return fmt.Errorf("failed to delete key %q from bbolt database: %w", key, err)
	}
	return nil
}

func (p *DiskBacked[V]) Len() (int, error) {
	var length int
	err := p.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		stats := bucket.Stats()
		length = stats.KeyN
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("failed to get length of bbolt database: %w", err)
	}
	return length, nil
}
