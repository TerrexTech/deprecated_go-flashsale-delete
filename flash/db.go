package flash

import (
	"log"

	"github.com/TerrexTech/go-mongoutils/mongo"
	mgo "github.com/mongodb/mongo-go-driver/mongo"
	"github.com/pkg/errors"
)

type ConfigSchema struct {
	Flash     *Flash
	Metric    *Metric
	Inventory *Inventory
}

type DBIConfig struct {
	Hosts               []string
	Username            string
	Password            string
	TimeoutMilliseconds uint32
	Database            string
	Collection          string
}

type DBI interface {
	Collection() *mongo.Collection
	DeleteFlashSale(fsale []Flash) ([]*mgo.DeleteResult, error)
}

type DB struct {
	collection *mongo.Collection
}

func GenerateDB(dbConfig DBIConfig, schema interface{}) (*DB, error) {
	config := mongo.ClientConfig{
		Hosts:               dbConfig.Hosts,
		Username:            dbConfig.Username,
		Password:            dbConfig.Password,
		TimeoutMilliseconds: dbConfig.TimeoutMilliseconds,
	}

	client, err := mongo.NewClient(config)
	if err != nil {
		err = errors.Wrap(err, "Error creating DB-client")
		return nil, err
	}

	conn := &mongo.ConnectionConfig{
		Client:  client,
		Timeout: 5000,
	}

	// indexConfigs := []mongo.IndexConfig{
	// 	mongo.IndexConfig{
	// 		ColumnConfig: []mongo.IndexColumnConfig{
	// 			mongo.IndexColumnConfig{
	// 				Name: "item_id",
	// 			},
	// 		},
	// 		IsUnique: true,
	// 		Name:     "item_id_index",
	// 	},
	// 	mongo.IndexConfig{
	// 		ColumnConfig: []mongo.IndexColumnConfig{
	// 			mongo.IndexColumnConfig{
	// 				Name:        "timestamp",
	// 				IsDescOrder: true,
	// 			},
	// 		},
	// 		IsUnique: true,
	// 		Name:     "timestamp_index",
	// 	},
	// }

	// ====> Create New Collection
	collConfig := &mongo.Collection{
		Connection:   conn,
		Database:     dbConfig.Database,
		Name:         dbConfig.Collection,
		SchemaStruct: schema,
		// Indexes:      indexConfigs,
	}
	c, err := mongo.EnsureCollection(collConfig)
	if err != nil {
		err = errors.Wrap(err, "Error creating DB-client")
		return nil, err
	}
	return &DB{
		collection: c,
	}, nil
}

func (d *DB) Collection() *mongo.Collection {
	return d.collection
}

func (db *DB) DeleteFlashSale(fsale []Flash) ([]*mgo.DeleteResult, error) {
	var deleteResult *mgo.DeleteResult
	var multipleDeletes []*mgo.DeleteResult
	var err error

	for _, v := range fsale {
		if v.ItemID.String() == "00000000-0000-0000-0000-000000000000" && v.FlashID.String() != "00000000-0000-0000-0000-000000000000" {
			deleteResult, err = db.collection.DeleteMany(&Flash{
				FlashID: v.FlashID,
			})
		} else if v.ItemID.String() != "00000000-0000-0000-0000-000000000000" && v.FlashID.String() == "00000000-0000-0000-0000-000000000000" {
			deleteResult, err = db.collection.DeleteMany(&Flash{
				ItemID: v.ItemID,
			})
		}
		if err != nil {
			err = errors.Wrap(err, "Unable to delete Flash sale")
			log.Println(err)
			return nil, err
		}
		multipleDeletes = append(multipleDeletes, deleteResult)
	}

	return multipleDeletes, nil

}
